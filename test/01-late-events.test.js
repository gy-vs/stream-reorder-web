/**
 * 场景一：迟到事件
 *
 * 验证：
 *  1. 乱序到达的事件先进入待定草稿，按 partition/seq 重排；
 *  2. 水位（maxEventTime − allowedLateness）越过窗口末端后窗口确认；
 *  3. 窗口确认后，事件时间更早的“旧事件”回来：
 *     - 不能无提示改写已确认结果 -> EXPIRED_LATE + 诊断
 *  4. 尚未确认窗口上的迟到事件仍然可以被接受并重排（late=true）；
 *  5. 结果视图把 eventTime 与 arrivalTime 平级展示。
 */

import { describe, test } from 'node:test';
import { controlledClock, makeKernel, ingest, assert } from './helpers.js';

describe('迟到事件：待定、水位确认、过期迟到诊断', () => {
  test('未确认的迟到事件进入待定区重排，确认后的迟到事件只能留诊断', async () => {
    const clock = controlledClock(10_000);
    const { kernel } = makeKernel(clock);

    // 默认规则：windowSize=1000, allowedLateness=500
    // 分区 P：先到 seq=2 (eventTime=200)，再到 seq=1 (eventTime=100)
    const r1 = await ingest(kernel, [
      { eventId: 'e2', partition: 'P', seq: 2, eventTime: 200, arrivalTime: 1010 },
    ]);
    assert.equal(r1.results[0].status, 'ACCEPTED');

    // wm = 200-500 = -300，窗口 [0,1000) 未确认；e2 在待定区，按 seq 排序只有它
    let pending = kernel.pending({ partition: 'P' });
    assert.equal(pending.drafts.length, 1);
    assert.equal(pending.drafts[0].events.map((e) => e.eventId).join(','), 'e2');
    assert.equal(pending.drafts[0].confirmable, false);

    // 旧的事件（eventTime 更早）在确认前到达：可恢复迟到
    const r2 = await ingest(kernel, [
      { eventId: 'e1', partition: 'P', seq: 1, eventTime: 100, arrivalTime: 1020 },
    ]);
    assert.equal(r2.results[0].status, 'ACCEPTED');
    // e1 eventTime=100 < wm=-300? 否（-300 < 100），不算 late；继续推进
    pending = kernel.pending({ partition: 'P' });
    assert.deepEqual(
      pending.drafts[0].events.map((e) => e.eventId),
      ['e1', 'e2'],
      '待定区按 seq 重排为 e1,e2'
    );

    // 推进事件时间到 1600：wm = 1600-500 = 1100 >= 窗口末端 1000 -> 确认 [0,1000)
    const r3 = await ingest(kernel, [
      { eventId: 'e3', partition: 'P', seq: 3, eventTime: 1600, arrivalTime: 1610 },
    ]);
    assert.ok(r3.confirmedWindowIds.includes('win#P#0'));

    const results = kernel.results({ partition: 'P' });
    const win = results.windows.find((w) => w.windowId === 'win#P#0');
    assert.ok(win);
    assert.deepEqual(win.events.map((e) => e.eventId), ['e1', 'e2']);
    assert.equal(win.ruleVersion, 'v1');
    // 事件时间与到达时间独立、平级
    assert.equal(win.events[0].eventTime, 100);
    assert.equal(win.events[0].arrivalTime, 1020);
    assert.equal(win.events[0].transportDelayMs, 920);

    // 已确认窗口后，同窗口的旧事件（eventTime=300）回来 -> EXPIRED_LATE
    const r4 = await ingest(kernel, [
      { eventId: 'e-late', partition: 'P', seq: 9, eventTime: 300, arrivalTime: 5000 },
    ]);
    assert.equal(r4.results[0].status, 'EXPIRED_LATE');
    assert.equal(r4.results[0].detail.reason, 'event-time-before-confirmed-frontier');
    assert.equal(r4.results[0].detail.frozenUntil, 1000);

    // 已确认结果没有被改写
    const after = kernel.results({ partition: 'P' });
    const win2 = after.windows.find((w) => w.windowId === 'win#P#0');
    assert.deepEqual(win2.events.map((e) => e.eventId), ['e1', 'e2']);

    // 诊断明确归属到被冻结的窗口
    const diags = kernel.diagnostics({ partition: 'P', kind: 'EXPIRED_LATE' });
    assert.equal(diags.length, 1);
    assert.equal(diags[0].windowId, 'win#P#0');
    assert.equal(diags[0].rejectedEventId, 'e-late');
  });

  test('可恢复迟到：事件时间早于水位但窗口尚未确认，标记 late=true 并参与重排', async () => {
    const clock = controlledClock(20_000);
    const { kernel } = makeKernel(clock);
    await ingest(kernel, [
      { eventId: 'a1', partition: 'X', seq: 1, eventTime: 100, arrivalTime: 110 },
    ]);
    // 让水位走到 1400（eventTime 1900 − 500），窗口 [1000,2000) 尚未确认
    await ingest(kernel, [
      { eventId: 'a3', partition: 'X', seq: 3, eventTime: 1900, arrivalTime: 1910 },
    ]);
    // 此时迟到的 seq=2，eventTime=1500 落在 [1000,2000)，wm=1400 < 1500 -> 还不算晚
    let r = await ingest(kernel, [
      { eventId: 'a2', partition: 'X', seq: 2, eventTime: 1500, arrivalTime: 2000 },
    ]);
    assert.equal(r.results[0].status, 'ACCEPTED');
    assert.equal(r.results[0].detail.late, false);

    // 再推进：eventTime=2000 -> wm=1500，窗口 [1000,2000) 末端是2000，wm 1500 还未确认
    await ingest(kernel, [
      { eventId: 'a4', partition: 'X', seq: 4, eventTime: 2000, arrivalTime: 2010 },
    ]);
    // 现在 wm=1500。迟到事件 seq=5? 用 seq 洞测的是窗口内：塞 eventTime=1200 的事件
    r = await ingest(kernel, [
      { eventId: 'a2b', partition: 'X', seq: 10, eventTime: 1200, arrivalTime: 2100 },
    ]);
    assert.equal(r.results[0].status, 'ACCEPTED');
    assert.equal(r.results[0].detail.late, true, 'eventTime 1200 < wm 1500 但窗口未确认 -> late');

    const pending = kernel.pending({ partition: 'X' });
    const draft1k = pending.drafts.find((d) => d.windowStart === 1000);
    assert.deepEqual(
      draft1k.events.map((e) => e.eventId),
      ['a2', 'a3', 'a2b'],
      'partition-seq 排序：seq 2,3,10（a2b 虽事件时间更早但 seq=10）'
    );
    const lateFlags = Object.fromEntries(draft1k.events.map((e) => [e.eventId, e.late]));
    assert.equal(lateFlags.a2b, true);
  });

  test('时间线支持按事件时间与按到达时间两种视图，两类时间平级展示', async () => {
    const clock = controlledClock(30_000);
    const { kernel } = makeKernel(clock);
    await ingest(kernel, [
      { eventId: 'z1', partition: 'P', seq: 1, eventTime: 100, arrivalTime: 900 },
      { eventId: 'z3', partition: 'P', seq: 3, eventTime: 300, arrivalTime: 500 },
      { eventId: 'z2', partition: 'P', seq: 2, eventTime: 200, arrivalTime: 700 },
    ]);
    const byEvent = kernel.timeline({ basis: 'event' });
    assert.deepEqual(byEvent.events.map((e) => e.eventId), ['z1', 'z2', 'z3']);
    const byArrival = kernel.timeline({ basis: 'arrival' });
    assert.deepEqual(byArrival.events.map((e) => e.eventId), ['z3', 'z2', 'z1'],
      '按到达时间，最晚发生的 z1 最后到达');
    for (const e of byEvent.events) {
      assert.equal(typeof e.eventTime, 'number');
      assert.equal(typeof e.arrivalTime, 'number');
      assert.equal(typeof e.transportDelayMs, 'number');
    }
  });

  test('确认时发现序号空洞会记录 SEQ_GAP 诊断', async () => {
    const clock = controlledClock(40_000);
    const { kernel } = makeKernel(clock);
    await ingest(kernel, [
      { eventId: 's1', partition: 'P', seq: 1, eventTime: 100, arrivalTime: 110 },
      { eventId: 's4', partition: 'P', seq: 4, eventTime: 400, arrivalTime: 410 },
      { eventId: 's9', partition: 'P', seq: 9, eventTime: 1900, arrivalTime: 1910 },
    ]);
    const diags = kernel.diagnostics({ kind: 'SEQ_GAP' });
    const gap = diags.find((d) => d.windowStart === 0);
    assert.ok(gap, '窗口 [0,1000) 确认时应报告 seq 2,3 缺失');
    assert.deepEqual(gap.gap, { from: 2, to: 3, missing: 2 });
  });
});
