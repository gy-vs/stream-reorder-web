/**
 * 场景四：规则版本
 *
 *  - 规则变化时，已确认窗口冻结在旧版本（含 orderBy、窗口尺寸），不被改写；
 *  - 未确认草稿按新版本重排，DRAFT_RESET / DRAFT_BUILT 在 journal 中可见；
 *  - 手动重算只能作用于未确认部分，命中已确认窗口 -> 409 + RECOMPUTE_REJECTED；
 *  - 结果视图同时暴露每个窗口的 ruleVersion，排查时可解释“为什么顺序不同”。
 */

import { describe, test } from 'node:test';
import { controlledClock, makeKernel, ingest, assert } from './helpers.js';

describe('规则版本与重算', () => {
  test('切换 orderBy：已确认结果保持旧版本顺序，未确认草稿按新版本重排', async () => {
    const clock = controlledClock(10_000);
    const { kernel } = makeKernel(clock);

    // seq 与 eventTime 交错：seq10 的事件时间早于 seq2
    await ingest(kernel, [
      { eventId: 'w1-seq10', partition: 'P', seq: 10, eventTime: 900, arrivalTime: 950 },
      { eventId: 'w1-seq2', partition: 'P', seq: 2, eventTime: 950, arrivalTime: 960 },
    ]);
    // 独立推进水位关闭窗口 [0,1000)，其事件不属于后续未确认窗口
    await ingest(kernel, [
      { eventId: 'closer', partition: 'P', seq: 99, eventTime: 1500, arrivalTime: 1510 },
    ]);

    // v1: partition-seq => seq2 在 seq10 前；窗口 [0,1000) 确认在 v1
    let win0 = kernel.results().windows.find((w) => w.windowStart === 0);
    assert.deepEqual(
      win0.events.map((e) => e.eventId),
      ['w1-seq2', 'w1-seq10']
    );
    assert.equal(win0.ruleVersion, 'v1');

    // 再来一批未确认数据（[1000,2000)，seq 取另一区间，避免跨窗口复用序号）
    await ingest(kernel, [
      { eventId: 'w2-seq110', partition: 'P', seq: 110, eventTime: 1900, arrivalTime: 1950 },
      { eventId: 'w2-seq102', partition: 'P', seq: 102, eventTime: 1950, arrivalTime: 1960 },
    ]);

    // 注册 v2：按 event-time 排序
    const reg = await kernel.registerRule({
      windowSizeMs: 1000,
      allowedLatenessMs: 500,
      orderBy: 'event-time',
    });
    assert.equal(reg.ruleVersion, 'v2');

    // 旧窗口纹丝不动
    win0 = kernel.results().windows.find((w) => w.windowStart === 0);
    assert.deepEqual(
      win0.events.map((e) => e.eventId),
      ['w1-seq2', 'w1-seq10']
    );
    assert.equal(win0.ruleVersion, 'v1');

    // closer(1500) 与 w2 两个事件(1900/1950) 都在未确认草稿 [1000,2000)；
    // v2 按 event-time 排序后顺序应为 1500,1900,1950（不再按 seq）
    const d1k = kernel.pending({ partition: 'P' }).drafts.find((d) => d.windowStart === 1000);
    assert.equal(d1k.ruleVersion, 'v2');
    assert.deepEqual(
      d1k.events.map((e) => e.eventId),
      ['closer', 'w2-seq110', 'w2-seq102']
    );

    // journal 里能看到作废与重建，而不是只看到最终结果
    const resetEntries = kernel
      .journalView()
      .entries.filter((e) => e.type === 'DRAFT_RESET')
      .map((e) => `${e.partition}@${e.windowStart}:${e.oldRuleVersion}->${e.newRuleVersion}`);
    assert.ok(resetEntries.some((x) => x.startsWith('P@1000:v1->v2')));
  });

  test('调整 allowedLateness：水位立即可见变化，只影响尚未确认的窗口', async () => {
    const clock = controlledClock(10_000);
    const { kernel } = makeKernel(clock);
    await ingest(kernel, [
      { eventId: 'x1', partition: 'P', seq: 1, eventTime: 100, arrivalTime: 110 },
      { eventId: 'x2', partition: 'P', seq: 2, eventTime: 1200, arrivalTime: 1210 },
    ]);
    // v1 lateness=500: wm=700，[0,1000) 未确认
    assert.equal(kernel.partitions()[0].watermark, 700);
    assert.equal(kernel.results().windows.length, 0);

    // v2 收紧迟到为 100：wm=1100，仍未确认 [0,1000)? 1100>=1000 -> 立即确认
    await kernel.registerRule({
      windowSizeMs: 1000,
      allowedLatenessMs: 100,
      orderBy: 'partition-seq',
    });
    assert.equal(kernel.partitions()[0].watermark, 1100);
    const win = kernel.results().windows.find((w) => w.windowStart === 0);
    assert.ok(win);
    assert.equal(win.ruleVersion, 'v2', '在 v2 下确认的窗口标记为 v2');
  });

  test('窗口尺寸变化只重切未确认尾部，已确认窗口不回溯重切', async () => {
    const clock = controlledClock(10_000);
    const { kernel } = makeKernel(clock);
    await ingest(kernel, [
      { eventId: 'g1', partition: 'P', seq: 1, eventTime: 100, arrivalTime: 110 },
      { eventId: 'g2', partition: 'P', seq: 2, eventTime: 600, arrivalTime: 610 },
      { eventId: 'gclose', partition: 'P', seq: 50, eventTime: 1600, arrivalTime: 1610 },
    ]);
    assert.ok(kernel.results().windows.some((w) => w.windowId === 'win#P#0'));

    await ingest(kernel, [
      { eventId: 'g3', partition: 'P', seq: 3, eventTime: 2200, arrivalTime: 2210 },
    ]);
    await kernel.registerRule({
      windowSizeMs: 2000,
      allowedLatenessMs: 500,
      orderBy: 'partition-seq',
    });
    // 已确认的 win#P#0 原样保留；尾部以冻结边界 1000 为锚点：
    // 第一个尾部草稿 [1000,2000)（gclose@1600），随后的草稿落在新网格 [2000,4000)（g3@2200）
    const confirmed = kernel.results().windows.map((w) => w.windowId);
    assert.ok(confirmed.includes('win#P#0'));
    const drafts = kernel.pending({ partition: 'P' }).drafts;
    assert.ok(drafts.some((d) => d.windowStart === 1000 && d.windowEnd === 2000));
    assert.ok(drafts.some((d) => d.windowStart === 2000 && d.windowEnd === 4000));
    const tail = drafts.find((d) => d.windowStart === 1000);
    assert.deepEqual(tail.events.map((e) => e.eventId), ['gclose']);
  });

  test('手动重算已确认窗口被拒（409 IMMUTABLE_RESULT）并留下诊断', async () => {
    const clock = controlledClock(10_000);
    const { kernel } = makeKernel(clock);
    await ingest(kernel, [
      { eventId: 'f1', partition: 'P', seq: 1, eventTime: 100, arrivalTime: 110 },
      { eventId: 'fc', partition: 'P', seq: 50, eventTime: 1600, arrivalTime: 1610 },
    ]);
    await assert.rejects(
      () => kernel.recompute({ partition: 'P', windowStart: 0 }),
      (err) => err.statusCode === 409 && err.code === 'IMMUTABLE_RESULT'
    );
    const rejected = kernel.diagnostics({ kind: 'RECOMPUTE_REJECTED' });
    assert.equal(rejected.length, 1);
    assert.equal(rejected[0].windowId, 'win#P#0');
  });

  test('手动重算未确认尾部：作废草稿、按当前规则重建', async () => {
    const clock = controlledClock(10_000);
    const { kernel } = makeKernel(clock);
    await ingest(kernel, [
      { eventId: 'p1', partition: 'P', seq: 1, eventTime: 100, arrivalTime: 110 },
    ]);
    const before = kernel.pending({ partition: 'P' }).drafts[0];
    const out = await kernel.recompute({ partition: 'P' });
    assert.equal(out.outcome, 'RECOMPUTED');
    assert.ok(out.resetDraftKeys.includes('P#0'));
    const after = kernel.pending({ partition: 'P' }).drafts[0];
    assert.equal(after.draftEpoch, before.draftEpoch + 1, '草稿代次增加，变化可区分');
  });
});
