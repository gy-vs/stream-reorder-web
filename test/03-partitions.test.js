/**
 * 场景三：分区隔离、空分区阻塞、空闲超时与全局发布边界
 *
 *  - 两个 ACTIVE 分区必须都确认，全局边界才推进（min frontier），
 *    “分区暂时没有数据”在边界上是显式阻塞原因，而不是被当成“无异常”；
 *  - tick 只会把长时间无数据的分区标记 IDLE（依据到达时间超时），
 *    IDLE 分区不再阻塞边界；
 *  - IDLE 分区有新事件到达自动复活，边界重新被阻塞/推进；
 *  - 每个全局边界都是独立记录，publishedWindows 可跳回窗口与原始事件。
 */

import { describe, test } from 'node:test';
import { controlledClock, makeKernel, ingest, assert } from './helpers.js';

async function closeFirstWindow(kernel, partition, seq = 100) {
  // eventTime=1500 → wm=1000，恰好关闭窗口 [0,1000)，事件本身进入下一窗口
  return ingest(kernel, [
    { eventId: `${partition}-close`, partition, seq, eventTime: 1500, arrivalTime: 1510 },
  ]);
}

describe('分区隔离与全局发布边界', () => {
  test('单个分区确认后即可发布，边界记录保留发布内容', async () => {
    const clock = controlledClock(10_000);
    const { kernel } = makeKernel(clock);
    await ingest(kernel, [
      { eventId: 'a0', partition: 'A', seq: 1, eventTime: 100, arrivalTime: 110 },
    ]);
    await closeFirstWindow(kernel, 'A');

    const results = kernel.results();
    assert.ok(results.windows.some((w) => w.windowId === 'win#A#0'));
    assert.equal(results.globalFrontier, 1000, '系统只有 A 一个已知分区，A 确认即全局发布');

    const boundaries = kernel.boundaries();
    const last = boundaries[boundaries.length - 1];
    assert.equal(last.frontier, 1000);
    assert.deepEqual(last.blockers, []);
    assert.ok(last.publishedWindows.some((w) => w.windowId === 'win#A#0'));
  });

  test('两个分区中落后的分区决定全局边界，IDLE 超时后放行', async () => {
    const clock = controlledClock(10_000);
    const { kernel } = makeKernel(clock);

    // A、B 都先有数据（都 ACTIVE）
    await ingest(kernel, [
      { eventId: 'a0', partition: 'A', seq: 1, eventTime: 100, arrivalTime: 110 },
      { eventId: 'b0', partition: 'B', seq: 1, eventTime: 100, arrivalTime: 120 },
    ]);

    // 只推 A：A 确认 [0,1000)，B 停在 wm=-400
    await closeFirstWindow(kernel, 'A');
    let r = kernel.results();
    assert.equal(r.globalFrontier, null, 'B 尚无确认，全局不能发布');
    let last = kernel.boundaries().at(-1);
    assert.ok(last.blockers.some((b) => b.partition === 'B' && b.reason === 'NO_CONFIRMATION'));
    assert.equal(last.publishedWindows.length, 0);

    // 到达时间推进超过空闲阈值（默认 5000ms），tick -> A、B 都 IDLE；
    // A 已确认所以仍贡献冻结边界 1000，B 未确认不阻塞
    clock.advance(6000);
    const tick = await kernel.tick();
    assert.deepEqual(tick.idlePartitions, ['A', 'B']);
    r = kernel.results();
    assert.equal(r.globalFrontier, 1000, '两边空闲后，A 已确认的 1000 可以全局发布');
    last = kernel.boundary(kernel.boundaries().at(-1).boundaryId);
    assert.deepEqual(last.idlePartitions, ['A', 'B']);
    const published = last.publishedWindows.find((w) => w.windowId === 'win#A#0');
    assert.ok(published);
    assert.equal(published.events.length, 1);

    // B 复活：新事件到达，全局边界再次受 B 约束
    await ingest(kernel, [
      { eventId: 'b1', partition: 'B', seq: 2, eventTime: 200, arrivalTime: clock.now() + 1 },
    ]);
    const parts = Object.fromEntries(kernel.partitions().map((p) => [p.partition, p]));
    assert.equal(parts.B.status, 'ACTIVE');
    r = kernel.results();
    assert.equal(r.globalFrontier, null, 'B 复活但尚未确认，边界回到不可发布');

    // B 跟上：两边都确认 [0,1000)，全局边界 1000；A 继续确认后由 min 决定
    await closeFirstWindow(kernel, 'B');
    r = kernel.results();
    assert.equal(r.globalFrontier, 1000);
  });

  test('空分区（从未出现）不自动阻塞；边界只统计已知分区', async () => {
    const clock = controlledClock(10_000);
    const { kernel } = makeKernel(clock);
    // 没有任何导入时，边界不存在
    assert.equal(kernel.results().globalFrontier, null);
    assert.deepEqual(kernel.partitions(), []);
  });

  test('全局边界逐次保留，可从边界跳回窗口再跳回原始事件', async () => {
    const clock = controlledClock(10_000);
    const { kernel } = makeKernel(clock);
    await ingest(kernel, [
      { eventId: 'a0', partition: 'A', seq: 1, eventTime: 100, arrivalTime: 110 },
      { eventId: 'b0', partition: 'B', seq: 1, eventTime: 100, arrivalTime: 120 },
    ]);
    await closeFirstWindow(kernel, 'A');
    await closeFirstWindow(kernel, 'B');

    const boundary = kernel.boundaries().at(-1);
    const detail = kernel.boundary(boundary.boundaryId);
    assert.equal(detail.frontier, 1000);
    const win = detail.publishedWindows.find((w) => w.partition === 'A');
    assert.ok(win);
    const ev = kernel.event(win.events[0].eventId);
    assert.equal(ev.eventId, 'a0');
    assert.equal(ev.attribution.kind, 'CONFIRMED_WINDOW');
    assert.equal(ev.attribution.windowId, win.windowId);
  });
});
