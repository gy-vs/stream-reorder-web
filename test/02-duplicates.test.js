/**
 * 场景二：相同事件重传 与 同序列号内容不同
 *
 *  - 同 eventId 重传（哪怕到达时间不同）：DUPLICATE，首次结果不变；
 *  - 同 (partition,seq)、eventId 不同但内容指纹相同：DUPLICATE（传输层换 ID）；
 *  - 同 (partition,seq)、内容不同：CONFLICT，隔离后来者，保留先到者，并记诊断；
 *  - 批次幂等：同一 batchId 重复提交返回首次结论。
 */

import { describe, test } from 'node:test';
import { controlledClock, makeKernel, ingest, assert } from './helpers.js';

describe('重复与冲突', () => {
  test('相同 eventId 重传是 DUPLICATE，不改变待定顺序', async () => {
    const clock = controlledClock(10_000);
    const { kernel } = makeKernel(clock);
    await ingest(kernel, [
      { eventId: 'd1', partition: 'P', seq: 1, eventTime: 100, arrivalTime: 110 },
    ]);
    const dup = await ingest(kernel, [
      { eventId: 'd1', partition: 'P', seq: 1, eventTime: 100, arrivalTime: 999 },
    ]);
    assert.equal(dup.results[0].status, 'DUPLICATE');
    assert.equal(dup.results[0].detail.reason, 'same-event-id');
    // 到达时间保留首次事实
    const ev = kernel.event('d1');
    assert.equal(ev.arrivalTime, 110);
    const pending = kernel.pending({ partition: 'P' });
    assert.deepEqual(pending.drafts[0].events.map((e) => e.eventId), ['d1']);
    assert.equal(kernel.stats().store.acceptedCount, 1);
    assert.equal(kernel.stats().store.duplicateCount, 1);
  });

  test('同 partition+seq 且内容一致（换 eventId）算重传', async () => {
    const clock = controlledClock(10_000);
    const { kernel } = makeKernel(clock);
    await ingest(kernel, [
      { eventId: 'orig', partition: 'P', seq: 7, eventTime: 700, arrivalTime: 710, data: { v: 1 } },
    ]);
    const dup = await ingest(kernel, [
      { eventId: 'retry-id', partition: 'P', seq: 7, eventTime: 700, arrivalTime: 800, data: { v: 1 } },
    ]);
    assert.equal(dup.results[0].status, 'DUPLICATE');
    assert.equal(dup.results[0].detail.reason, 'same-partition-seq-same-content');
    assert.equal(dup.results[0].detail.originalEventId, 'orig');
  });

  test('同 partition+seq 内容不同是 CONFLICT：保留先到版本，后来者隔离', async () => {
    const clock = controlledClock(10_000);
    const { kernel } = makeKernel(clock);
    await ingest(kernel, [
      { eventId: 'good', partition: 'P', seq: 5, eventTime: 500, arrivalTime: 510, data: { v: 'A' } },
    ]);
    const bad = await ingest(kernel, [
      { eventId: 'evil', partition: 'P', seq: 5, eventTime: 500, arrivalTime: 600, data: { v: 'B' } },
    ]);
    assert.equal(bad.results[0].status, 'CONFLICT');
    assert.equal(bad.results[0].detail.reason, 'same-partition-seq-different-content');

    // 确认结果里仍然是先到的 good
    await ingest(kernel, [
      { eventId: 'push', partition: 'P', seq: 20, eventTime: 2000, arrivalTime: 2010 },
    ]);
    const win = kernel.results({ partition: 'P' }).windows.find((w) => w.windowStart === 0);
    assert.deepEqual(win.events.map((e) => e.eventId), ['good']);
    assert.equal(win.events[0].data.v, 'A');

    // evil 的原始事实仍可查，归属 DIAGNOSTIC_ONLY
    const evilView = kernel.event('evil');
    assert.equal(evilView.status, 'CONFLICT');
    assert.equal(evilView.attribution.kind, 'DIAGNOSTIC_ONLY');

    const conflict = kernel.diagnostics({ kind: 'SEQ_CONTENT_CONFLICT' });
    assert.equal(conflict.length, 1);
    assert.equal(conflict[0].retainedEventId, 'good');
    assert.equal(conflict[0].rejectedEventId, 'evil');
  });

  test('同 batchId 重提返回首次结论，状态不增长', async () => {
    const clock = controlledClock(10_000);
    const { kernel } = makeKernel(clock);
    const payload = [
      { eventId: 'b1', partition: 'P', seq: 1, eventTime: 100, arrivalTime: 110 },
    ];
    const first = await kernel.importEvents(payload, { batchId: 'batch-xyz' });
    assert.equal(first.outcome, 'APPLIED');
    const second = await kernel.importEvents(payload, { batchId: 'batch-xyz' });
    assert.equal(second.outcome, 'IGNORED_DUPLICATE_BATCH');
    assert.equal(kernel.stats().store.acceptedCount, 1);
  });
});
