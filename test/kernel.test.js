// kernel.test.js —— 内核状态链路：迟到事件、重复/冲突、分区隔离、规则版本、确认不可改写、边界回溯
import { test, describe, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { makeKernel, ev, ids, reopen } from './helpers.js';
import {
  WINDOW_ORDERED,
  WINDOW_CONFIRMED,
  EVENT_PENDING,
  EVENT_ORDERED,
  EVENT_CONFIRMED,
  EVENT_LATE_DIAG,
  ORDER_BY_SEQUENCE,
  PARTITION_IDLE,
} from '../src/index.js';

let harness;
let dir;

beforeEach(async () => {
  harness = await makeKernel();
  dir = harness.dir;
});
afterEach(() => harness.dispose());

const K = () => harness.kernel;

describe('迟到事件：待定 / 容忍窗口重排 / 仅诊断 三态', () => {
  test('晚于排序线先进待定区，头事件推进后按事件时间排出', async () => {
    const k = K();
    await k.ingestOne(ev('p', 1, 100));
    await k.ingestOne(ev('p', 2, 300, 320));
    let pending = k.pendingView().pending;
    assert.equal(pending.length, 2);
    assert.ok(pending.every((x) => x.status === EVENT_PENDING));

    await k.ingestOne(ev('p', 3, 2000)); // 头事件本身仍待定，早到的 100/300 被排出
    pending = k.pendingView().pending;
    assert.deepEqual(pending.map((x) => x.eventId), ['p|3']);

    const win = k.getWindow('p@0');
    assert.equal(win.status, WINDOW_ORDERED);
    assert.deepEqual(ids(k.resultView().windows[0]), ['p|1', 'p|2']);
    // 修订是可回放记录，而不是只剩最终顺序
    assert.equal(win.revisions.length, 1);
    assert.equal(win.revisions[0].reason, 'WATERMARK_PUBLISH');
  });

  test('事件时间与到达时间分离显示：网络延迟不等于业务时间错误', async () => {
    const k = K();
    const r = await k.ingestOne(ev('p', 1, 100, 1900)); // 到达时 head 仍 -1：缓冲
    await k.ingestOne(ev('p', 2, 2000, 2000));
    const detail = k.eventDetailView('p|1').event;
    assert.equal(detail.eventTime, 100);
    assert.equal(detail.arrivalTime, 1900);
    assert.equal(detail.networkDelay, 1800);
    assert.ok(r.ingested.eventTime !== r.ingested.arrivalTime);
  });

  test('迟到但在容忍窗口内：插入已发布窗口并留下 LATE_ARRIVAL 修订痕迹', async () => {
    const k = K();
    await k.ingestOne(ev('p', 1, 100, 100));
    await k.ingestOne(ev('p', 2, 300, 320));
    await k.ingestOne(ev('p', 3, 2000, 2000)); // 排序线=1000，确认线=0
    await k.ingestOne(ev('p', 4, 250, 2200)); // 晚于排序线、早于确认线

    const win = k.resultView().windows.find((w) => w.windowKey === 'p@0');
    assert.deepEqual(ids(win), ['p|1', 'p|4', 'p|2']); // 重排，非追加
    assert.equal(win.revision, 2);
    assert.equal(win.revisions[1].reason, 'LATE_ARRIVAL');
    assert.equal(win.revisions[1].insertedEventId, 'p|4');

    const late = k.anomaliesView({ kind: 'LATE_REVISION' }).anomalies;
    assert.equal(late.length, 1);
    assert.equal(late[0].lateness, 2200 - 250);
    // 异常归属同时带两种时间
    assert.equal(late[0].eventTime, 250);
    assert.equal(late[0].arrivalTime, 2200);
  });

  test('确认线之后到达的事件只留诊断，已确认窗口不被无提示改写', async () => {
    const k = K();
    await k.ingestBatch([ev('p', 1, 100), ev('p', 2, 300), ev('p', 3, 2000), ev('p', 4, 250, 2200)]);
    await k.ingestOne(ev('p', 5, 4000)); // 确认线=2000：p@0 与 p@2000 全确认

    const confirmed = k.getWindow('p@0');
    assert.equal(confirmed.status, WINDOW_CONFIRMED);
    assert.deepEqual(ids(k.resultView().windows.find((w) => w.windowKey === 'p@0')), [
      'p|1',
      'p|4',
      'p|2',
    ]);

    const diag = await k.ingestOne(ev('p', 6, 150, 4100)); // 旧事件在确认后回来
    assert.equal(diag.lateDiagnostic.status, EVENT_LATE_DIAG);
    const anomaly = k.anomaliesView({ kind: 'LATE_DIAGNOSTIC' }).anomalies[0];
    assert.equal(anomaly.eventId, 'p|6');
    assert.equal(anomaly.confirmLine, 2000);

    const after = k.resultView().windows.find((w) => w.windowKey === 'p@0');
    assert.equal(after.status, WINDOW_CONFIRMED);
    assert.equal(after.revision, 2, '已确认结果不能被旧事件改写');
    assert.deepEqual(ids(after), ['p|1', 'p|4', 'p|2']);
    assert.equal(after.confirmedRuleVersion, 1);
  });

  test('能从确认边界记录跳回原始事件', async () => {
    const k = K();
    await k.ingestBatch([ev('p', 1, 100), ev('p', 2, 300), ev('p', 3, 2000), ev('p', 5, 4000)]);

    const tl = k.timelineView({ limit: 500 });
    const boundary = tl.entries.find(
      (e) => e.type === 'CONFIRM_BOUNDARY' && e.windowKey === 'p@0' && e.windowClosed
    );
    assert.ok(boundary, '时间线必须包含关闭 p@0 的确认边界');
    assert.deepEqual(boundary.lockedEventIds.sort(), ['p|1', 'p|2']);

    // 从边界偏移回放，再用事件 id 跳回原始事件
    const replayed = k.replayView({ fromOffset: boundary.offset, toOffset: boundary.offset });
    assert.equal(replayed.entries[0].type, 'CONFIRM_BOUNDARY');
    for (const id of boundary.lockedEventIds) {
      const detail = k.eventDetailView(id);
      assert.equal(detail.event.eventTime <= boundary.confirmLine, true);
      assert.ok(detail.event.lifecycle.some((l) => l.transition === 'CONFIRMED'));
    }
  });
});

describe('重传与同序列号不同内容', () => {
  test('相同内容重传：幂等忽略，不新增修订、不改变 orderIndex', async () => {
    const k = K();
    await k.ingestOne(ev('p', 1, 100, 100, { amount: 5 }));
    await k.ingestOne(ev('p', 9, 2000, 2000));
    const before = k.getEvent('p|1');
    const revBefore = k.getWindow('p@0')?.revision;

    const r = await k.ingestOne(ev('p', 1, 100, 3000, { amount: 5 }));
    assert.ok(r.duplicate);
    assert.equal(r.duplicate.eventId, 'p|1');

    const after = k.getEvent('p|1');
    assert.equal(after.orderIndex, before.orderIndex);
    assert.ok(after.lifecycle.some((l) => l.transition === 'DUPLICATE_IGNORED'));
    assert.equal(k.getWindow('p@0').revision, revBefore);
    assert.equal(k.anomaliesView({ kind: 'DUPLICATE' }).anomalies.length, 1);
  });

  test('同序列号内容不同：首写获胜，拒绝内容保留为冲突诊断', async () => {
    const k = K();
    await k.ingestOne(ev('p', 1, 100, 100, { amount: 5 }));
    const r = await k.ingestOne(ev('p', 1, 100, 1200, { amount: 999 }));
    assert.ok(r.conflict);
    const conflict = k.anomaliesView({ kind: 'CONFLICT' }).anomalies[0];
    assert.equal(conflict.kind, 'CONFLICT');
    assert.deepEqual(conflict.rejectedPayload, { amount: 999 });
    assert.notEqual(conflict.winnerContentHash, conflict.rejectedContentHash);
    // 胜出的还是首条
    assert.deepEqual(k.getEvent('p|1').payload, { amount: 5 });
    assert.equal(k.state.events.size, 1, '冲突事件不占新身份');
  });
});

describe('分区隔离与空闲分区', () => {
  test('水位、窗口、确认按分区独立计算', async () => {
    const k = K();
    await k.ingestOne(ev('A', 1, 100, 100));
    await k.ingestOne(ev('A', 2, 5000, 5000)); // A 早早确认
    await k.ingestOne(ev('B', 1, 200, 200));
    await k.ingestOne(ev('B', 2, 2000, 2000)); // B 排序线=1000

    const view = k.resultView();
    const a = view.partitions.find((p) => p.partition === 'A');
    const b = view.partitions.find((p) => p.partition === 'B');
    assert.equal(a.orderLine, 4000);
    assert.equal(b.orderLine, 1000);
    // 全局水位取活跃分区最小值：B 把全局压在 1000
    assert.equal(view.globalWatermark.orderLine, 1000);
    assert.deepEqual(view.globalWatermark.contributors.sort(), ['A', 'B']);

    const winA = view.windows.find((w) => w.windowKey === 'A@0');
    const winB = view.windows.find((w) => w.windowKey === 'B@0');
    assert.equal(winA.status, WINDOW_CONFIRMED);
    assert.equal(winB.status, WINDOW_ORDERED);
  });

  test('暂时没有数据的分区超过空闲超时被标记 IDLE 且不再拖低全局水位，恢复后重新计入', async () => {
    const k = K();
    await k.ingestOne(ev('A', 1, 5000, 5000)); // A: 排序线 4000
    await k.ingestOne(ev('B', 1, 200, 200)); // B 先有早期事件
    await k.ingestOne(ev('B', 2, 2000, 2000)); // B: 排序线 1000，最后活动时间 2000
    assert.equal(k.resultView().globalWatermark.orderLine, 1000);

    await k.tick(7001); // 距 B 最后活动 5001ms（超过空闲超时 5000）
    const b = k.getPartition('B');
    assert.equal(b.status, PARTITION_IDLE);
    assert.equal(k.resultView().globalWatermark.orderLine, 4000);
    assert.deepEqual(k.resultView().globalWatermark.idlePartitions, ['B']);

    // B 恢复：新事件让它重新活跃并再次拖住全局水位
    await k.ingestOne(ev('B', 3, 2500, 7050));
    assert.equal(k.getPartition('B').status, 'ACTIVE');
    assert.equal(k.resultView().globalWatermark.orderLine, 1500);
  });

  test('重算一个分区的窗口不影响其他分区', async () => {
    const k = K();
    await k.ingestBatch([
      ev('A', 10, 300, 300),
      ev('A', 11, 100, 110),
      ev('B', 10, 300, 300),
      ev('B', 11, 100, 110),
      ev('A', 20, 2000, 2000),
      ev('B', 20, 2000, 2000),
    ]);
    await k.activateRule({ orderBy: ORDER_BY_SEQUENCE }, { expectedRuleVersion: 1 });
    const before = ids(k.resultView().windows.find((w) => w.windowKey === 'B@0'));
    await k.recomputeWindow('A@0', { expectedRuleVersion: 2 });
    assert.deepEqual(ids(k.resultView().windows.find((w) => w.windowKey === 'B@0')), before);
    assert.deepEqual(ids(k.resultView().windows.find((w) => w.windowKey === 'A@0')), [
      'A|10',
      'A|11',
    ]);
  });
});

describe('规则版本与重算', () => {
  async function buildStaleWindow(k) {
    await k.ingestBatch([
      ev('p', 10, 300, 300), // 序列号大但时间晚
      ev('p', 11, 100, 110),
      ev('p', 20, 2000, 2000),
    ]);
    // 事件时间优先：[seq11(et100), seq10(et300)]
    assert.deepEqual(ids(k.resultView().windows[0]), ['p|11', 'p|10']);
  }

  test('规则切换只把未确认窗口标记过期，不自动改写顺序', async () => {
    const k = K();
    await buildStaleWindow(k);
    const r = await k.activateRule({ orderBy: ORDER_BY_SEQUENCE }, { expectedRuleVersion: 1 });
    assert.equal(r.rule.ruleVersion, 2);
    const win = k.resultView().windows[0];
    assert.deepEqual(ids(win), ['p|11', 'p|10'], '显式重算前顺序不变');
    assert.equal(win.staleForRule, 2);
    assert.equal(win.publishedRuleVersion, 1);
    // 规则版本链完整保留
    const rules = k.state.rules;
    assert.equal(rules[0].status, 'SUPERSEDED');
    assert.equal(rules[1].status, 'ACTIVE');
  });

  test('手动重算未确认窗口：按新版本排序并产生 MANUAL_RECOMPUTE 修订', async () => {
    const k = K();
    await buildStaleWindow(k);
    await k.activateRule({ orderBy: ORDER_BY_SEQUENCE }, { expectedRuleVersion: 1 });
    const r = await k.recomputeWindow('p@0', { expectedRuleVersion: 2 });
    assert.deepEqual(ids(r.window), ['p|10', 'p|11']);
    assert.equal(r.window.revisions.at(-1).reason, 'MANUAL_RECOMPUTE');
    assert.equal(r.window.staleForRule, null);
  });

  test('带旧 expectedRuleVersion 的重算被拒绝，旧结果不能覆盖新规则选择', async () => {
    const k = K();
    await buildStaleWindow(k);
    await k.activateRule({ orderBy: ORDER_BY_SEQUENCE }, { expectedRuleVersion: 1 });
    await assert.rejects(
      () => k.recomputeWindow('p@0', { expectedRuleVersion: 1 }),
      (e) => e.status === 409 && e.code === 'RULE_VERSION_CONFLICT'
    );
  });

  test('已确认窗口不能重算', async () => {
    const k = K();
    await k.ingestBatch([ev('p', 1, 100), ev('p', 2, 4000)]);
    assert.equal(k.getWindow('p@0').status, WINDOW_CONFIRMED);
    await assert.rejects(
      () => k.recomputeWindow('p@0'),
      (e) => e.status === 409 && e.code === 'WINDOW_CONFIRMED_LOCKED'
    );
  });

  test('规则版本冲突同样作用于规则切换本身', async () => {
    const k = K();
    await buildStaleWindow(k);
    await k.activateRule({ allowedLateness: 500 }, { expectedRuleVersion: 1 }); // v2
    await assert.rejects(
      () => k.activateRule({ allowedLateness: 200 }, { expectedRuleVersion: 1 }),
      (e) => e.status === 409 && e.code === 'RULE_VERSION_CONFLICT'
    );
  });
});

describe('回放与刷新', () => {
  test('状态变化是可回放记录：从头重放重建出相同状态', async () => {
    const k = K();
    await k.ingestBatch([
      ev('p', 1, 100, 100),
      ev('p', 2, 300, 320),
      ev('p', 3, 2000, 2000),
      ev('p', 4, 250, 2200),
      ev('p', 5, 4000, 4000),
      ev('p', 6, 150, 4100),
      ev('p', 1, 100, 5000),
    ]);
    const before = k.resultView();
    const timelineBefore = k.timelineView({ limit: 1000 });
    assert.ok(timelineBefore.entries.length > 10, '不能只剩最终数字');

    const { kernel: k2 } = await reopen(dir);
    const after = k2.resultView();
    assert.equal(after.asOfOffset, before.asOfOffset);
    assert.equal(JSON.stringify(after.windows), JSON.stringify(before.windows));
    assert.equal(after.counts.confirmed, before.counts.confirmed);
    assert.equal(
      k2.anomaliesView().anomalies.length,
      k.anomaliesView().anomalies.length
    );
    assert.equal(k2.timelineView({ limit: 1000 }).entries.length, timelineBefore.entries.length);
    k2.close();
  });

  test('无快照（删除快照文件）时从 journal 全量重放结果一致', async () => {
    const k = K();
    await k.snapshot();
    await k.ingestBatch([ev('p', 1, 100), ev('p', 2, 4000)]);
    const before = k.resultView();
    k.close();

    const { rmSync } = await import('node:fs');
    const { join } = await import('node:path');
    rmSync(join(dir, 'snapshot.json'), { force: true });

    const { kernel: k2 } = await reopen(dir);
    assert.deepEqual(
      k2.resultView().windows.map((w) => [w.windowKey, w.status]),
      before.windows.map((w) => [w.windowKey, w.status])
    );
    k2.close();
  });
});

describe('并发：新导入与旧查询/旧重算', () => {
  test('写操作严格串行，提交顺序偏移单调；查询视图是不可变快照', async () => {
    const k = K();
    const writes = [];
    for (let i = 0; i < 20; i++) {
      writes.push(k.ingestOne(ev('p', i + 1, i * 100, i * 100)));
    }
    const ruleChange = k.activateRule({ allowedLateness: 250 }, { expectedRuleVersion: 1 });
    const oldView = k.resultView(); // 规则变化前捕获的视图
    const results = await Promise.all([...writes, ruleChange]);

    const offsets = [];
    for (let i = 0; i < 5; i++) {
      await Promise.resolve();
      offsets.push(k.resultView().asOfOffset);
    }
    assert.ok(offsets.at(-1) >= offsets[0]);
    assert.equal(results.length, 21); // 全部成功，无串扰
    // 旧查询视图不被之后的状态变化覆盖
    assert.equal(oldView.currentRule.ruleVersion, 1);
    assert.equal(k.resultView().currentRule.ruleVersion, 2);
  });

  test('重算先入队（按 v1 成功），规则切换随后生效；再用旧版本重算则失败', async () => {
    const k = K();
    await k.ingestBatch([ev('p', 10, 300), ev('p', 11, 100), ev('p', 20, 2000)]);
    // 同步按顺序入队：重算在前
    const recompute = k.recomputeWindow('p@0', { expectedRuleVersion: 1 });
    const activate = k.activateRule({ orderBy: ORDER_BY_SEQUENCE }, { expectedRuleVersion: 1 });
    const [r1] = await Promise.all([recompute, activate]);
    assert.equal(r1.window.revisions.at(-1).ruleVersion, 1);

    await assert.rejects(
      () => k.recomputeWindow('p@0', { expectedRuleVersion: 1 }),
      (e) => e.code === 'RULE_VERSION_CONFLICT'
    );
  });

  test('事件状态不能压成布尔值：每个事件带完整生命周期（导入/缓冲/排序/确认/异常）', async () => {
    const k = K();
    await k.ingestBatch([ev('p', 1, 100), ev('p', 2, 4000)]);
    const transitions = k.getEvent('p|1').lifecycle.map((l) => l.transition);
    assert.deepEqual(transitions, ['IMPORTED', 'BUFFERED', 'ORDERED', 'CONFIRMED']);
    // 每个生命周期节点都能定位到状态记录偏移
    for (const l of k.getEvent('p|1').lifecycle) {
      assert.equal(typeof l.atOffset, 'number');
    }
  });
});
