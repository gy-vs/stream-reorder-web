// server.test.js —— HTTP 端到端：真实地把消费端、事件存储、回放接口连起来
import { test, describe, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { createApp } from '../src/server.js';
import { tempDir, cleanup } from './helpers.js';

let app;
let base;
let dir;

async function call(method, path, body) {
  const res = await fetch(base + path, {
    method,
    headers: body ? { 'content-type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const json = await res.json();
  return { status: res.status, body: json, headers: res.headers };
}

const postEvents = (events, arrivalTime) =>
  call('POST', '/api/events', arrivalTime !== undefined ? { events, arrivalTime } : { events });
const ev = (partition, sequence, eventTime, arrivalTime = eventTime, payload = { v: sequence }) => ({
  partition,
  sequence,
  eventTime,
  arrivalTime,
  payload,
});

describe('HTTP 端到端', async () => {
  before(async () => {
    dir = tempDir();
    app = await createApp({ dataDir: dir });
    await new Promise((resolve) => app.server.listen(0, resolve));
    const addr = app.server.address();
    base = `http://127.0.0.1:${addr.port}`;
  });
  after(async () => {
    await app.close();
    cleanup(dir);
  });

  test('健康检查返回当前偏移', async () => {
    const r = await call('GET', '/health');
    assert.equal(r.status, 200);
    assert.equal(typeof r.body.asOfOffset, 'number');
  });

  test('完整迟到链路通过 HTTP 可见：导入→待定→重排→确认→仅诊断', async () => {
    await postEvents([ev('h', 1, 100, 100), ev('h', 2, 300, 320), ev('h', 3, 2000, 2000)]);
    let results = await call('GET', '/api/results');
    let win = results.body.windows.find((w) => w.windowKey === 'h@0');
    assert.deepEqual(win.events.map((e) => e.eventId), ['h|1', 'h|2']);
    assert.equal(results.body.counts.pending, 1);

    // 迟到容忍区内重排
    await postEvents([ev('h', 4, 250, 2200)]);
    results = await call('GET', '/api/results');
    win = results.body.windows.find((w) => w.windowKey === 'h@0');
    assert.deepEqual(win.events.map((e) => e.eventId), ['h|1', 'h|4', 'h|2']);
    assert.equal(win.revision, 2);

    // 公开结果把两种时间分开
    const e4 = win.events.find((e) => e.eventId === 'h|4');
    assert.equal(e4.eventTime, 250);
    assert.equal(e4.arrivalTime, 2200);
    assert.equal(e4.networkDelay, 1950);

    // 推进到确认
    await postEvents([ev('h', 5, 4000, 4000)]);
    const confirmed = await call('GET', '/api/windows/h%400');
    assert.equal(confirmed.body.status, 'CONFIRMED');
    assert.equal(confirmed.body.confirmedRuleVersion, 1);

    // 旧事件回来：仅诊断，不改确认结果
    const late = await postEvents([ev('h', 6, 150, 4100)]);
    assert.equal(late.body.lateDiagnostics[0].status, 'LATE_DIAGNOSTIC');
    const after = await call('GET', '/api/windows/h%400');
    assert.equal(after.body.status, 'CONFIRMED');
    assert.equal(after.body.revision, 2);

    const anomalies = await call('GET', '/api/anomalies?kind=LATE_DIAGNOSTIC');
    assert.equal(anomalies.body.anomalies[0].eventId, 'h|6');
  });

  test('重复事件与冲突通过 HTTP 有明确状态', async () => {
    await postEvents([ev('r', 1, 100, 100, { a: 1 })]);
    const dup = await postEvents([ev('r', 1, 100, 900, { a: 1 })]);
    assert.equal(dup.body.duplicates.length, 1);
    const conflict = await postEvents([ev('r', 1, 100, 900, { a: 2 })]);
    assert.equal(conflict.body.conflicts.length, 1);
    assert.deepEqual(conflict.body.conflicts[0].rejectedPayload, { a: 2 });
  });

  test('分区筛选只返回目标分区', async () => {
    await postEvents([ev('zA', 1, 100), ev('zB', 1, 100)]);
    const r = await call('GET', '/api/results?partition=zA');
    assert.ok(r.body.partitions.every((p) => p.partition === 'zA'));
    assert.ok(r.body.windows.every((w) => w.partition === 'zA'));
    const full = await call('GET', '/api/results');
    assert.ok(full.body.partitions.length >= 2);
  });

  test('待定详情、事件详情可从结果跳回原始事件', async () => {
    await postEvents([ev('d', 1, 50, 60)]);
    const pending = await call('GET', '/api/pending?partition=d');
    assert.equal(pending.body.pending[0].waitingReason.includes('排序线'), true);
    const detail = await call('GET', '/api/events/d%7C1');
    assert.equal(detail.body.event.payload.v, 1);
    assert.equal(detail.body.event.eventTime, 50);
    assert.equal(detail.body.event.arrivalTime, 60);
  });

  test('规则版本：标记过期→重算；旧版本重算返回 409', async () => {
    await postEvents([
      ev('g', 10, 300, 300),
      ev('g', 11, 100, 110),
      ev('g', 20, 2000, 2000),
    ]);
    const activated = await call('POST', '/api/rules', {
      orderBy: 'SEQUENCE',
      expectedRuleVersion: 1,
    });
    assert.equal(activated.body.rule.ruleVersion, 2);
    const stale = await call('GET', '/api/windows/g%400');
    assert.equal(stale.body.staleForRule, 2);
    assert.deepEqual(stale.body.events.map((e) => e.eventId), ['g|11', 'g|10']);

    const old = await call('POST', '/api/windows/g%400/recompute', { expectedRuleVersion: 1 });
    assert.equal(old.status, 409);
    assert.equal(old.body.error.code, 'RULE_VERSION_CONFLICT');

    const rec = await call('POST', '/api/windows/g%400/recompute', { expectedRuleVersion: 2 });
    assert.deepEqual(rec.body.window.events.map((e) => e.eventId), ['g|10', 'g|11']);
    assert.equal(rec.body.window.revisions.at(-1).reason, 'MANUAL_RECOMPUTE');
  });

  test('已确认窗口重算返回 409', async () => {
    await postEvents([ev('c', 1, 100), ev('c', 2, 4000)]);
    const r = await call('POST', '/api/windows/c%400/recompute', {});
    assert.equal(r.status, 409);
    assert.equal(r.body.error.code, 'WINDOW_CONFIRMED_LOCKED');
  });

  test('时间线与回放接口返回可回放记录（不是只有最终数字）', async () => {
    const tl = await call('GET', '/api/timeline?limit=500&partition=h');
    const types = tl.body.entries.map((e) => e.type);
    assert.ok(types.includes('EVENT_IMPORTED'));
    assert.ok(types.includes('WINDOW_REVISED'));
    assert.ok(types.includes('CONFIRM_BOUNDARY'));

    const first = tl.body.entries[0].offset;
    const replay = await call('GET', `/api/replay?fromOffset=${first}&limit=5`);
    assert.equal(replay.body.entries.length, 5);
    assert.equal(replay.body.entries[0]._offset, first);
    // 回放记录是原始状态记录，含规则版本、锁定事件等溯源字段
    const boundary = replay.body.entries.find((e) => e.type === 'CONFIRM_BOUNDARY');
    if (boundary) assert.ok(Array.isArray(boundary.lockedEventIds));
  });

  test('并发刷新：旧查询带 asOfOffset，新导入推进偏移且不破坏旧视图语义', async () => {
    await postEvents([ev('m', 1, 100)]);
    const v1 = await call('GET', '/api/results');
    const off1 = v1.body.asOfOffset;

    await Promise.all([
      postEvents([ev('m', 2, 110)]),
      postEvents([ev('m', 3, 120)]),
      call('POST', '/api/tick', { arrivalTime: 1 }),
    ]);
    const v2 = await call('GET', '/api/results');
    assert.ok(v2.body.asOfOffset > off1);
    // 分区状态、事件计数单调一致
    assert.ok(v2.body.counts.events >= v1.body.counts.events + 2);
  });

  test('快照接口可用，重启后结果一致', async () => {
    const snap = await call('POST', '/api/snapshot', {});
    assert.equal(snap.status, 200);
    assert.equal(typeof snap.body.offset, 'number');

    // 用同一数据目录启动新服务
    const app2 = await createApp({ dataDir: dir, cleanup: false });
    await new Promise((resolve) => app2.server.listen(0, resolve));
    const port2 = app2.server.address().port;
    const res = await fetch(`http://127.0.0.1:${port2}/health`).then((x) => x.json());
    assert.ok(res.asOfOffset >= snap.body.offset);
    const results = await fetch(`http://127.0.0.1:${port2}/api/results`).then((x) => x.json());
    assert.ok(results.windows.some((w) => w.windowKey === 'h@0' && w.status === 'CONFIRMED'));
    await new Promise((resolve) => app2.server.close(resolve));
    app2.kernel.close();
  });

  test('非法输入有明确错误（400）与未知窗口（404）', async () => {
    const bad = await call('POST', '/api/events', { events: [{ partition: '', sequence: 1, eventTime: 1 }] });
    assert.equal(bad.status, 400);
    assert.equal(bad.body.error.code, 'BAD_EVENT');
    const missing = await call('GET', '/api/windows/nope%400');
    assert.equal(missing.status, 404);
  });
});
