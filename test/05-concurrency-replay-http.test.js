/**
 * 场景五：并发刷新、可回放状态链路、增量读取与 HTTP 端到端
 *
 *  - 新导入与基于旧规则版本的请求并发：旧请求收到 409 STALE_RULE，
 *    不能把旧的重排选择覆盖到新规则；
 *  - 导入与读查询并发不互相破坏（读拿到的是某个时刻的完整快照）；
 *  - journal 重放出的内核状态与实时状态逐字段一致（verifyReplay）；
 *  - FileJournal 重启恢复（快照 + 增量）后结果一致；
 *  - 重算只产生区间读取（rangeReads），不产生全表扫描（fullScans=0）。
 */

import { describe, test, before, after } from 'node:test';
import { tmpdir } from 'node:os';
import { promises as fs } from 'node:fs';
import path from 'node:path';
import { controlledClock, makeKernel, ingest, assert } from './helpers.js';
import { Kernel, createClock } from '../src/index.js';
import { FileJournal } from '../src/persistence.js';
import { createApp } from '../src/app.js';

describe('并发与规则版本守卫', () => {
  test('并发：规则切换后，携带旧 expectedRuleVersion 的导入与重算被拒', async () => {
    const clock = controlledClock(10_000);
    const { kernel } = makeKernel(clock);
    await ingest(kernel, [
      { eventId: 'c0', partition: 'P', seq: 1, eventTime: 100, arrivalTime: 110 },
    ]);

    // 同时发出：一个规则注册，两个基于 v1 的旧选择
    const ruleP = kernel.registerRule(
      { windowSizeMs: 1000, allowedLatenessMs: 100, orderBy: 'partition-seq' },
      { expectedRuleVersion: 'v1' }
    );
    const staleImportP = kernel.importEvents(
      [{ eventId: 'c1', partition: 'P', seq: 2, eventTime: 200, arrivalTime: 210 }],
      { expectedRuleVersion: 'v1' }
    );
    const staleRecomputeP = kernel.recompute({
      partition: 'P',
      expectedRuleVersion: 'v1',
    });

    const reg = await ruleP;
    assert.equal(reg.ruleVersion, 'v2');
    await assert.rejects(staleImportP, (err) => err.code === 'STALE_RULE' && err.statusCode === 409);
    await assert.rejects(staleRecomputeP, (err) => err.code === 'STALE_RULE');

    // 被拒的事件没有进入存储
    assert.throws(() => kernel.event('c1'), /事件不存在/);
    assert.equal(kernel.stats().store.totalArrivals, 1);
  });

  test('并发：导入进行中读视图不报错且时间字段一致', async () => {
    const clock = controlledClock(50_000);
    const { kernel } = makeKernel(clock);
    const reads = [];
    const writers = [];
    for (let i = 0; i < 8; i++) {
      writers.push(
        ingest(kernel, [
          { eventId: `m${i}`, partition: 'P', seq: i + 1, eventTime: 1000 + i, arrivalTime: 5000 + i },
        ])
      );
      for (let j = 0; j < 5; j++) reads.push(kernel.timeline({ basis: 'event' }));
    }
    await Promise.all(writers);
    for (const r of reads) {
      for (const e of r.events) {
        assert.equal(typeof e.eventTime, 'number');
        assert.equal(typeof e.arrivalTime, 'number');
      }
    }
    assert.equal(kernel.stats().store.acceptedCount, 8);
  });
});

describe('可回放状态链路', () => {
  test('verifyReplay：实时状态与逐条规约结果逐字段一致', async () => {
    const clock = controlledClock(60_000);
    const { kernel } = makeKernel(clock);
    await ingest(kernel, [
      { eventId: 'r1', partition: 'A', seq: 1, eventTime: 100, arrivalTime: 110 },
      { eventId: 'r2', partition: 'A', seq: 2, eventTime: 200, arrivalTime: 900 },
      { eventId: 'r3', partition: 'B', seq: 1, eventTime: 150, arrivalTime: 300 },
      { eventId: 'r1dup', partition: 'A', seq: 1, eventTime: 100, arrivalTime: 999, data: null },
    ]);
    await ingest(kernel, [
      { eventId: 'r4', partition: 'A', seq: 20, eventTime: 1800, arrivalTime: 1810 },
      { eventId: 'r5', partition: 'B', seq: 20, eventTime: 1800, arrivalTime: 1820 },
    ]);
    await kernel.registerRule({
      windowSizeMs: 1000,
      allowedLatenessMs: 200,
      orderBy: 'event-time',
    });
    clock.advance(6000);
    await kernel.tick();

    // r1dup: 同 partition/seq 不同 eventId，且指纹相同? eventId 在指纹里 -> 不同，
    // 它其实是 CONFLICT。保持该场景：冲突也必须可回放。
    const rep = kernel.verifyReplay();
    assert.equal(rep.equal, true, JSON.stringify(rep, null, 2));
    assert.ok(rep.entryCount > 10);
  });

  test('FileJournal：快照+增量重启后状态一致，journal.jsonl 可独立重放', async () => {
    const dir = await fs.mkdtemp(path.join(tmpdir(), 'rcw-'));
    const clock = controlledClock(70_000);
    {
      const journal = new FileJournal(dir);
      await journal.init();
      const kernel = new Kernel({ clock, journal });
      await ingest(kernel, [
        { eventId: 'f1', partition: 'A', seq: 1, eventTime: 100, arrivalTime: 110 },
        { eventId: 'f2', partition: 'B', seq: 1, eventTime: 120, arrivalTime: 130 },
      ]);
      const snap = await kernel.checkpoint();
      assert.ok(snap.lastSeq >= 0);
      // 快照之后又有新的状态变化
      await ingest(kernel, [
        { eventId: 'f3', partition: 'A', seq: 20, eventTime: 1700, arrivalTime: 1710 },
        { eventId: 'f4', partition: 'B', seq: 20, eventTime: 1700, arrivalTime: 1720 },
      ]);
      await journal.flush();
    }
    {
      const journal2 = new FileJournal(dir);
      await journal2.init();
      const restored = await Kernel.restore(journal2, { clock });
      const rep = restored.verifyReplay();
      assert.equal(rep.equal, true);
      const wins = restored.results().windows.map((w) => w.windowId).sort();
      assert.deepEqual(wins, ['win#A#0', 'win#B#0']);
      assert.equal(restored.results().globalFrontier, 1000);
      await journal2.flush();
    }
    await fs.rm(dir, { recursive: true, force: true });
  });

  test('journal 视图包含完整状态变化链，而不仅是最终数字', async () => {
    const clock = controlledClock(80_000);
    const { kernel } = makeKernel(clock);
    await ingest(kernel, [
      { eventId: 'j1', partition: 'P', seq: 1, eventTime: 100, arrivalTime: 110 },
      // seq 留空（缺 2..8）→ 窗口确认时产生 SEQ_GAP 诊断
      { eventId: 'j9', partition: 'P', seq: 9, eventTime: 1700, arrivalTime: 1710 },
    ]);
    const view = kernel.journalView();
    const types = [...new Set(view.entries.map((e) => e.type))];
    for (const t of ['BATCH_IMPORT', 'EVENT_RECEIVED', 'WM_ADVANCED', 'DRAFT_BUILT',
      'DIAGNOSTIC', 'WINDOW_CONFIRMED', 'GLOBAL_BOUNDARY', 'RULE_ADDED']) {
      assert.ok(types.includes(t), `journal 缺少 ${t}`);
    }
    assert.ok(view.entries.every((e, i, arr) => i === 0 || e.seq > arr[i - 1].seq), 'seq 单调');
    assert.ok(view.entries.at(-1).seq === view.lastSeq);
  });
});

describe('增量读取：服务端不每次全表扫描', () => {
  test('重算路径只产生区间读取，fullScans 为 0', async () => {
    const clock = controlledClock(90_000);
    const { kernel } = makeKernel(clock);
    for (let w = 0; w < 6; w++) {
      await ingest(kernel, [
        { eventId: `e-${w}`, partition: 'P', seq: w * 10, eventTime: w * 1000 + 100, arrivalTime: w * 1000 + 110 },
      ]);
    }
    // 推进水位确认前几个窗口
    await ingest(kernel, [
      { eventId: 'e-close', partition: 'P', seq: 999, eventTime: 7000, arrivalTime: 7010 },
    ]);
    kernel.state.store.resetStats();
    await kernel.recompute({ partition: 'P' });
    const stats = kernel.stats().store;
    assert.equal(stats.fullScans, 0, '重算不应全表扫描');
    assert.ok(stats.rangeReads > 0, '应使用区间读取');

    // 其他分区的重算不读取 P 的数据（读调用次数不随 P 的事件量增长）
    const before = kernel.stats().store.rangeReads;
    await ingest(kernel, [
      { eventId: 'q1', partition: 'Q', seq: 1, eventTime: 100, arrivalTime: 9000 },
    ]);
    const after = kernel.stats().store.rangeReads;
    assert.ok(after - before < 5, 'Q 的 pump 只读取 Q 自己的区间');
  });
});

describe('HTTP 端到端', () => {
  let app;
  let base;

  before(async () => {
    app = await createApp({ clock: createClock(() => 95_000) });
    const addr = await app.listen(0);
    base = `http://127.0.0.1:${addr.port}`;
  });

  after(async () => {
    await app.close();
  });

  const api = async (method, route, body) => {
    const res = await fetch(base + route, {
      method,
      headers: { 'content-type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const json = await res.json();
    return { status: res.status, json };
  };

  test('导入 -> 待定 -> 确认 -> 结果/边界/原始事件 全链路', async () => {
    let r = await api('POST', '/events/import', {
      events: [
        { eventId: 'h1', partition: 'H', seq: 1, eventTime: 100, arrivalTime: 95_000 },
        { eventId: 'h2', partition: 'H', seq: 2, eventTime: 200, arrivalTime: 95_100 },
      ],
    });
    assert.equal(r.status, 200);
    assert.equal(r.json.data.results.length, 2);

    r = await api('GET', '/pending?partition=H');
    assert.equal(r.json.data.drafts[0].events.length, 2);

    r = await api('POST', '/events/import', {
      events: [{ eventId: 'h3', partition: 'H', seq: 9, eventTime: 1700, arrivalTime: 96_000 }],
    });
    assert.ok(r.json.data.confirmedWindowIds.includes('win#H#0'));

    r = await api('GET', '/results?partition=H');
    const win = r.json.data.windows.find((w) => w.windowStart === 0);
    assert.deepEqual(win.events.map((e) => e.eventId), ['h1', 'h2']);
    assert.equal(win.events[0].eventTime, 100);
    assert.equal(win.events[0].arrivalTime, 95_000);

    r = await api('GET', '/boundaries');
    assert.ok(r.json.data.boundaries.length >= 1);
    const bid = r.json.data.boundaries.at(-1).boundaryId;
    r = await api('GET', `/boundaries/${bid}`);
    assert.equal(r.json.data.frontier, 1000);

    r = await api('GET', '/events/h1');
    assert.equal(r.json.data.attribution.kind, 'CONFIRMED_WINDOW');
  });

  test('过期迟到事件经 HTTP 留诊断且结果不变', async () => {
    let r = await api('POST', '/events/import', {
      events: [{ eventId: 'h-late', partition: 'H', seq: 77, eventTime: 150, arrivalTime: 99_000 }],
    });
    assert.equal(r.json.data.results[0].status, 'EXPIRED_LATE');

    r = await api('GET', '/diagnostics?partition=H&kind=EXPIRED_LATE');
    assert.equal(r.json.data.diagnostics.length, 1);
    assert.equal(r.json.data.diagnostics[0].windowId, 'win#H#0');
  });

  test('规则版本冲突经 HTTP 返回 409', async () => {
    const r = await api('POST', '/events/import', {
      expectedRuleVersion: 'v9',
      events: [{ eventId: 'h-stale', partition: 'H', seq: 100, eventTime: 5000, arrivalTime: 100_000 }],
    });
    assert.equal(r.status, 409);
    assert.equal(r.json.error.code, 'STALE_RULE');
  });

  test('/debug/replay 证明 HTTP 链路上的状态可重放', async () => {
    const r = await api('GET', '/debug/replay');
    assert.equal(r.status, 200);
    assert.equal(r.json.data.equal, true);
  });
});
