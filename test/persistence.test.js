// persistence.test.js —— 追加日志、快照恢复、内容哈希稳定性
import { test, describe, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { JournalStore, hashContent } from '../src/persistence.js';
import { ReorderKernel } from '../src/kernel.js';
import { makeKernel, ev, cleanup } from './helpers.js';

let dirs = [];
afterEach(() => dirs.forEach(cleanup));

async function makeAt(dir, options = {}) {
  return makeKernel({ dir, ...options });
}

describe('JournalStore', () => {
  test('内容哈希不受键顺序影响（重复判定的基础）', () => {
    assert.equal(hashContent({ a: 1, b: 2 }), hashContent({ b: 2, a: 1 }));
    assert.notEqual(hashContent({ a: 1 }), hashContent({ a: 2 }));
    assert.equal(hashContent([1, 2, 3]), hashContent([1, 2, 3]));
  });

  test('append 落盘、偏移单调；重启 load 重放同一批记录', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'persist-'));
    dirs.push(dir);
    const h = await makeAt(dir);
    await h.kernel.ingestOne(ev('p', 1, 100));

    const seen = [];
    const store2 = new JournalStore(dir);
    store2.load((entry) => seen.push(entry.type), () => {});
    assert.equal(seen[0], 'RULE_ACTIVATED');
    assert.equal(store2.nextOffset, h.store.nextOffset);

    const lines = readFileSync(join(dir, 'journal.jsonl'), 'utf8').split('\n').filter(Boolean);
    assert.equal(lines.length, h.store.nextOffset);
    assert.ok(lines.every((l) => JSON.parse(l)));
    h.kernel.close();
  });

  test('自动快照：重启时只重放快照之后的记录，窗口状态仍一致', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'persist-'));
    dirs.push(dir);
    const h = await makeAt(dir, { autoSnapshotEvery: 3 });
    await h.kernel.ingestBatch([ev('p', 1, 100), ev('p', 2, 4000)]);
    assert.ok(existsSync(join(dir, 'snapshot.json')));
    const snapshotOffset = JSON.parse(readFileSync(join(dir, 'snapshot.json'), 'utf8')).offset;

    let replayedAfterSnapshot = 0;
    const store2 = new JournalStore(dir);
    store2.load(
      (entry) => {
        if (entry._offset > snapshotOffset) replayedAfterSnapshot += 1;
      },
      () => {}
    );
    assert.equal(replayedAfterSnapshot, h.store.nextOffset - 1 - snapshotOffset);

    // 真正用内核 initialize 恢复（快照 + 增量重放）
    const k2 = new ReorderKernel(new JournalStore(dir), { clock: () => 0 });
    await k2.initialize();
    const win = k2.getWindow('p@0');
    assert.equal(win.status, 'CONFIRMED');
    assert.deepEqual(win.events.map((e) => e.eventId), ['p|1']);
    // p|2 是头事件（et=4000），自身尚未越过排序线，正确停在待定区
    assert.equal(k2.getEvent('p|1').status, 'CONFIRMED');
    assert.equal(k2.getEvent('p|2').status, 'PENDING_BUFFERED');
    k2.close();
    h.kernel.close();
  });

  test('快照偏移脱节（找不到后续记录）时安全降级为只恢复快照', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'persist-'));
    dirs.push(dir);
    const h = await makeAt(dir);
    await h.kernel.ingestOne(ev('p', 1, 100));
    await h.kernel.snapshot();

    const snapPath = join(dir, 'snapshot.json');
    const snap = JSON.parse(readFileSync(snapPath, 'utf8'));
    snap.offset += 100; // 模拟快照与日志脱节
    writeFileSync(snapPath, JSON.stringify(snap));

    const k2 = new ReorderKernel(new JournalStore(dir), { clock: () => 0 });
    await k2.initialize(); // 不应抛错
    assert.ok(k2.getEvent('p|1'));
    k2.close();
    h.kernel.close();
  });

  test('无快照文件时从 journal 全量重放', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'persist-'));
    dirs.push(dir);
    const h = await makeAt(dir);
    await h.kernel.ingestBatch([ev('p', 1, 100), ev('p', 2, 300), ev('p', 3, 4000)]);
    h.kernel.close();
    const { rmSync } = await import('node:fs');
    rmSync(join(dir, 'snapshot.json'), { force: true });

    const k2 = new ReorderKernel(new JournalStore(dir), { clock: () => 0 });
    await k2.initialize();
    assert.equal(k2.getWindow('p@0').status, 'CONFIRMED');
    assert.equal(k2.anomaliesView().anomalies.length, 0);
    k2.close();
  });
});
