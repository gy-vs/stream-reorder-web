// persistence.js —— 仅追加（append-only）状态日志 + 快照
//
// 所有状态变化都以一条不可变记录写入 journal.jsonl；每隔一段写入一次快照，
// 重启时从最近快照重放其后的记录，而不是从头扫全部事件。
import { createHash } from 'node:crypto';
import {
  mkdirSync,
  readFileSync,
  writeFileSync,
  appendFileSync,
  renameSync,
  existsSync,
} from 'node:fs';
import { join } from 'node:path';

export function hashContent(value) {
  const json = stableStringify(value);
  return createHash('sha256').update(json).digest('hex').slice(0, 16);
}

// 稳定序列化：对象键排序后再序列化，保证 {a:1,b:2} 与 {b:2,a:1} 哈希一致
export function stableStringify(value) {
  return JSON.stringify(toStable(value));
}

function toStable(value) {
  if (value === null || typeof value !== 'object') return value ?? null;
  if (Array.isArray(value)) return value.map(toStable);
  const out = {};
  for (const key of Object.keys(value).sort()) out[key] = toStable(value[key]);
  return out;
}

export class JournalStore {
  constructor(dir = './data', { autoSnapshotEvery = 100 } = {}) {
    this.dir = dir;
    this.autoSnapshotEvery = autoSnapshotEvery;
    this.journalPath = join(dir, 'journal.jsonl');
    this.snapshotPath = join(dir, 'snapshot.json');
    this.nextOffset = 0; // 下一条记录的偏移（也是已提交记录数）
    this.entries = []; // 内存中的全部状态记录（查询层按偏移读，不扫事件、不重读文件）
  }

  ensureDir() {
    mkdirSync(this.dir, { recursive: true });
  }

  // 启动时加载：先用快照恢复内存状态，再重放快照之后的增量记录。
  // apply(entry) 由内核提供；restore(state) 把快照恢复成内核内存结构。
  load(apply, restore) {
    this.ensureDir();
    let entries = [];
    let snapshot = null;
    if (existsSync(this.snapshotPath)) {
      snapshot = JSON.parse(readFileSync(this.snapshotPath, 'utf8'));
    }
    if (existsSync(this.journalPath)) {
      const lines = readFileSync(this.journalPath, 'utf8')
        .split('\n')
        .filter((line) => line.trim());
      entries = lines.map((line) => JSON.parse(line));
    }
    this.entries = entries;

    let startIndex = 0;
    if (snapshot) {
      restore(snapshot.state);
      // 只重放快照之后的记录
      startIndex = entries.findIndex((e) => e._offset === snapshot.offset + 1);
      if (startIndex === -1) startIndex = entries.length;
    }

    for (let i = startIndex; i < entries.length; i++) {
      apply(entries[i]);
    }

    this.nextOffset = entries.length ? entries[entries.length - 1]._offset + 1 : 0;
    return { snapshot, replayedCount: entries.length - startIndex };
  }

  // 查询/回放入口：直接返回内存记录（按 _offset 升序）
  readEntries() {
    return this.entries;
  }

  // 追加一条记录并落盘。entry 为内核构造的状态变化记录（不含 _offset）。
  append(entry) {
    this.ensureDir();
    const stored = { ...entry, _offset: this.nextOffset };
    appendFileSync(this.journalPath, JSON.stringify(stored) + '\n');
    this.entries.push(stored);
    this.nextOffset += 1;
    return stored;
  }

  // 保存快照（原子写入：先写临时文件再 rename）。
  saveSnapshot(offset, state) {
    this.ensureDir();
    const payload = JSON.stringify({ offset, state });
    const tmp = `${this.snapshotPath}.tmp`;
    writeFileSync(tmp, payload);
    renameSync(tmp, this.snapshotPath);
  }

  close() {
    // 当前为同步落盘，无需额外收尾；保留接口给调用方表达生命周期。
  }
}
