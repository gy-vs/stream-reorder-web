/**
 * persistence.js — 可回放日志与快照
 *
 *  - journal.jsonl：所有状态变化逐条追加（每提交一个命令 fsync 一次，
 *    本地进程崩溃后可完整重放）。
 *  - snapshot.json：周期性状态快照，重启时 快照 + 快照之后的条目。
 *  - MemoryJournal：测试使用的内存实现，同一接口。
 *
 * 回放不是“重新解释输入”，而是按 seq 顺序规约同一份状态机条目，
 * 保证实时与回放产生逐字节一致的状态（Kernel.verifyReplay 验证）。
 */

import { promises as fs } from 'node:fs';
import path from 'node:path';
import { createWriteStream } from 'node:fs';

export class MemoryJournal {
  constructor() {
    this.lines = [];
    this.snapshot = null;
    this.appends = 0;
  }

  async append(entry) {
    this.lines.push(JSON.stringify(entry));
    this.appends += 1;
  }

  async readEntriesAfter(lastSeq) {
    return this.lines
      .map((l) => JSON.parse(l))
      .filter((e) => e.seq > lastSeq)
      .sort((a, b) => a.seq - b.seq);
  }

  async allEntries() {
    return this.lines.map((l) => JSON.parse(l)).sort((a, b) => a.seq - b.seq);
  }

  async writeSnapshot(snap) {
    this.snapshot = snap;
  }

  async loadSnapshot() {
    return this.snapshot;
  }
}

export class FileJournal {
  constructor(dir) {
    this.dir = dir;
    this.logPath = path.join(dir, 'journal.jsonl');
    this.snapPath = path.join(dir, 'snapshot.json');
    this._stream = null;
  }

  async init() {
    await fs.mkdir(this.dir, { recursive: true });
    this._stream = createWriteStream(this.logPath, { flags: 'a' });
    await new Promise((resolve, reject) => {
      this._stream.once('open', resolve);
      this._stream.once('error', reject);
    });
  }

  append(entry) {
    return new Promise((resolve, reject) => {
      this._stream.write(JSON.stringify(entry) + '\n', (err) =>
        err ? reject(err) : resolve()
      );
    });
  }

  async flush() {
    if (!this._stream) return;
    await new Promise((resolve, reject) => {
      this._stream.once('error', reject);
      this._stream.end(resolve);
    });
    this._stream = null;
  }

  async readEntriesAfter(lastSeq) {
    let raw;
    try {
      raw = await fs.readFile(this.logPath, 'utf8');
    } catch {
      return [];
    }
    const out = [];
    for (const line of raw.split('\n')) {
      if (!line.trim()) continue;
      const e = JSON.parse(line);
      if (e.seq > lastSeq) out.push(e);
    }
    out.sort((a, b) => a.seq - b.seq);
    return out;
  }

  async writeSnapshot(snap) {
    const tmp = this.snapPath + '.tmp';
    await fs.writeFile(tmp, JSON.stringify(snap));
    await fs.rename(tmp, this.snapPath);
  }

  async loadSnapshot() {
    try {
      return JSON.parse(await fs.readFile(this.snapPath, 'utf8'));
    } catch {
      return null;
    }
  }
}
