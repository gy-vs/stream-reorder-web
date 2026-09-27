// 测试辅助：临时目录 + 确定性时钟的内核
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ReorderKernel } from '../src/kernel.js';
import { JournalStore } from '../src/persistence.js';

export function tempDir() {
  return mkdtempSync(join(tmpdir(), 'reorder-test-'));
}

export function cleanup(dir) {
  rmSync(dir, { recursive: true, force: true });
}

// clock: 返回一个函数，同时暴露 set(t)/advance(ms) 控制“系统时间”
export function fakeClock(start = 0) {
  let t = start;
  const fn = () => t;
  fn.set = (v) => {
    t = v;
    return fn;
  };
  fn.advance = (ms) => {
    t += ms;
    return fn;
  };
  return fn;
}

export async function makeKernel(options = {}) {
  const dir = options.dir || tempDir();
  const clock = options.clock || fakeClock(0);
  const store = new JournalStore(dir, { autoSnapshotEvery: options.autoSnapshotEvery ?? 100 });
  const kernel = new ReorderKernel(store, { clock, rule: options.rule });
  await kernel.initialize();
  return { kernel, store, dir, clock, dispose: () => cleanup(dir) };
}

// 方便构造事件。arrival 默认等于 eventTime（无网络延迟），迟到测试里显式放大差值。
export function ev(partition, sequence, eventTime, arrivalTime = eventTime, payload = { v: sequence }) {
  return { partition, sequence, eventTime, arrivalTime, payload };
}

// 用一个全新内核重放同一数据目录，验证“从头重放 == 快照恢复”
export async function reopen(dir, clock = fakeClock(0)) {
  const store = new JournalStore(dir, { autoSnapshotEvery: 100 });
  const kernel = new ReorderKernel(store, { clock });
  await kernel.initialize();
  return { kernel, store };
}

export function ids(win) {
  return win.events.map((e) => e.eventId);
}
