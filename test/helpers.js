/**
 * 测试辅助：可控时钟 + 常用断言
 */

import assert from 'node:assert/strict';
import { Kernel, MemoryJournal } from '../src/index.js';

export function controlledClock(start = 1_000_000) {
  let t = start;
  return {
    now: () => t,
    advance(ms) {
      t += ms;
      return t;
    },
    set(ms) {
      t = ms;
      return t;
    },
  };
}

export function makeKernel(clock) {
  const journal = new MemoryJournal();
  return { kernel: new Kernel({ clock, journal }), journal };
}

/** 用显式到达时间导入，避免依赖时钟顺序 */
export async function ingest(kernel, events) {
  return kernel.importEvents(
    events.map((e) => ({ arrivalTime: e.eventTime + 10, ...e }))
  );
}

export { assert };
