#!/usr/bin/env node
/**
 * scripts/demo.js — 不走 HTTP 的本地状态链路演示
 *
 * 场景：两个分区、乱序 + 迟到 + 重传 + 规则版本变化，
 * 打印事件时间/到达时间分离的视图、待定区、确认边界、诊断与回放校验。
 */

import { Kernel, MemoryJournal, createClock } from '../src/index.js';

const clock = createClock(() => 100_000);
const kernel = new Kernel({ clock, journal: new MemoryJournal() });

const E = (eventId, partition, seq, eventTime, arrivalTime, data = null) => ({
  eventId, partition, seq, eventTime, arrivalTime, data,
});

const show = (title, value) => {
  console.log(`\n===== ${title} =====`);
  console.log(JSON.stringify(value, null, 2));
};

// 1) 乱序导入：先到 seq=2，再到 seq=1（事件时间更早）
await kernel.importEvents([
  E('evt-2', 'orders', 2, 10_200, 10_020),
  E('evt-1', 'orders', 1, 10_100, 10_030),
  E('pay-1', 'payments', 1, 10_150, 10_040),
]);

show('待定区（按 seq 重排后）', kernel.pending().drafts.map((d) => ({
  window: [d.windowStart, d.windowEnd],
  partition: d.partition,
  order: d.events.map((e) => `${e.eventId}@et=${e.eventTime},at=${e.arrivalTime}`),
})));

// 2) 推进水位，确认窗口 [10000,11000)
await kernel.importEvents([E('evt-9', 'orders', 9, 11_600, 10_200)]);
await kernel.importEvents([E('pay-9', 'payments', 9, 11_600, 10_210)]);

show('已确认窗口（冻结在 v1）', kernel.results().windows.map((w) => ({
  windowId: w.windowId,
  ruleVersion: w.ruleVersion,
  order: w.events.map((e) => e.eventId),
})));

show('全局发布边界', kernel.boundaries().map((b) => ({
  id: b.boundaryId,
  frontier: b.frontier,
  blockers: b.blockers.map((x) => x.reason),
  windows: b.publishedWindows.map((x) => x.windowId),
})));

// 3) 旧事件迟到：窗口已冻结 -> 只能留诊断
await kernel.importEvents([E('evt-late', 'orders', 50, 10_400, 12_000)]);

// 4) 相同事件重传 + 同序号不同内容
await kernel.importEvents([E('evt-1', 'orders', 1, 10_100, 12_100)]);
await kernel.importEvents([E('evt-2-evil', 'orders', 2, 10_200, 12_200, { tampered: true })]);

// 5) 规则版本变化：只重排未确认尾部
await kernel.registerRule({
  windowSizeMs: 1000,
  allowedLatenessMs: 500,
  orderBy: 'event-time',
});

show('诊断（迟到/冲突/序号空洞）', kernel.diagnostics().map((d) => ({
  kind: d.kind,
  partition: d.partition,
  windowId: d.windowId ?? null,
  message: d.message,
})));

show('时间线（按到达时间，排查网络延迟）', kernel.timeline({ basis: 'arrival' }).events.map((e) => ({
  eventId: e.eventId,
  status: e.status,
  eventTime: e.eventTime,
  arrivalTime: e.arrivalTime,
  transportDelayMs: e.transportDelayMs,
  attribution: e.attribution.kind,
})));

show('回放校验（实时状态 === 逐条规约）', kernel.verifyReplay());
