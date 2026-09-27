// demo.mjs —— 进程内消费端交互演示（不依赖 HTTP，直接使用内核模块）
//
// 运行：node examples/demo.mjs
// 场景：一个分区里事件乱序到达，观察“待定 → 重排 → 确认 → 迟到仅诊断”，
//      以及规则版本切换后如何重算、如何沿确认边界跳回原始事件。
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  ReorderKernel,
  JournalStore,
  ORDER_BY_SEQUENCE,
} from '../src/index.js';

const dir = mkdtempSync(join(tmpdir(), 'reorder-demo-'));
let clock = 0;
const kernel = new ReorderKernel(new JournalStore(dir, { autoSnapshotEvery: 50 }), {
  clock: () => clock,
});
await kernel.initialize();

const E = (sequence, eventTime, arrivalTime = eventTime, payload = { amount: sequence }) => ({
  partition: 'orders',
  sequence,
  eventTime,
  arrivalTime,
  payload,
});

const line = (s) => console.log(`\n=== ${s} ===`);

line('1) 两个早到事件：eventTime 还没越过排序线，进入待定区');
await kernel.ingestOne(E(1, 100, 100));
await kernel.ingestOne(E(2, 300, 320));
console.log('待定:', kernel.pendingView().pending.map((e) => e.eventId));

line('2) 头事件推进到 2000：排序线=1000，早到事件按事件时间排出（未确认）');
await kernel.ingestOne(E(3, 2000, 2000));
printWindow('orders@0');

line('3) 迟到事件 et=250 在 2200 才到：晚于排序线、早于确认线 → 容忍区内重排');
await kernel.ingestOne(E(4, 250, 2200));
printWindow('orders@0');

line('4) 规则切换为“按序列号排序”：未确认窗口只标记过期，不自动改写');
const v2 = await kernel.activateRule({ orderBy: ORDER_BY_SEQUENCE }, { expectedRuleVersion: 1 });
console.log('当前规则版本:', v2.rule.ruleVersion, '过期窗口:', v2.staleWindows);
printWindow('orders@0');

line('5) 调用方显式重算该窗口（携带 expectedRuleVersion=2）');
const rec = await kernel.recomputeWindow('orders@0', { expectedRuleVersion: 2 });
console.log('顺序变为:', rec.window.events.map((e) => `${e.eventId}(et=${e.eventTime})`).join(' '));

line('6) 旧版本重算会被拒绝（防止旧结果覆盖新规则）');
try {
  await kernel.recomputeWindow('orders@0', { expectedRuleVersion: 1 });
} catch (e) {
  console.log(`${e.status} ${e.code}: ${e.message}`);
}

line('7) 水位推进到确认线之外：窗口锁定');
await kernel.ingestOne(E(5, 4000, 4000));
printWindow('orders@0');

line('8) 更老的事件 et=150 在 4100 才到：晚于确认线，只留诊断');
const diag = await kernel.ingestOne(E(6, 150, 4100));
console.log('迟到诊断:', diag.lateDiagnostic.eventId, diag.lateDiagnostic.status);
console.log('已确认窗口顺序保持不变，revision =', kernel.getWindow('orders@0').revision);

line('9) 从确认边界跳回原始事件');
const tl = kernel.timelineView({ limit: 500 });
const boundary = tl.entries
  .filter((e) => e.type === 'CONFIRM_BOUNDARY' && e.windowKey === 'orders@0')
  .at(-1);
console.log('确认边界 @offset', boundary.offset, '锁定', boundary.lockedEventIds);
for (const id of boundary.lockedEventIds) {
  const { event } = kernel.eventDetailView(id);
  console.log(
    `  原始事件 ${id}: eventTime=${event.eventTime} arrivalTime=${event.arrivalTime}`,
    `网络延迟=${event.networkDelay} 生命周期=${event.lifecycle.map((l) => l.transition).join('→')}`
  );
}

line('10) 状态变化是可回放记录（节选时间线类型）');
console.log(tl.entries.map((e) => e.type).join(' , '));

kernel.close();
rmSync(dir, { recursive: true, force: true });

function printWindow(key) {
  const w = kernel.resultView().windows.find((x) => x.windowKey === key);
  if (!w) return console.log('  (窗口尚未建立)');
  console.log(
    `  ${key} [${w.status}] revision=${w.revision} rule=v${w.publishedRuleVersion}`,
    w.staleForRule ? `staleFor=v${w.staleForRule}` : '',
    '\n  顺序:',
    w.events
      .map((e) => `${e.eventId}(et=${e.eventTime},arrival=${e.arrivalTime},${e.confirmed ? '已确认' : '未确认'})`)
      .join(' ')
  );
}
