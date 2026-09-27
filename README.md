# 乱序事件重排与确认工作台（Reorder & Confirm Workbench）

检查流处理系统在**迟到数据**出现时到底发布了什么：导入带事件时间、到达时间、分区和序列号的事件，
设定允许迟到窗口，观察哪些事件进入待定区、哪些窗口结果已经确认、哪些迟到事件只能留下诊断。

- **事件时间(eventTime) 与到达时间(arrivalTime) 永远分开、平级显示**，
  并给出 `transportDelayMs = arrivalTime − eventTime`，避免把网络延迟误读成业务时间错误。
- **确认结果不可变**：已确认窗口冻结在确认那一刻的规则版本与事件顺序上；旧事件回来只会
  产生 `EXPIRED_LATE` 诊断，不能无提示改写结果。
- **未确认部分可按规则重算**：规则（窗口大小、允许迟到、排序方式、空闲超时）是有版本的，
  新版本只重排未确认尾部。
- **状态变化是一条可回放的日志（journal）**：刷新结果视图看到的不是最终数字，而是每次
  水位推进、草稿构建/作废、窗口确认、全局发布边界；`verifyReplay` 证明实时状态能从日志逐条规约复现。

零第三方依赖，Node.js ≥ 18。

## 快速开始

```bash
npm test                   # 28 个测试：迟到/重复/分区隔离/规则版本/并发/回放/HTTP
npm start                  # http://127.0.0.1:3000 （内存模式）
DATA_DIR=./data npm start  # journal.jsonl + snapshot.json 持久化，重启自动恢复
npm run demo               # 不走 HTTP 的状态链路演示
```

## 状态模型

```
导入批次 BATCH_IMPORT
  └─ 每个事件 EVENT_RECEIVED，四种明确分类：
       ACCEPTED      进入分区有序索引，参与重排
       DUPLICATE     相同事件重传（同 eventId，或同 partition+seq 且业务内容一致）
       CONFLICT      同 partition+seq 但内容不同 → 隔离后来者，保留先到者 + 诊断
       EXPIRED_LATE  事件时间早于确认冻结边界 → 不入结果，仅诊断（归属到被冻结的窗口）
       （ACCEPTED 上另有 late=true：晚于当前水位但窗口尚未确认，仍可进待定区重排）
  └─ WM_ADVANCED  分区水位 = max(已见事件时间) − allowedLatenessMs（单调不减）
  └─ DRAFT_BUILT / DRAFT_RESET  待定窗口按当前规则版本重排
  └─ DIAGNOSTIC   SEQ_GAP / SEQ_CONTENT_CONFLICT / EXPIRED_LATE / RECOMPUTE_REJECTED
  └─ WINDOW_CONFIRMED  水位越过窗口末端 → 确认，结果（顺序+规则版本+哈希）冻结
  └─ GLOBAL_BOUNDARY   跨分区发布边界
```

- **分区状态**：`NO_DATA → ACTIVE → IDLE`。到达时间超过 `idlePartitionTimeoutMs` 无数据的分区
  在 `TICK` 后标记 IDLE，不再阻塞全局发布；新事件到达自动复活。
- **全局发布边界**：所有参与中分区冻结边界的最小值。`NO_DATA`/有数据但无确认的分区作为
  `blockers` 显式列出（“暂时没数据”是明确状态，不是“无异常”）；IDLE 分区已确认的部分仍贡献边界。
- **窗口网格**：每个分区以其首个事件对齐网格；规则切换窗口尺寸时，已确认窗口保留，
  第一个尾部草稿从冻结边界开始（可能是一个短窗口），其后的窗口落在新网格上。

## 查询层（只读，一致快照）

| 接口 | 含义 |
| --- | --- |
| `GET /timeline?basis=event\|arrival&partition=&from=&to=` | 时间线，可按业务时间或到达时间排序，每条带归属 |
| `GET /pending?partition=` | 待定草稿：窗口、规则版本、水位、可确认性、可见序号空洞、内部事件 |
| `GET /results?partition=` | 已确认窗口（冻结的规则版本、顺序、事件双时间）+ 全部历史全局边界 |
| `GET /boundaries` / `GET /boundaries/:id` | 每个发布边界都保留；详情可一路跳到窗口与原始事件 |
| `GET /events/:id` | 原始事件事实（含重传收据）与归属：CONFIRMED_WINDOW / PENDING_DRAFT / DIAGNOSTIC_ONLY |
| `GET /partitions` | 分区状态、水位、冻结边界、待定/已确认计数 |
| `GET /rules` | 规则版本列表与当前版本 |
| `GET /diagnostics?kind=&partition=` | 迟到、冲突、序号空洞、重算被拒 |
| `GET /journal?since=` | 可回放的状态变化记录 |
| `GET /debug/replay` | 从 journal 重新规约并逐字段比对实时状态 |
| `GET /debug/stats` | 区间读取/全表扫描计数（验证不做全表扫描） |

写接口：`POST /events/import`、`POST /rules`、`POST /recompute`、`POST /tick`、`POST /checkpoint`。

## 并发与规则版本守卫

所有写命令进入内核的串行队列。写请求可携带 `expectedRuleVersion`：当处理过程中规则已变化，
旧的导入/重排选择会收到 **409 `STALE_RULE`**，不会把旧结果覆盖到新规则上。手动重算命中已确认
窗口时收到 **409 `IMMUTABLE_RESULT`** 并留 `RECOMPUTE_REJECTED` 诊断。

重算/导入只按分区做**区间读取**（二分定位 `readRange/readFrom`），不从头扫描全部事件；
`/debug/stats` 暴露 `rangeReads` 与 `fullScans` 计数。

## 直接作为模块使用

```js
import { Kernel, MemoryJournal } from './src/index.js';

const kernel = new Kernel({ journal: new MemoryJournal() });
await kernel.importEvents([
  { eventId: 'e2', partition: 'P', seq: 2, eventTime: 200, arrivalTime: 1010 },
  { eventId: 'e1', partition: 'P', seq: 1, eventTime: 100, arrivalTime: 1020 }, // 乱序
]);
kernel.pending();      // 待定区：e1,e2（按 seq 重排）
await kernel.importEvents([
  { eventId: 'e9', partition: 'P', seq: 9, eventTime: 1500, arrivalTime: 1510 },
]);                    // 水位越过窗口末端 → 确认 [0,1000)
kernel.results();      // 冻结结果，事件上 eventTime/arrivalTime 平级
kernel.verifyReplay(); // { equal: true, ... }
```

## 目录

```
src/
  util.js         纯函数：指纹、二分区间、窗口对齐、稳定排序
  event-store.js  到达登记（四类状态+重传收据）、分区有序索引、区间读取与扫描统计
  rules.js        有版本的规则注册表
  kernel.js       状态机：journal 规约、水位、草稿、确认、诊断、全局边界、查询视图、回放
  persistence.js  MemoryJournal / FileJournal（journal.jsonl + snapshot.json）
  http-server.js  原生 http 接口
  app.js          组装
test/             node:test（5 个场景文件 + helpers）
scripts/demo.js
server.js
```
