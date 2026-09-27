# 乱序事件重排与确认工作台（stream-reorder-web）

用于检查流处理系统在**迟到数据**出现时到底发布了什么的内核与本地 HTTP 服务。

调用方导入带 `eventTime`（业务时间）、`arrivalTime`（到达时间）、`partition`、`sequence`
的事件，设置允许迟到窗口，然后观察：哪些事件进了**待定区**、哪些结果已经**确认锁定**、
哪些迟到事件只能留下**诊断**。所有公开结果都把事件时间和到达时间分开显示——网络延迟
（`arrivalTime - eventTime`）不会被误当成业务时间错误。

- **后端真实保存**事件、分区状态、确认进度（追加日志 + 快照，不是静态 JSON）
- **状态变化即记录**：刷新结果视图看到的是完整时间线，而不是只剩最终数字
- **确认不可静默改写**：已确认结果不会被一个旧事件回来改掉；旧事件只进诊断
- **未确认可重算**：规则版本切换后，未确认窗口可按新版本重算，并保留每次修订
- **乐观并发**：旧客户端携带 `expectedRuleVersion`，过期重算/改规则返回 409
- **不全量扫描**：确认/查询只走分区窗口索引，时间线/回放按偏移读取

## 运行

要求 Node.js >= 20（无第三方依赖）。

```bash
npm test                 # 39 个测试：迟到/重复/分区隔离/规则版本/并发刷新/回放
node examples/demo.mjs   # 进程内消费端完整场景演示
npm start                # HTTP 服务，默认 http://localhost:3000，数据落 ./data
# 自定义：node src/server.js --port 4000 --data ./data
```

## 概念模型

每个分区独立维护三条边界（默认 `allowedLateness = L`）：

| 边界 | 计算 | 含义 |
| --- | --- | --- |
| 头事件时间 `head` | 分区已观察到的最大 `eventTime` | 水位来源 |
| 排序线 `orderLine` | `head - L` | `eventTime > orderLine` 的事件先进**待定区** |
| 确认线 `confirmLine` | `head - 2L` | 跨过确认线的结果**永久锁定** |

事件落点三态（不是一个布尔值）：

- `PENDING_BUFFERED`：比排序线新，等待水位推进；
- `LATE_TOLERATED`：晚于排序线但早于确认线，进入未确认窗口并触发一次带痕迹的重排；
- `LATE_DIAGNOSTIC`：晚于确认线（或目标窗口已确认），只写诊断，绝不触碰已发布结果。

事件状态最终走向 `ORDERED → CONFIRMED`，每个事件都带完整 `lifecycle`
（IMPORTED / BUFFERED / ORDERED / CONFIRMED / DUPLICATE_IGNORED / CONFLICT_REJECTED / LATE_DIAGNOSTIC），
每个生命周期节点都能定位到产生它的日志偏移 `atOffset`。

窗口状态：`OPEN → ORDERED（可重算）→ CONFIRMED（锁定）`。每次发布/重排都是一条
`WINDOW_REVISED`，带 `revision`、`globalRevision`、`reason`（WATERMARK_PUBLISH /
BUFFER_FLUSH / LATE_ARRIVAL / MANUAL_RECOMPUTE）和当时的 `ruleVersion`，即“顺序的来源与版本”。

分区空闲：超过 `idleTimeout` 没有活动的分区标记 `IDLE`，暂时不再拖低全局水位；
新事件到达自动恢复 `ACTIVE`。

## 作为模块使用

```js
import { ReorderKernel, JournalStore, ORDER_BY_SEQUENCE } from './src/index.js';

const kernel = new ReorderKernel(new JournalStore('./data'), {
  clock: () => Date.now(), // 可注入确定性时钟
});
await kernel.initialize();

const r = await kernel.ingestOne({
  partition: 'orders',
  sequence: 1,
  eventTime: 100,     // 业务时间
  arrivalTime: 1900,  // 到达时间；networkDelay = 1800
  payload: { amount: 5 },
});

kernel.resultView();        // 结果视图：水位、分区、窗口（两种时间分开）
kernel.pendingView();       // 待定事件详情与等待原因
kernel.eventDetailView(id); // 事件 → 窗口、异常、生命周期
kernel.timelineView();      // 可回放状态记录
kernel.anomaliesView();     // LATE_DIAGNOSTIC / DUPLICATE / CONFLICT / LATE_REVISION

await kernel.activateRule({ orderBy: ORDER_BY_SEQUENCE }, { expectedRuleVersion: 1 });
await kernel.recomputeWindow('orders@0', { expectedRuleVersion: 2 });
```

重复与冲突的明确语义：

- **相同事件重传**（同 `partition|sequence` 且内容哈希一致）：幂等忽略，记 `DUPLICATE`；
- **同序列号内容不同**：首写获胜，拒绝内容（payload/哈希/到达时间）保留为 `CONFLICT` 诊断，不占新身份；
- 内容哈希对 JSON 键序不敏感（`{a:1,b:2}` 与 `{b:2,a:1}` 视为相同）。

## HTTP 接口

所有响应带 `asOfOffset`（数据版本）；GET 读已提交快照，POST 进入内核串行队列。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/events` | 导入：`{events:[...], arrivalTime?}`（也支持单个 `event`） |
| POST | `/api/tick` | 推进系统时钟，重算空闲状态：`{arrivalTime?}` |
| POST | `/api/heartbeat` | 分区心跳（分区暂时没有数据也能推进活动时间） |
| GET | `/api/results?partition=` | 结果视图：规则、全局/分区水位、窗口、计数 |
| GET | `/api/pending?partition=` | 待定事件详情、等待原因、将落入的窗口 |
| GET | `/api/partitions/:name` | 单分区：状态、窗口、待定、全部事件 |
| GET | `/api/windows/:key` | 窗口详情（含每次修订与确认边界偏移） |
| POST | `/api/windows/:key/recompute` | 重算未确认窗口，body 可带 `expectedRuleVersion` |
| GET | `/api/events/:id` | 事件详情（两种时间、生命周期、关联异常、所属窗口） |
| GET | `/api/timeline?fromOffset=&limit=&partition=` | 可回放时间线（投影后的关键状态变化） |
| GET | `/api/replay?fromOffset=&toOffset=&limit=` | 原始状态记录回放 |
| GET | `/api/anomalies?kind=&partition=` | 异常归属 |
| GET | `/api/rules` / POST `/api/rules` | 规则版本链 / 切换新版本 |
| POST | `/api/snapshot` | 立即落快照 |

错误：`400 BAD_EVENT/BAD_RULE`、`404 *_NOT_FOUND`、`409 RULE_VERSION_CONFLICT`、
`409 WINDOW_CONFIRMED_LOCKED`。

### 快速试用

```bash
curl -s localhost:3000/api/results
curl -s -X POST localhost:3000/api/events -H 'content-type: application/json' \
  -d '{"events":[{"partition":"p","sequence":1,"eventTime":100,"arrivalTime":100,"payload":{"v":1}}]}'
```

## 存储与回放

- `data/journal.jsonl`：仅追加的状态记录（每条带单调 `_offset`），是唯一事实来源。
- `data/snapshot.json`：周期性原子快照（临时文件 + rename）。启动时先恢复快照，
  再只重放其后的增量记录；没有快照则全量重放；快照与日志脱节时安全降级。
- 查询层时间线/回放直接按 `_offset` 读内存记录，确认路径只遍历该分区窗口引用的事件，
  不会每次从头扫描全部事件（见 `test/query-path.test.js`）。

## 代码结构

```
src/
  kernel.js        事件溯源内核：边界计算、三态落点、窗口修订、确认、规则版本、查询投影
  persistence.js   追加日志 + 快照（稳定内容哈希、原子快照）
  server.js        本地 HTTP 接口
  index.js         模块出口
examples/demo.mjs  消费端进程内完整场景
test/              内核 / HTTP / 持久化回放 / 查询路径 共 39 个测试
```
