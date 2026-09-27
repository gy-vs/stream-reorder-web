/**
 * kernel.js — 乱序重排与确认内核
 *
 * 状态链路（全部状态变化都是 journal 里一条带序号的记录，可逐条回放）：
 *
 *   导入事件 BATCH_IMPORT
 *     └─ 每个事件 EVENT_RECEIVED (ACCEPTED | DUPLICATE | CONFLICT | EXPIRED_LATE)
 *           ├─ accepted 后可能 IDLE_CHANGED（分区复活）
 *           └─ WM_ADVANCED（分区水位：rawMax − allowedLateness，单调）
 *     └─ 每个受影响分区 pump：
 *           ├─ DRAFT_RESET（规则版本/窗口尺寸变化，旧草稿作废）
 *           ├─ DRAFT_BUILT（按当前规则版本重排的待定窗口）
 *           ├─ DIAGNOSTIC(SEQ_GAP...)（确认时发现序号空洞）
 *           └─ WINDOW_CONFIRMED（确认边界，结果按确认时规则冻结）
 *     └─ GLOBAL_BOUNDARY（跨分区可发布边界，含阻塞原因与发布窗口清单）
 *
 *   规则变化 RULE_ADDED → 旧草稿全部作废、按新版本重排未确认尾部；
 *                         已确认窗口不受影响。
 *   手动重算 RECOMPUTE → 只允许作用于未确认部分；命中已确认窗口返回 409 并记诊断。
 *   时钟 TICK → 只影响分区 IDLE 判定与全局边界，绝不凭空推进事件时间水位。
 *
 * 实时执行与回放共用同一个 reduceEntry：实时路径先“生成”完整的派生条目序列，
 * 回放路径只按序 reduce。Kernel.verifyReplay() 比对两端状态，证明链路可重现。
 */

import { EventStore } from './event-store.js';
import { RuleRegistry, DEFAULT_RULE } from './rules.js';
import {
  contentHash,
  fingerprintEvent,
  businessFingerprint,
  eventOrderCompare,
  windowStartOf,
  deterministicId,
  assert,
  clone,
  createClock,
} from './util.js';

export const STATUS = Object.freeze({
  ACCEPTED: 'ACCEPTED', // 进入有序索引，参与重排
  DUPLICATE: 'DUPLICATE', // 相同事件重传（eventId 相同，或同键同指纹）
  CONFLICT: 'CONFLICT', // 同 (partition,seq) 但内容不同
  EXPIRED_LATE: 'EXPIRED_LATE', // 晚于确认边界，只留诊断，不能改写结果
});

/** 迟到但可恢复（已过当前水位、但其窗口尚未确认）的标记放在 ACCEPTED 记录上 */
const NEG_INF = null; // 状态里用 null 表示 −∞（JSON 友好）
const num = (v) => (v === null || v === undefined ? -Infinity : v);
const ser = (v) => (v === -Infinity || v === undefined ? null : v);

// ---------------------------------------------------------------------------
// 初始状态
// ---------------------------------------------------------------------------

export function createInitialState() {
  return {
    nextSeq: 0,
    entries: [], // 内存中的完整 journal（持久化由 persistence 负责）
    store: new EventStore(),
    rules: new RuleRegistry(DEFAULT_RULE),

    /** partition -> 分区运行时状态 */
    parts: new Map(),
    /** `${partition}#${windowStart}` -> 草稿（待定窗口） */
    drafts: new Map(),
    /** winId -> 已确认结果（冻结） */
    confirmations: new Map(),
    /** 已确认窗口按确认先后的 winId 顺序 */
    confirmedOrder: [],
    /** 诊断记录 id -> diag */
    diagnostics: new Map(),
    diagnosticOrder: [],

    /** 全局发布边界状态 */
    globalFrontier: null, // null = 尚不存在有限边界
    globalSig: null,
    globalBoundaryCount: 0,
    lastTick: null,
  };
}

function ensurePart(state, partition) {
  let p = state.parts.get(partition);
  if (!p) {
    p = {
      partition,
      status: 'NO_DATA', // NO_DATA | ACTIVE | IDLE
      rawMax: NEG_INF,
      currentWatermark: NEG_INF,
      frozenUntil: NEG_INF,
      originMs: null,
      firstEventTime: null,
      lastSeenArrival: null,
      lastConfirmedStart: null,
      draftEpoch: 0,
      /** 已接受序号的有序集合（快照中以数组保存） */
      acceptedSeqs: [],
      /** 已经报告过的空洞区间（避免多个窗口确认时重复报同一空洞） */
      reportedGaps: [],
    };
    state.parts.set(partition, p);
  }
  return p;
}

// ---------------------------------------------------------------------------
// 条目规约：实时与回放的唯一状态修改入口
// ---------------------------------------------------------------------------

export function reduceEntry(state, entry) {
  const s = state.store;
  switch (entry.type) {
    case 'BATCH_IMPORT': {
      s.registerBatch(entry.batchId, entry.eventIds, 'APPLIED', entry.ts);
      break;
    }

    case 'EVENT_RECEIVED': {
      const { event, status, detail } = entry;
      const part = ensurePart(state, event.partition);
      s.registerArrival(event, status, detail);
      if (status === STATUS.ACCEPTED) {
        s.indexAccepted(event);
        if (!part.acceptedSeqs.includes(event.seq)) {
          part.acceptedSeqs.push(event.seq);
          part.acceptedSeqs.sort((a, b) => a - b);
        }
        if (part.rawMax === NEG_INF) {
          part.firstEventTime = event.eventTime;
          part.originMs = windowStartOf(
            event.eventTime,
            state.rules.current().windowSizeMs
          );
          part.status = 'ACTIVE';
        }
        part.rawMax = ser(Math.max(num(part.rawMax), event.eventTime));
        part.lastSeenArrival = Math.max(part.lastSeenArrival ?? -Infinity, event.arrivalTime);
      }
      break;
    }

    case 'IDLE_CHANGED': {
      const p = state.parts.get(entry.partition);
      if (p) p.status = entry.status;
      break;
    }

    case 'WM_ADVANCED': {
      const p = state.parts.get(entry.partition);
      if (p) p.currentWatermark = ser(Math.max(num(p.currentWatermark), entry.watermark));
      break;
    }

    case 'RULE_ADDED': {
      // 种子 v1 与构造器初始注册表内容一致（幂等）；其余版本正常注册
      if (!state.rules.rules.has(entry.version)) {
        state.rules.register(entry.spec, { version: entry.version, now: entry.ts });
      }
      state.rules.currentVersion = entry.version;
      break;
    }

    case 'DRAFT_RESET': {
      state.drafts.delete(entry.key);
      const p = state.parts.get(entry.partition);
      if (p && entry.bumpEpoch) p.draftEpoch += 1;
      break;
    }

    case 'DRAFT_BUILT': {
      state.drafts.set(entry.draft.key, { ...clone(entry.draft), builtAt: entry.ts });
      break;
    }

    case 'DIAGNOSTIC': {
      if (!state.diagnostics.has(entry.diag.id)) {
        state.diagnostics.set(entry.diag.id, clone(entry.diag));
        state.diagnosticOrder.push(entry.diag.id);
        if (entry.diag.kind === 'SEQ_GAP') {
          const p = state.parts.get(entry.diag.partition);
          if (p) p.reportedGaps.push([entry.diag.gap.from, entry.diag.gap.to]);
        }
      }
      break;
    }

    case 'WINDOW_CONFIRMED': {
      const { result } = entry;
      state.confirmations.set(result.id, clone(result));
      state.confirmedOrder.push(result.id);
      state.drafts.delete(`${result.partition}#${result.windowStart}`);
      const p = state.parts.get(result.partition);
      if (p) {
        p.frozenUntil = ser(Math.max(num(p.frozenUntil), result.windowEnd));
        p.lastConfirmedStart = result.windowStart;
      }
      break;
    }

    case 'GLOBAL_BOUNDARY': {
      state.globalFrontier = entry.frontier;
      state.globalSig = entry.sig;
      state.globalBoundaryCount = entry.count;
      break;
    }

    case 'TICK': {
      state.lastTick = entry.now;
      break;
    }

    case 'RECOMPUTE': {
      // 尝试本身留痕；是否被拒绝看 entry.rejected
      break;
    }

    default:
      assert(false, `未知 journal 条目类型: ${entry.type}`);
  }
  return state;
}

// ---------------------------------------------------------------------------
// 内核外观：串行化命令、生成派生条目、提供查询视图
// ---------------------------------------------------------------------------

export class Kernel {
  constructor({ clock = null, journal = null, seed = true } = {}) {
    this.state = createInitialState();
    this.clock = clock ?? createClock();
    this.journal = journal; // persistence.Journal 实例（可选）
    this._chain = Promise.resolve();
    this._restoredSeq = 0;
    if (seed) {
      // 默认规则 v1 也是 journal 的第一条记录：从头回放时不依赖构造器隐式状态
      this._emit({
        ts: 0,
        type: 'RULE_ADDED',
        version: 'v1',
        spec: {
          windowSizeMs: DEFAULT_RULE.windowSizeMs,
          allowedLatenessMs: DEFAULT_RULE.allowedLatenessMs,
          orderBy: DEFAULT_RULE.orderBy,
          idlePartitionTimeoutMs: DEFAULT_RULE.idlePartitionTimeoutMs,
        },
        basedOn: null,
        seed: true,
      });
    }
  }

  /** 串行执行：新导入与旧查询式命令并发时，状态修改按到达顺序逐个提交 */
  _enqueue(fn) {
    const run = this._chain.then(() => fn());
    // 队列自身不因失败而中断
    this._chain = run.then(
      () => undefined,
      () => undefined
    );
    return run;
  }

  _emit(entry) {
    entry.seq = this.state.nextSeq;
    this.state.nextSeq += 1;
    this.state.entries.push(entry);
    reduceEntry(this.state, entry);
    if (this.journal) this.journal.append(entry);
    return entry;
  }

  // ---- 校验 ---------------------------------------------------------------

  static normalizeEvent(raw, now) {
    assert(raw && typeof raw === 'object', '事件必须是对象');
    assert(raw.eventId !== undefined && raw.eventId !== null, 'eventId 必填');
    assert(raw.partition !== undefined && raw.partition !== null, 'partition 必填');
    assert(Number.isInteger(raw.seq), 'seq 必须是整数');
    assert(Number.isInteger(raw.eventTime), 'eventTime 必须是整数毫秒');
    const arrivalTime = raw.arrivalTime === undefined ? now : raw.arrivalTime;
    assert(Number.isInteger(arrivalTime), 'arrivalTime 必须是整数毫秒');
    return {
      eventId: String(raw.eventId),
      partition: String(raw.partition),
      seq: raw.seq,
      eventTime: raw.eventTime,
      arrivalTime,
      data: raw.data ?? null,
    };
  }

  _checkRuleVersion(expectedRuleVersion) {
    if (expectedRuleVersion !== undefined && expectedRuleVersion !== null) {
      const cur = this.state.rules.currentVersion;
      if (expectedRuleVersion !== cur) {
        const err = new Error(
          `规则版本冲突：请求基于 ${expectedRuleVersion}，当前已是 ${cur}（旧重排结果不会覆盖新选择）`
        );
        err.statusCode = 409;
        err.code = 'STALE_RULE';
        err.expectedRuleVersion = expectedRuleVersion;
        err.currentRuleVersion = cur;
        throw err;
      }
    }
  }

  // ---- 命令：导入 ----------------------------------------------------------

  importEvents(rawEvents, { batchId = null, expectedRuleVersion } = {}) {
    return this._enqueue(() => {
      this._checkRuleVersion(expectedRuleVersion);
      const now = this.clock.now();
      const events = rawEvents.map((e) => Kernel.normalizeEvent(e, now));
      const bid =
        batchId ??
        `batch#${deterministicId([
          events.map((e) => e.eventId).join(','),
          events.map((e) => e.arrivalTime).join(','),
        ])}`;

      // 导入幂等：同一 batchId 直接返回上次结论，不产生新状态
      const prior = this.state.store.getBatch(bid);
      if (prior) {
        return {
          outcome: 'IGNORED_DUPLICATE_BATCH',
          batchId: bid,
          results: prior.eventIds.map((id) => this._arrivalView(this.state.store.getArrival(id))),
          note: '该批次已导入过，返回首次处理的结论',
        };
      }

      this._emit({ ts: now, type: 'BATCH_IMPORT', batchId: bid, eventIds: events.map((e) => e.eventId) });

      const touchedPartitions = new Set();
      const results = [];
      for (const event of events) {
        const r = this._ingestOne(event, now);
        results.push(r);
        touchedPartitions.add(event.partition);
      }

      const confirmed = [];
      const boundaries = [];
      for (const partition of [...touchedPartitions].sort()) {
        const out = this._pumpPartition(partition, 'import');
        confirmed.push(...out.confirmed);
        boundaries.push(...out.boundaries);
      }
      const gb = this._recomputeGlobalBoundary(now);
      if (gb) boundaries.push(gb.id);

      return {
        outcome: 'APPLIED',
        batchId: bid,
        ruleVersion: this.state.rules.currentVersion,
        results,
        touchedPartitions: [...touchedPartitions].sort(),
        confirmedWindowIds: [...new Set(confirmed)],
        boundaryIds: [...new Set(boundaries)],
      };
    });
  }

  _ingestOne(event) {
    const now = event.arrivalTime;
    const state = this.state;
    const part = ensurePart(state, event.partition);
    const rule = state.rules.current();
    // 非接受尝试的返回视图：反映“本次导入”的分类；
    // 首次事实（canonical arrival）仍保留原始状态，两者不互相覆盖。
    const attempt = (status, detail) => ({
      eventId: event.eventId,
      partition: event.partition,
      seq: event.seq,
      eventTime: event.eventTime,
      arrivalTime: event.arrivalTime,
      transportDelayMs: event.arrivalTime - event.eventTime,
      data: event.data ?? null,
      fingerprint: fingerprintEvent(event),
      status,
      detail,
    });

    // 1) 相同 eventId：重传。指纹不一致说明同 id 内容被篡改/串号，也明确标出
    const existing = state.store.getArrival(event.eventId);
    if (existing) {
      const contentChanged = existing.fingerprint !== fingerprintEvent(event);
      this._emit({
        ts: now,
        type: 'EVENT_RECEIVED',
        event,
        status: STATUS.DUPLICATE,
        detail: {
          ruleVersion: rule.version,
          watermark: part.currentWatermark,
          reason: 'same-event-id',
          contentChanged,
        },
      });
      return attempt(STATUS.DUPLICATE, {
        ruleVersion: rule.version,
        watermark: part.currentWatermark,
        reason: 'same-event-id',
        contentChanged,
      });
    }

    // 2) 同 (partition, seq) 但 eventId 不同
    const seqHolder = state.store.acceptedEventAtSeq(event.partition, event.seq);
    if (seqHolder) {
      const sameBusiness = seqHolder.fingerprintBusiness === businessFingerprint(event);
      if (sameBusiness) {
        // 同业务事件换了传输层 ID 的重传
        this._emit({
          ts: now,
          type: 'EVENT_RECEIVED',
          event,
          status: STATUS.DUPLICATE,
          detail: {
            ruleVersion: rule.version,
            watermark: part.currentWatermark,
            reason: 'same-partition-seq-same-content',
            originalEventId: seqHolder.eventId,
          },
        });
        return attempt(STATUS.DUPLICATE, {
          ruleVersion: rule.version,
          watermark: part.currentWatermark,
          reason: 'same-partition-seq-same-content',
          originalEventId: seqHolder.eventId,
        });
      }
      // 同序号内容不同：隔离为冲突，绝不覆盖已接受的那个版本
      this._emit({
        ts: now,
        type: 'EVENT_RECEIVED',
        event,
        status: STATUS.CONFLICT,
        detail: {
          ruleVersion: rule.version,
          watermark: part.currentWatermark,
          reason: 'same-partition-seq-different-content',
          originalEventId: seqHolder.eventId,
          originalFingerprint: seqHolder.fingerprint,
          receivedFingerprint: fingerprintEvent(event),
        },
      });
      this._emit({
        ts: now,
        type: 'DIAGNOSTIC',
        diag: {
          id: `diag#conflict#${deterministicId([event.partition, event.seq, event.eventId])}`,
          kind: 'SEQ_CONTENT_CONFLICT',
          at: now,
          partition: event.partition,
          seq: event.seq,
          windowStart: null,
          rejectedEventId: event.eventId,
          retainedEventId: seqHolder.eventId,
          message: `分区 ${event.partition} 序号 ${event.seq} 出现内容不同的事件，保留首次接受的 ${seqHolder.eventId}`,
        },
      });
      return attempt(STATUS.CONFLICT, {
        ruleVersion: rule.version,
        watermark: part.currentWatermark,
        reason: 'same-partition-seq-different-content',
        originalEventId: seqHolder.eventId,
      });
    }

    // 3) 迟到判定：先看是否已经晚于确认冻结边界（不可恢复），再看是否晚于水位（可恢复）
    const frozen = num(part.frozenUntil);
    if (frozen !== -Infinity && event.eventTime < frozen) {
      this._emit({
        ts: now,
        type: 'EVENT_RECEIVED',
        event,
        status: STATUS.EXPIRED_LATE,
        detail: {
          ruleVersion: rule.version,
          watermark: part.currentWatermark,
          frozenUntil: part.frozenUntil,
          lateMillis: frozen - event.eventTime,
          reason: 'event-time-before-confirmed-frontier',
        },
      });
      // 找到它“本该属于”的那个已确认窗口，便于排查归属
      const owner = this._findConfirmedWindow(event.partition, event.eventTime);
      this._emit({
        ts: now,
        type: 'DIAGNOSTIC',
        diag: {
          id: `diag#late#${deterministicId([event.eventId, frozen])}`,
          kind: 'EXPIRED_LATE',
          at: now,
          partition: event.partition,
          seq: event.seq,
          windowStart: owner ? owner.windowStart : null,
          windowId: owner ? owner.id : null,
          rejectedEventId: event.eventId,
          eventTime: event.eventTime,
          arrivalTime: event.arrivalTime,
          frozenUntil: part.frozenUntil,
          message: owner
            ? `事件晚于确认边界 ${part.frozenUntil}，结果窗口 ${owner.id} 已冻结，仅保留诊断`
            : `事件晚于确认边界 ${part.frozenUntil}，仅保留诊断`,
        },
      });
      return this._arrivalView(state.store.getArrival(event.eventId));
    }

    const wm = num(part.currentWatermark);
    const late = wm !== -Infinity && event.eventTime < wm;
    this._emit({
      ts: now,
      type: 'EVENT_RECEIVED',
      event,
      status: STATUS.ACCEPTED,
      detail: {
        ruleVersion: rule.version,
        watermark: part.currentWatermark,
        late, // true = 水位后到达但窗口未确认，仍可进入待定区参与重排
      },
    });

    // 分区因空闲超时被挂起后，新数据到达自动复活
    if (part.status === 'IDLE') {
      this._emit({ ts: now, type: 'IDLE_CHANGED', partition: event.partition, status: 'ACTIVE' });
    }

    // 水位 = 最大事件时间 − 当前规则允许迟到量（取历史最大值，保持单调）
    this._advanceWatermark(part, rule, now);

    return this._arrivalView(state.store.getArrival(event.eventId));
  }

  _advanceWatermark(part, rule, now) {
    const rawMax = num(part.rawMax);
    if (rawMax === -Infinity) return;
    const candidate = rawMax - rule.allowedLatenessMs;
    if (candidate > num(part.currentWatermark)) {
      this._emit({
        ts: now,
        type: 'WM_ADVANCED',
        partition: part.partition,
        watermark: candidate,
        rawMax: ser(rawMax),
        allowedLatenessMs: rule.allowedLatenessMs,
        ruleVersion: rule.version,
      });
    }
  }

  // ---- 草稿与确认 ----------------------------------------------------------

  _draftKey(partition, windowStart) {
    return `${partition}#${windowStart}`;
  }

  _buildDraft(partition, windowStart, rule, windowEnd = null) {
    const end = windowEnd ?? windowStart + rule.windowSizeMs;
    const events = this.state.store.readRange(partition, windowStart, end);
    const ordered =
      rule.orderBy === 'event-time'
        ? [...events].sort((a, b) =>
            a.eventTime !== b.eventTime
              ? a.eventTime - b.eventTime
              : a.seq !== b.seq
                ? a.seq - b.seq
                : eventOrderCompare(a, b)
          )
        : [...events].sort(eventOrderCompare);

    const key = this._draftKey(partition, windowStart);
    const part = this.state.parts.get(partition);
    return {
      key,
      partition,
      windowStart,
      windowEnd: end,
      windowSizeMs: rule.windowSizeMs,
      ruleVersion: rule.version,
      orderBy: rule.orderBy,
      draftEpoch: part.draftEpoch,
      eventIds: ordered.map((e) => e.eventId),
      eventCount: ordered.length,
      fingerprint: contentHash([rule.version, rule.orderBy, ...ordered.map((e) => e.eventId)].join('|')),
      builtAt: this.clock.now(),
    };
  }

  /**
   * 重排某分区的未确认尾部：
   *  - 作废与当前规则版本/窗口尺寸不一致的草稿（记录 DRAFT_RESET，而不是静默丢弃）
   *  - 只从 frozenUntil 起按区间读取该分区事件（不全表扫描）
   *  - 水位已关闭的窗口立即确认；其余保留在待定区
   */
  _pumpPartition(partition, reason) {
    const state = this.state;
    const part = ensurePart(state, partition);
    const rule = state.rules.current();
    const now = this.clock.now();

    // 作废过时草稿
    for (const [key, draft] of [...state.drafts]) {
      if (draft.partition !== partition) continue;
      if (draft.ruleVersion !== rule.version || draft.windowSizeMs !== rule.windowSizeMs) {
        this._emit({
          ts: now,
          type: 'DRAFT_RESET',
          key,
          partition,
          windowStart: draft.windowStart,
          oldRuleVersion: draft.ruleVersion,
          newRuleVersion: rule.version,
          reason,
          bumpEpoch: false,
        });
      }
    }

    const confirmed = [];
    const boundaries = [];

    if (part.firstEventTime === null) return { confirmed, boundaries };

    const frozen = num(part.frozenUntil);
    const wSize = rule.windowSizeMs;
    // 每个分区以其首个 accepted 事件所在的网格为分区原点：不凭空生成
    // 分区出生之前的空窗口结果。规则变化时网格本身不变；若冻结边界没有落在
    // 新网格上（窗口尺寸改变），第一个尾部草稿从冻结边界本身开始，末端对齐新网格。
    const origin = Math.floor(part.firstEventTime / wSize) * wSize;
    const firstTailStart = frozen !== -Infinity ? frozen : origin;

    // 只读未确认尾部（从冻结边界起；未确认时从分区原点起）
    const tailEvents =
      frozen !== -Infinity
        ? state.store.readFrom(partition, frozen)
        : state.store.readFrom(partition, origin);
    if (tailEvents.length === 0) return { confirmed, boundaries };

    const wm = num(part.currentWatermark);
    // 草稿只为“真正含数据”的窗口创建；空窗口不会被发布成结果，水位越过时直接推进。
    let dataMaxStart = null;
    for (const ev of tailEvents) {
      const st = windowStartOf(ev.eventTime, wSize, origin);
      dataMaxStart = dataMaxStart === null ? st : Math.max(dataMaxStart, st);
    }

    // 第一个尾部窗口可能是非对齐短窗口（窗口尺寸变更后与冻结边界相接），
    // 其后的窗口全部落在新网格上。
    const alignedFloor = (t) => Math.floor((t - origin) / wSize) * wSize + origin;
    const windowSpecs = [];
    {
      let st = firstTailStart;
      // 末端：已对齐则取 st+wSize；非对齐（st 落在某网格内部）则取“包含 st 的网格”的末端，
      // 再往后从该末端（对齐点）继续，绝不回退到已确认区间。
      let end = alignedFloor(st) === st ? st + wSize : alignedFloor(st) + wSize;
      while (st <= dataMaxStart) {
        const hasData = tailEvents.some((ev) => ev.eventTime >= st && ev.eventTime < end);
        if (hasData) windowSpecs.push([st, end]);
        st = end;
        end = alignedFloor(st) + wSize;
      }
    }

    for (const [st, end] of windowSpecs) {
      const fresh = this._buildDraft(partition, st, rule, end);
      const old = state.drafts.get(this._draftKey(partition, st));
      // 新窗口或内容/排序发生变化才产生 DRAFT_BUILT（回放时据此得到同样的条目集合）
      if (!old || old.fingerprint !== fresh.fingerprint || old.ruleVersion !== fresh.ruleVersion) {
        this._emit({ ts: now, type: 'DRAFT_BUILT', draft: fresh });
      }
    }

    // 按事件时间顺序确认所有 wm 已关闭的窗口
    const confirmable = [...state.drafts.values()]
      .filter((d) => d.partition === partition)
      .sort((a, b) => a.windowStart - b.windowStart);

    for (const draft of confirmable) {
      if (wm === -Infinity || draft.windowEnd > wm) continue;
      const winId = `win#${partition}#${draft.windowStart}`;
      // 序号空洞按“分区连续序号”判定，跨窗口也成立：
      // 窗口确认意味着它之前的事件时间范围已封闭；以分区当前最大已接受序号为上界，
      // 中间缺失的序号再回来也只能进诊断（后续事件已把水位推过本窗口）。
      const maxSeq = part.acceptedSeqs.length
        ? part.acceptedSeqs[part.acceptedSeqs.length - 1]
        : -Infinity;
      const gaps = maxSeq === -Infinity ? [] : this._takeSeqGaps(part, maxSeq);
      for (const gap of gaps) {
        this._emit({
          ts: now,
          type: 'DIAGNOSTIC',
          diag: {
            id: `diag#seqgap#${deterministicId([partition, draft.windowStart, gap.from, gap.to])}`,
            kind: 'SEQ_GAP',
            at: now,
            partition,
            windowStart: draft.windowStart,
            windowId: winId,
            gap,
            ruleVersion: draft.ruleVersion,
            message: `分区 ${partition} 窗口 ${draft.windowStart} 确认时序号缺失 [${gap.from}, ${gap.to}]（缺 ${gap.missing} 条）`,
          },
        });
      }
      const result = {
        id: winId,
        partition,
        windowStart: draft.windowStart,
        windowEnd: draft.windowEnd,
        ruleVersion: draft.ruleVersion, // 冻结在确认时的规则
        orderBy: draft.orderBy,
        orderedEventIds: [...draft.eventIds],
        seqGaps: gaps,
        draftFingerprint: draft.fingerprint,
        watermarkAtConfirm: part.currentWatermark,
        confirmedAt: now,
        contentHash: contentHash(
          [partition, draft.windowStart, draft.ruleVersion, ...draft.eventIds].join('|')
        ),
      };
      this._emit({ ts: now, type: 'WINDOW_CONFIRMED', result });
      confirmed.push(winId);
    }

    return { confirmed, boundaries };
  }

  /**
   * 计算分区中 [1..maxSeq] 内尚未报告过的序号缺失区间（不修改状态）。
   * 标记“已报告”由规约器在应用 SEQ_GAP 诊断时完成，保证实时/回放一致。
   */
  _takeSeqGaps(part, maxSeq) {
    const seqSet = new Set(part.acceptedSeqs);
    const reported = new Set();
    for (const [from, to] of part.reportedGaps) {
      for (let s = from; s <= to; s++) reported.add(s);
    }
    const out = [];
    let runStart = null;
    for (let s = 1; s <= maxSeq; s++) {
      const missing = !seqSet.has(s) && !reported.has(s);
      if (missing && runStart === null) runStart = s;
      if (runStart !== null && (s === maxSeq || seqSet.has(s + 1) || reported.has(s + 1))) {
        out.push({ from: runStart, to: s, missing: s - runStart + 1 });
        runStart = null;
      }
    }
    return out;
  }

  _findConfirmedWindow(partition, eventTime) {
    for (const id of this.state.confirmedOrder) {
      const w = this.state.confirmations.get(id);
      if (
        w.partition === partition &&
        eventTime >= w.windowStart &&
        eventTime < w.windowEnd
      ) {
        return w;
      }
    }
    return null;
  }

  /**
   * 全局发布边界 = 所有“参与中”分区冻结边界的最小值。
   *  - ACTIVE/NO_DATA 且尚无确认的分区会显式阻塞并给出原因；
   *  - IDLE 分区不阻塞：已确认的空闲分区仍把其冻结边界贡献给 min，
   *    从未确认过的空闲分区既不贡献也不阻塞（显式列出）；
   *  - 若没有任何分区提供有限边界，frontier=null（还无法发布）。
   */
  _recomputeGlobalBoundary(now) {
    const state = this.state;
    const blockers = [];
    const idlePartitions = [];
    let frontier = Infinity;
    let anyFinite = false;

    for (const [partition, p] of [...state.parts.entries()].sort((a, b) =>
      a[0] < b[0] ? -1 : 1
    )) {
      if (p.status === 'IDLE') {
        idlePartitions.push(partition);
        const fIdle = num(p.frozenUntil);
        if (fIdle !== -Infinity) {
          frontier = Math.min(frontier, fIdle);
          anyFinite = true;
        }
        continue;
      }
      if (p.status === 'NO_DATA') {
        blockers.push({ partition, reason: 'NO_DATA', message: '分区尚无任何 accepted 事件' });
        continue;
      }
      const f = num(p.frozenUntil);
      if (f === -Infinity) {
        blockers.push({ partition, reason: 'NO_CONFIRMATION', watermark: p.currentWatermark,
          message: '分区有数据但还没有任何窗口达到确认条件' });
        continue;
      }
      frontier = Math.min(frontier, f);
      anyFinite = true;
    }

    const resolved = blockers.length === 0 && anyFinite;
    const newFrontier = resolved ? ser(frontier) : null;
    const sig = JSON.stringify({
      frontier: newFrontier,
      blockers: blockers.map((b) => `${b.partition}:${b.reason}`),
      idle: idlePartitions,
    });
    if (sig === state.globalSig && newFrontier === state.globalFrontier) return null;

    const count = state.globalBoundaryCount + 1;
    const id = `gb-${count}`;
    const publishedWindows = resolved
      ? state.confirmedOrder
          .map((wid) => state.confirmations.get(wid))
          .filter((w) => w.windowEnd <= frontier)
          .sort((a, b) =>
            a.windowEnd !== b.windowEnd
              ? a.windowEnd - b.windowEnd
              : a.partition < b.partition
                ? -1
                : a.partition > b.partition
                  ? 1
                  : 0
          )
          .map((w) => ({
            windowId: w.id,
            partition: w.partition,
            windowStart: w.windowStart,
            windowEnd: w.windowEnd,
            ruleVersion: w.ruleVersion,
            eventCount: w.orderedEventIds.length,
          }))
      : [];

    const entry = this._emit({
      ts: now ?? this.clock.now(),
      type: 'GLOBAL_BOUNDARY',
      id,
      count,
      frontier: newFrontier,
      blockers,
      idlePartitions,
      publishedWindows,
      sig,
    });
    return entry;
  }

  // ---- 命令：规则版本 -------------------------------------------------------

  registerRule(spec, { version = null, expectedRuleVersion } = {}) {
    return this._enqueue(() => {
      this._checkRuleVersion(expectedRuleVersion);
      const now = this.clock.now();
      // 在临时注册表里完成全部校验与版本构造（失败不留任何痕迹）
      const candidate = new RuleRegistry().register(
        {
          windowSizeMs: spec.windowSizeMs,
          allowedLatenessMs: spec.allowedLatenessMs,
          orderBy: spec.orderBy,
          idlePartitionTimeoutMs: spec.idlePartitionTimeoutMs,
        },
        { version: version ?? `v${this.state.rules.rules.size + 1}`, now }
      );
      // journal 是唯一事实来源：规约幂等应用后 registry 才真正拥有新版本
      this._emit({
        ts: now,
        type: 'RULE_ADDED',
        version: candidate.version,
        spec: {
          windowSizeMs: candidate.windowSizeMs,
          allowedLatenessMs: candidate.allowedLatenessMs,
          orderBy: candidate.orderBy,
          idlePartitionTimeoutMs: candidate.idlePartitionTimeoutMs,
        },
        basedOn: this.state.rules.currentVersion,
      });

      // 新规则只重排未确认尾部：逐个分区作废旧草稿并重建
      const resetPartitions = [];
      const confirmed = [];
      for (const [partition, p] of this.state.parts) {
        const hasTail =
          num(p.rawMax) !== -Infinity &&
          (num(p.frozenUntil) === -Infinity || num(p.frozenUntil) <= num(p.rawMax));
        if (hasTail) {
          resetPartitions.push(partition);
          const out = this._pumpPartition(partition, 'rule-change');
          confirmed.push(...out.confirmed);
        }
      }
      // 允许迟到量变化也会改变水位
      const rule = this.state.rules.current();
      for (const partition of resetPartitions) {
        const p = this.state.parts.get(partition);
        this._advanceWatermark(p, rule, now);
        const out = this._pumpPartition(partition, 'rule-change-watermark');
        confirmed.push(...out.confirmed);
      }
      const boundary = this._recomputeGlobalBoundary(now);

      return {
        outcome: 'RULE_REGISTERED',
        ruleVersion: candidate.version,
        resetPartitions: resetPartitions.sort(),
        confirmedWindowIds: [...new Set(confirmed)],
        boundaryId: boundary ? boundary.id : null,
      };
    });
  }

  // ---- 命令：手动重算 -------------------------------------------------------

  /**
   * 重新计算某个窗口/某分区尾部。
   *  - 已确认窗口不可变：命中返回 409 IMMUTABLE_RESULT，并记录 RECOMPUTE_REJECTED 诊断
   *  - expectedRuleVersion 不匹配：409 STALE_RULE，不产生状态
   */
  recompute({ partition, windowStart = null, expectedRuleVersion = undefined, reason = 'manual' } = {}) {
    return this._enqueue(() => {
      this._checkRuleVersion(expectedRuleVersion);
      assert(partition !== undefined && partition !== null, 'partition 必填');
      const now = this.clock.now();
      const part = this.state.parts.get(partition);
      assert(part, `未知分区: ${partition}`, 404);
      const rule = this.state.rules.current();

      const recomputeId = `rc#${deterministicId([
        partition,
        windowStart ?? 'tail',
        rule.version,
        part.draftEpoch,
        state_seq(this),
      ])}`;

      if (windowStart !== null) {
        assert(Number.isInteger(windowStart), 'windowStart 必须是整数毫秒');
        const winId = `win#${partition}#${windowStart}`;
        if (this.state.confirmations.has(winId) || windowStart < num(part.frozenUntil)) {
          this._emit({
            ts: now,
            type: 'RECOMPUTE',
            id: recomputeId,
            partition,
            windowStart,
            ruleVersion: rule.version,
            rejected: true,
            reason,
          });
          this._emit({
            ts: now,
            type: 'DIAGNOSTIC',
            diag: {
              id: `diag#rc-reject#${deterministicId([recomputeId])}`,
              kind: 'RECOMPUTE_REJECTED',
              at: now,
              partition,
              windowStart,
              windowId: this.state.confirmations.has(winId) ? winId : null,
              frozenUntil: part.frozenUntil,
              recomputeId,
              message: '已确认结果不可因旧事件/手动请求被改写；如需更正请在新版本规则下处理未确认部分',
            },
          });
          const err = new Error(`窗口 ${winId} 已确认，结果不可变`);
          err.statusCode = 409;
          err.code = 'IMMUTABLE_RESULT';
          err.windowId = winId;
          throw err;
        }
      }

      this._emit({
        ts: now,
        type: 'RECOMPUTE',
        id: recomputeId,
        partition,
        windowStart,
        ruleVersion: rule.version,
        rejected: false,
        reason,
      });

      // 作废指定窗口及其后的全部草稿（区间作废，不触碰其他分区、已确认部分）
      const resetKeys = [];
      for (const [key, draft] of [...this.state.drafts]) {
        if (draft.partition !== partition) continue;
        if (windowStart !== null && draft.windowStart < windowStart) continue;
        resetKeys.push(key);
      }
      for (const key of [...resetKeys].sort()) {
        this._emit({
          ts: now,
          type: 'DRAFT_RESET',
          key,
          partition,
          windowStart: Number(key.split('#')[1]),
          oldRuleVersion: rule.version,
          newRuleVersion: rule.version,
          reason: `recompute:${reason}`,
          bumpEpoch: true,
        });
      }

      this._advanceWatermark(part, rule, now);
      const out = this._pumpPartition(partition, `recompute:${reason}`);
      const boundary = this._recomputeGlobalBoundary(now);

      return {
        outcome: 'RECOMPUTED',
        recomputeId,
        partition,
        windowStart,
        ruleVersion: rule.version,
        resetDraftKeys: resetKeys.sort(),
        rebuiltDraftKeys: [...this.state.drafts.keys()]
          .filter((k) => k.startsWith(`${partition}#`))
          .sort(),
        confirmedWindowIds: out.confirmed,
        boundaryId: boundary ? boundary.id : null,
      };
    });
  }

  // ---- 命令：时钟/空闲 ------------------------------------------------------

  tick(now = null) {
    return this._enqueue(() => {
      const ts = now ?? this.clock.now();
      assert(Number.isInteger(ts), 'tick.now 必须是整数毫秒');
      this._emit({ ts, type: 'TICK', now: ts });

      const changed = [];
      for (const [partition, p] of this.state.parts) {
        if (p.status !== 'ACTIVE' || p.lastSeenArrival === null) continue;
        const timeout = this.state.rules.current().idlePartitionTimeoutMs;
        if (ts - p.lastSeenArrival >= timeout) {
          this._emit({
            ts,
            type: 'IDLE_CHANGED',
            partition,
            status: 'IDLE',
            reason: 'idle-timeout',
            silentForMs: ts - p.lastSeenArrival,
          });
          changed.push(partition);
        }
      }
      const boundary = this._recomputeGlobalBoundary(ts);
      return {
        outcome: 'TICKED',
        now: ts,
        idlePartitions: changed.sort(),
        boundaryId: boundary ? boundary.id : null,
      };
    });
  }

  // ===========================================================================
  // 查询层（只读，不经过串行队列；Node 单线程下拿到的是一致快照）
  // ===========================================================================

  _arrivalView(a) {
    if (!a) return null;
    return {
      eventId: a.eventId,
      partition: a.partition,
      seq: a.seq,
      // 两类时间始终平级、独立出现，杜绝把网络延迟误读成业务时间错误
      eventTime: a.eventTime,
      arrivalTime: a.arrivalTime,
      transportDelayMs: a.arrivalTime - a.eventTime,
      data: a.data,
      fingerprint: a.fingerprint,
      status: a.status,
      detail: a.detail,
    };
  }

  /**
   * 时间线：basis='event' 按业务时间排（看重排后的业务顺序），
   *         basis='arrival' 按到达时间排（看乱序/迟到的物理发生顺序）。
   * 每条都标注它当前归属：哪个待定草稿 / 哪个已确认窗口 / 哪个诊断。
   */
  timeline({ partition = null, basis = 'event', from = null, to = null, limit = 500 } = {}) {
    assert(['event', 'arrival'].includes(basis), "basis 只能是 'event' 或 'arrival'");
    let rows = [...this.state.store.arrivals.values()];
    if (partition !== null) rows = rows.filter((a) => a.partition === partition);
    const field = basis === 'event' ? 'eventTime' : 'arrivalTime';
    if (from !== null) rows = rows.filter((a) => a[field] >= from);
    if (to !== null) rows = rows.filter((a) => a[field] < to);
    rows.sort((a, b) =>
      a[field] !== b[field]
        ? a[field] - b[field]
        : a.partition !== b.partition
          ? a.partition < b.partition
            ? -1
            : 1
          : a.seq - b.seq
    );
    const total = rows.length;
    rows = rows.slice(0, limit);
    return {
      basis,
      total,
      shown: rows.length,
      events: rows.map((a) => ({ ...this._arrivalView(a), ...this._attribution(a) })),
    };
  }

  _attribution(a) {
    // 只有 accepted 事件才能归属到“结果/待定”；
    // DUPLICATE / CONFLICT / EXPIRED_LATE 的原始事实保留，但不进入结果顺序。
    if (a.status !== STATUS.ACCEPTED) {
      return { attribution: { kind: 'DIAGNOSTIC_ONLY' } };
    }
    // 归属：已确认窗口 / 待定草稿
    const owner = this._findConfirmedWindow(a.partition, a.eventTime);
    if (owner && owner.orderedEventIds.includes(a.eventId)) {
      return {
        attribution: {
          kind: 'CONFIRMED_WINDOW',
          windowId: owner.id,
          windowStart: owner.windowStart,
          windowEnd: owner.windowEnd,
          ruleVersion: owner.ruleVersion,
          ordinal: owner.orderedEventIds.indexOf(a.eventId),
        },
      };
    }
    const rule = this.state.rules.get(a.detail?.ruleVersion ?? this.state.rules.currentVersion);
    // 按事件实际落入的窗口区间查草稿（窗口尺寸变更后草稿可能不是全局网格对齐的）
    let draft = null;
    for (const d of this.state.drafts.values()) {
      if (
        d.partition === a.partition &&
        a.eventTime >= d.windowStart &&
        a.eventTime < d.windowEnd
      ) {
        draft = d;
        break;
      }
    }
    if (draft) {
      return {
        attribution: {
          kind: 'PENDING_DRAFT',
          draftKey: draft.key,
          windowStart: draft.windowStart,
          windowEnd: draft.windowEnd,
          ruleVersion: draft.ruleVersion,
          ordinal: draft.eventIds.indexOf(a.eventId),
        },
      };
    }
    return { attribution: { kind: 'DIAGNOSTIC_ONLY' } };
  }

  /** 待定区：草稿窗口 + 内部事件全量 + 序号空洞 + 当前水位与可确认性 */
  pending({ partition = null } = {}) {
    const rule = this.state.rules.current();
    const drafts = [...this.state.drafts.values()]
      .filter((d) => partition === null || d.partition === partition)
      .sort((a, b) =>
        a.partition !== b.partition ? (a.partition < b.partition ? -1 : 1) : a.windowStart - b.windowStart
      );
    return {
      currentRuleVersion: rule.version,
      count: drafts.length,
      drafts: drafts.map((d) => {
        const p = this.state.parts.get(d.partition);
        // 待定阶段可见的空洞：到当前草稿最大序号为止、尚未被任何确认报告过的缺失
        // （注意：它们还不是诊断，只有窗口确认封闭后才升级为 SEQ_GAP）
        const maxSeq = d.eventIds.reduce(
          (m, id) => Math.max(m, this.state.store.getArrival(id).seq),
          -Infinity
        );
        const observedGaps = maxSeq === -Infinity ? [] : this._takeSeqGaps(p, maxSeq);
        return {
          draftKey: d.key,
          partition: d.partition,
          windowStart: d.windowStart,
          windowEnd: d.windowEnd,
          ruleVersion: d.ruleVersion,
          orderBy: d.orderBy,
          stale: d.ruleVersion !== rule.version || d.windowSizeMs !== rule.windowSizeMs,
          watermark: p.currentWatermark,
          confirmable: p.currentWatermark !== null && p.currentWatermark >= d.windowEnd,
          observedSeqGaps: observedGaps,
          draftEpoch: d.draftEpoch,
          fingerprint: d.fingerprint,
          events: d.eventIds.map((id) => {
            const a = this.state.store.getArrival(id);
            return {
              eventId: id,
              seq: a.seq,
              eventTime: a.eventTime,
              arrivalTime: a.arrivalTime,
              late: !!a.detail?.late,
              data: a.data,
            };
          }),
        };
      }),
    };
  }

  /** 结果视图：每个确认边界（含全局边界）都保留，且可跳回原始事件 */
  results({ partition = null } = {}) {
    const wins = this.state.confirmedOrder
      .map((id) => this.state.confirmations.get(id))
      .filter((w) => partition === null || w.partition === partition);
    return {
      currentRuleVersion: this.state.rules.currentVersion,
      globalFrontier: this.state.globalFrontier,
      confirmedCount: wins.length,
      windows: wins.map((w) => this._windowView(w)),
      boundaries: this.state.entries
        .filter((e) => e.type === 'GLOBAL_BOUNDARY')
        .map((e) => this._boundaryView(e, false)),
    };
  }

  _windowView(w) {
    return {
      windowId: w.id,
      partition: w.partition,
      windowStart: w.windowStart,
      windowEnd: w.windowEnd,
      ruleVersion: w.ruleVersion,
      orderBy: w.orderBy,
      confirmedAt: w.confirmedAt,
      watermarkAtConfirm: w.watermarkAtConfirm,
      contentHash: w.contentHash,
      seqGaps: w.seqGaps,
      events: w.orderedEventIds.map((id, ordinal) => {
        const a = this.state.store.getArrival(id);
        return {
          ordinal,
          eventId: id,
          seq: a.seq,
          eventTime: a.eventTime,
          arrivalTime: a.arrivalTime,
          transportDelayMs: a.arrivalTime - a.eventTime,
          late: !!a.detail?.late,
          data: a.data,
        };
      }),
    };
  }

  boundaries() {
    return this.state.entries
      .filter((e) => e.type === 'GLOBAL_BOUNDARY')
      .map((e) => this._boundaryView(e, true));
  }

  boundary(id) {
    const e = this.state.entries.find((x) => x.type === 'GLOBAL_BOUNDARY' && x.id === id);
    assert(e, `边界不存在: ${id}`, 404);
    return this._boundaryView(e, true);
  }

  _boundaryView(e, expand) {
    return {
      boundaryId: e.id,
      seq: e.seq,
      emittedAt: e.ts,
      frontier: e.frontier, // null = 当时还无法发布
      blockers: e.blockers,
      idlePartitions: e.idlePartitions,
      publishedWindows: expand
        ? e.publishedWindows.map((ref) => this._windowView(this.state.confirmations.get(ref.windowId)))
        : e.publishedWindows,
    };
  }

  event(eventId) {
    const a = this.state.store.getArrival(eventId);
    assert(a, `事件不存在: ${eventId}`, 404);
    return { ...this._arrivalView(a), ...this._attribution(a) };
  }

  partitions() {
    return [...this.state.parts.values()].map((p) => ({
      partition: p.partition,
      status: p.status,
      rawMaxEventTime: p.rawMax,
      watermark: p.currentWatermark,
      confirmedFrontier: p.frozenUntil,
      originMs: p.originMs,
      firstEventTime: p.firstEventTime,
      lastSeenArrivalTime: p.lastSeenArrival,
      pendingDraftCount: [...this.state.drafts.keys()].filter((k) =>
        k.startsWith(`${p.partition}#`)
      ).length,
      confirmedCount: this.state.confirmedOrder.filter((id) =>
        id.startsWith(`win#${p.partition}#`)
      ).length,
    }));
  }

  rules() {
    return { current: this.state.rules.currentVersion, versions: this.state.rules.list() };
  }

  diagnostics({ partition = null, kind = null } = {}) {
    return this.state.diagnosticOrder
      .map((id) => this.state.diagnostics.get(id))
      .filter((d) => partition === null || d.partition === partition)
      .filter((d) => kind === null || d.kind === kind);
  }

  journalView({ since = 0, limit = 1000 } = {}) {
    const items = this.state.entries
      .filter((e) => e.seq >= since)
      .slice(0, limit)
      .map((e) => ({ seq: e.seq, ts: e.ts, type: e.type, ...stripInternals(e) }));
    return { lastSeq: this.state.nextSeq - 1, count: items.length, entries: items };
  }

  stats() {
    return {
      store: { ...this.state.store.stats },
      journalEntries: this.state.entries.length,
      drafts: this.state.drafts.size,
      confirmations: this.state.confirmations.size,
      diagnostics: this.state.diagnostics.size,
      globalBoundaryCount: this.state.globalBoundaryCount,
    };
  }

  // ---- 回放验证 ------------------------------------------------------------

  /**
   * 从 journal（或持久化日志）从头规约出一个新内核，逐字段比对，
   * 证明“状态变化作为可回放记录”成立，而不是只留下最终数字。
   */
  verifyReplay() {
    const replayed = Kernel.replay(this.state.entries);
    const a = serializeKernelState(this.state);
    const b = serializeKernelState(replayed.state);
    const equal = JSON.stringify(a) === JSON.stringify(b);
    return {
      equal,
      entryCount: this.state.entries.length,
      liveSummary: summary(this.state),
      replayedSummary: summary(replayed.state),
    };
  }

  static replay(entries) {
    // seed:false —— 包括种子规则在内的全部事实都来自传入条目流
    const k = new Kernel({ clock: createClock(() => 0), seed: false });
    for (const entry of entries) reduceEntry(k.state, entry);
    k.state.nextSeq = entries.reduce((m, e) => Math.max(m, e.seq + 1), 0);
    k.state.entries = [...entries];
    return k;
  }

  /** 从持久化日志恢复：快照 + 增量条目（内存中保留完整条目流，供视图/回放使用） */
  static async restore(journal, { clock = null } = {}) {
    const snapshot = await journal.loadSnapshot();
    const k = new Kernel({ clock, journal, seed: false });
    const allEntries = await journal.readEntriesAfter(-1);
    if (snapshot) {
      restoreStateInto(k.state, snapshot.state);
      for (const entry of allEntries) {
        if (entry.seq > snapshot.lastSeq) reduceEntry(k.state, entry);
      }
    } else {
      for (const entry of allEntries) reduceEntry(k.state, entry);
    }
    k.state.entries = allEntries;
    k.state.nextSeq = allEntries.length
      ? Math.max(...allEntries.map((e) => e.seq)) + 1
      : snapshot
        ? snapshot.lastSeq + 1
        : 0;
    return k;
  }

  async checkpoint() {
    if (!this.journal) return null;
    return this._enqueue(async () => {
      const snap = {
        lastSeq: this.state.nextSeq - 1,
        takenAt: this.clock.now(),
        state: serializeKernelState(this.state),
      };
      await this.journal.writeSnapshot(snap);
      return { lastSeq: snap.lastSeq, takenAt: snap.takenAt };
    });
  }
}

function state_seq(k) {
  return k.state.nextSeq;
}

function stripInternals(entry) {
  const { seq, ts, type, ...rest } = entry;
  return rest;
}

function summary(state) {
  return {
    arrivals: state.store.stats.totalArrivals,
    drafts: state.drafts.size,
    confirmations: state.confirmations.size,
    diagnostics: state.diagnostics.size,
    globalFrontier: state.globalFrontier,
    globalBoundaryCount: state.globalBoundaryCount,
    confirmedHash: contentHash(
      state.confirmedOrder
        .map((id) => state.confirmations.get(id).contentHash)
        .join('|')
    ),
  };
}

// ---- 状态序列化（快照/回放比对） --------------------------------------------

function serializeKernelState(state) {
  return {
    nextSeq: state.nextSeq,
    store: state.store.toJSON(),
    rules: state.rules.toJSON(),
    parts: [...state.parts.entries()]
      .sort((a, b) => (a[0] < b[0] ? -1 : 1))
      .map(([k, p]) => [k, { ...p, partition: k }]),
    drafts: [...state.drafts.entries()]
      .map(([k, d]) => [k, d])
      .sort((a, b) => (a[0] < b[0] ? -1 : 1)),
    confirmations: state.confirmedOrder.map((id) => state.confirmations.get(id)),
    diagnosticOrder: state.diagnosticOrder,
    diagnostics: state.diagnosticOrder.map((id) => state.diagnostics.get(id)),
    globalFrontier: state.globalFrontier,
    globalSig: state.globalSig,
    globalBoundaryCount: state.globalBoundaryCount,
    lastTick: state.lastTick,
  };
}

function restoreStateInto(target, data) {
  target.nextSeq = data.nextSeq;
  target.store = EventStore.fromJSON(data.store);
  target.rules = RuleRegistry.fromJSON(data.rules);
  target.parts = new Map(data.parts.map(([k, p]) => [k, p]));
  target.drafts = new Map(data.drafts);
  target.confirmations = new Map(data.confirmations.map((c) => [c.id, c]));
  target.confirmedOrder = data.confirmations.map((c) => c.id);
  target.diagnostics = new Map(data.diagnostics.map((d) => [d.id, d]));
  target.diagnosticOrder = data.diagnosticOrder;
  target.globalFrontier = data.globalFrontier;
  target.globalSig = data.globalSig;
  target.globalBoundaryCount = data.globalBoundaryCount;
  target.lastTick = data.lastTick;
}

export { serializeKernelState, restoreStateInto };
