// kernel.js —— 乱序事件重排与确认内核（事件溯源：调用方只产出状态记录，#apply 是唯一状态变更点）
//
// 核心概念（排查时必须能区分，而不是压成一个布尔值）：
//   事件时间 eventTime   —— 业务发生时间
//   到达时间 arrivalTime —— 系统看到它的时间（网络延迟体现在二者之差）
//   头事件时间 head      —— 分区内已观察到的最大事件时间
//   排序线 orderLine     —— head - allowedLateness：超过它还没到，就先挂起待定
//   确认线 confirmLine   —— head - 2 * allowedLateness：跨过它的结果永久锁定，
//                           旧事件再回来只能留诊断，不能无提示改写
//
// 事件在分区内的落点：
//   PENDING_BUFFERED   eventTime > orderLine，先进待定区等待排序
//   LATE_TOLERATED     eventTime <= orderLine 且 > confirmLine：迟到但在容忍窗口内，
//                      可进入未确认窗口并触发重排（留下修订痕迹）
//   LATE_DIAGNOSTIC    eventTime <= confirmLine：晚于确认线，只留诊断，不碰已发布结果
import { EventEmitter } from 'node:events';
import { hashContent } from './persistence.js';

export const RULE_ACTIVE = 'ACTIVE';
export const RULE_SUPERSEDED = 'SUPERSEDED';

export const PARTITION_ACTIVE = 'ACTIVE';
export const PARTITION_IDLE = 'IDLE';

export const WINDOW_OPEN = 'OPEN';
export const WINDOW_ORDERED = 'ORDERED';
export const WINDOW_CONFIRMED = 'CONFIRMED';

export const EVENT_PENDING = 'PENDING_BUFFERED';
export const EVENT_LATE_TOL = 'LATE_TOLERATED';
export const EVENT_ORDERED = 'ORDERED';
export const EVENT_CONFIRMED = 'CONFIRMED';
export const EVENT_LATE_DIAG = 'LATE_DIAGNOSTIC';

export const ORDER_BY_EVENT_TIME_SEQ = 'EVENT_TIME_SEQ';
export const ORDER_BY_SEQUENCE = 'SEQUENCE';

export class KernelError extends Error {
  constructor(status, code, message, details = undefined) {
    super(message);
    this.name = 'KernelError';
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

export const DEFAULT_RULE = {
  ruleVersion: 1,
  allowedLateness: 1000,
  windowSize: 1000,
  idleTimeout: 5000,
  orderBy: ORDER_BY_EVENT_TIME_SEQ,
};

export function freshState() {
  return {
    systemTime: 0,
    rules: [],
    currentRuleVersion: 0,
    partitions: new Map(),
    events: new Map(),
    windows: new Map(),
    anomalies: [],
    revisionSeq: 0,
    orderIndexSeq: new Map(),
  };
}

export function eventId(event) {
  return `${event.partition}|${event.sequence}`;
}

export function windowStart(eventTime, windowSize) {
  return Math.floor(eventTime / windowSize) * windowSize;
}

export function windowKey(partition, start) {
  return `${partition}@${start}`;
}

function sortedInsert(list, value, compare) {
  let lo = 0;
  let hi = list.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (compare(value, list[mid]) < 0) hi = mid;
    else lo = mid + 1;
  }
  list.splice(lo, 0, value);
  return lo;
}

export class ReorderKernel extends EventEmitter {
  constructor(store, { clock = () => Date.now(), rule = DEFAULT_RULE } = {}) {
    super();
    this.store = store;
    this.clock = clock;
    this.initialRule = { ...rule };
    this.state = freshState();
    this._queue = Promise.resolve();
    this._snapshotThreshold = store.autoSnapshotEvery ?? 100;
  }

  async initialize() {
    const { snapshot } = this.store.load(
      (entry) => this.#apply(entry),
      (state) => {
        this.state = ReorderKernel.restoreState(state);
      }
    );
    if (this.state.rules.length === 0) {
      this.#append({
        type: 'RULE_ACTIVATED',
        rule: { ...this.initialRule },
        reason: 'INITIAL',
        replacedVersion: null,
      });
    }
  }

  get currentRule() {
    return this.state.rules.find((r) => r.version === this.state.currentRuleVersion) || null;
  }

  ruleAt(version) {
    return this.state.rules.find((r) => r.version === version) || null;
  }

  get offset() {
    return this.store.nextOffset - 1;
  }

  getEvent(id) {
    return this.state.events.get(id) || null;
  }

  getPartition(name) {
    return this.state.partitions.get(name) || null;
  }

  getWindow(key) {
    return this.state.windows.get(key) || null;
  }

  // 写操作串行化：新导入与旧查询/旧重算并发时，提交严格按队列顺序发生。
  #enqueue(job) {
    const run = this._queue.then(() => job());
    this._queue = run.then(
      () => undefined,
      () => undefined
    );
    return run;
  }

  #append(entry) {
    const stored = this.store.append(entry);
    this.#apply(stored);
    if (this.store.nextOffset % this._snapshotThreshold === 0) {
      this.store.saveSnapshot(stored._offset, this.#serializeState());
    }
    return stored;
  }

  ingestBatch(rawEvents, { arrivalTime } = {}) {
    return this.#enqueue(() => this.#ingestBatch(rawEvents, arrivalTime));
  }

  ingestOne(rawEvent, opts = {}) {
    return this.ingestBatch([rawEvent], opts).then((r) => ({
      ingested: r.ingested[0] || null,
      duplicate: r.duplicates[0] || null,
      conflict: r.conflicts[0] || null,
      lateDiagnostic: r.lateDiagnostics[0] || null,
      windows: r.windows,
      confirmations: r.confirmations,
    }));
  }

  #ingestBatch(rawEvents, overrideArrival) {
    if (!Array.isArray(rawEvents) || rawEvents.length === 0) {
      throw new KernelError(400, 'EMPTY_BATCH', '至少导入一个事件');
    }
    const sysNow = Math.max(this.clock(), this.state.systemTime);
    this.state.systemTime = sysNow;

    const result = {
      ingested: [],
      duplicates: [],
      conflicts: [],
      lateDiagnostics: [],
      windows: [],
      confirmations: [],
    };
    const touched = new Set();

    for (const raw of rawEvents) {
      const event = this.#validate(raw);
      const arrival = overrideArrival ?? event.arrivalTime ?? sysNow;
      const id = eventId(event);
      touched.add(event.partition);

      const existing = this.state.events.get(id);
      if (existing) {
        const newHash = hashContent(event.payload);
        if (existing.contentHash === newHash) {
          this.#append({
            type: 'DUPLICATE_OBSERVED',
            eventId: id,
            partition: event.partition,
            arrivalTime: arrival,
            eventTime: event.eventTime,
            sequence: event.sequence,
            contentHash: newHash,
          });
          result.duplicates.push(this.#presentEvent(this.state.events.get(id)));
          continue;
        }
        this.#append({
          type: 'CONFLICT_OBSERVED',
          eventId: id,
          partition: event.partition,
          arrivalTime: arrival,
          eventTime: event.eventTime,
          sequence: event.sequence,
          winnerContentHash: existing.contentHash,
          rejectedContentHash: newHash,
          rejectedPayload: event.payload,
          rejectedArrivalTime: arrival,
        });
        result.conflicts.push(this.state.anomalies[this.state.anomalies.length - 1]);
        continue;
      }

      this.#append({
        type: 'EVENT_IMPORTED',
        event: {
          eventId: id,
          partition: event.partition,
          eventTime: event.eventTime,
          arrivalTime: arrival,
          sequence: event.sequence,
          payload: event.payload,
          contentHash: hashContent(event.payload),
        },
      });

      this.#admit(id, result);
      touched.add(event.partition);
    }

    for (const partition of touched) this.#idleSweep(partition, Math.max(this.clock(), this.state.systemTime));

    return result;
  }

  #validate(raw) {
    const e = raw || {};
    const { partition, sequence, eventTime, payload } = e;
    if (typeof partition !== 'string' || partition.length === 0) {
      throw new KernelError(400, 'BAD_EVENT', 'partition 必须是非空字符串', { field: 'partition' });
    }
    if (!Number.isInteger(sequence) || sequence < 0) {
      throw new KernelError(400, 'BAD_EVENT', 'sequence 必须是非负整数', { field: 'sequence' });
    }
    if (!Number.isInteger(eventTime) || eventTime < 0) {
      throw new KernelError(400, 'BAD_EVENT', 'eventTime 必须是非负整数（毫秒）', { field: 'eventTime' });
    }
    if (payload === undefined) {
      throw new KernelError(400, 'BAD_EVENT', 'payload 必须提供（可为 null）', { field: 'payload' });
    }
    if (
      e.arrivalTime !== undefined &&
      (!Number.isInteger(e.arrivalTime) || e.arrivalTime < eventTime)
    ) {
      throw new KernelError(
        400,
        'BAD_EVENT',
        'arrivalTime 必须为不小于 eventTime 的整数；网络延迟表现为二者之差',
        { field: 'arrivalTime' }
      );
    }
    return { partition, sequence, eventTime, arrivalTime: e.arrivalTime, payload };
  }

  // 落点判定。只构造记录，状态由 #apply 改变。
  #admit(id, result) {
    const ev = this.state.events.get(id);
    const p = this.state.partitions.get(ev.partition);
    const rule = this.currentRule;
    const orderLine = p.head - rule.allowedLateness;
    const confirmLine = p.head - 2 * rule.allowedLateness;

    if (ev.eventTime <= confirmLine) {
      this.#append({
        type: 'EVENT_LATE_DIAGNOSTIC',
        eventId: id,
        partition: ev.partition,
        eventTime: ev.eventTime,
        arrivalTime: ev.arrivalTime,
        sequence: ev.sequence,
        head: p.head,
        orderLine,
        confirmLine,
        lateness: ev.arrivalTime - ev.eventTime,
        ruleVersion: rule.version,
      });
      result.lateDiagnostics.push(this.#presentEvent(this.state.events.get(id)));
      return;
    }

    if (ev.eventTime <= orderLine) {
      this.#admitLate(ev, rule);
    } else {
      this.#append({ type: 'EVENT_BUFFERED', eventId: id, partition: ev.partition });
    }
    result.ingested.push(this.#presentEvent(this.state.events.get(id)));

    // 头事件时间只由参与处理的事件推进（晚于确认线的诊断事件不推进水位）
    if (ev.eventTime > p.head) {
      this.#appendWatermark(ev.partition, ev.eventTime, 'EVENT_HEAD', rule);
    }
    // 水位推进后无条件排空待定区并过确认线（早期缓冲事件可能在此刻才被排出）
    this.#drainPartition(ev.partition, rule, result);
    this.#confirmPartition(ev.partition, this.currentRule, result);
  }

  #appendWatermark(partition, newHead, reason, rule) {
    const p = this.state.partitions.get(partition);
    if (newHead <= p.head) return;
    this.#append({
      type: 'WATERMARK_ADVANCED',
      partition,
      head: newHead,
      orderLine: newHead - rule.allowedLateness,
      confirmLine: newHead - 2 * rule.allowedLateness,
      reason,
      ruleVersion: rule.version,
    });
  }

  // 迟到但在容忍窗口内：打开（或取得）窗口，按当前规则插入排序，产生一次 LATE_ARRIVAL 修订。
  #admitLate(ev, rule) {
    const start = windowStart(ev.eventTime, rule.windowSize);
    const key = windowKey(ev.partition, start);
    const win = this.state.windows.get(key);
    if (win && win.status === WINDOW_CONFIRMED) {
      const p = this.state.partitions.get(ev.partition);
      this.#append({
        type: 'EVENT_LATE_DIAGNOSTIC',
        eventId: ev.eventId,
        partition: ev.partition,
        eventTime: ev.eventTime,
        arrivalTime: ev.arrivalTime,
        sequence: ev.sequence,
        head: p.head,
        orderLine: p.head - rule.allowedLateness,
        confirmLine: p.head - 2 * rule.allowedLateness,
        lateness: ev.arrivalTime - ev.eventTime,
        ruleVersion: rule.version,
        note: '目标窗口已确认，拒绝改写',
      });
      return;
    }
    if (!win) this.#append({ type: 'WINDOW_OPENED', windowKey: key, start, ruleVersion: rule.version });

    const orderIndex = (this.state.orderIndexSeq.get(ev.partition) ?? 0) + 1;
    const existingRefs = (this.state.windows.get(key)?.events || []).map((r) => ({ ...r }));
    const refs = [...existingRefs, this.#refOf(ev, orderIndex)];
    this.#sortRefs(refs, rule);

    this.state.revisionSeq += 1;
    this.#append({
      type: 'WINDOW_REVISED',
      windowKey: key,
      revision: (this.state.windows.get(key)?.revision || 0) + 1,
      globalRevision: this.state.revisionSeq,
      reason: 'LATE_ARRIVAL',
      ruleVersion: rule.version,
      insertedEventId: ev.eventId,
      order: refs,
      changed: true,
    });
    this.#append({
      type: 'ORDER_EMITTED',
      partition: ev.partition,
      eventId: ev.eventId,
      orderIndex,
      windowKey: key,
      source: 'LATE_TOLERATED',
      ruleVersion: rule.version,
    });
    this.#append({
      type: 'LATE_REVISION',
      windowKey: key,
      partition: ev.partition,
      eventId: ev.eventId,
      eventTime: ev.eventTime,
      arrivalTime: ev.arrivalTime,
      lateness: ev.arrivalTime - ev.eventTime,
      revision: this.state.windows.get(key).revision,
      changed: true,
    });
  }

  // 排序线推进后，把所有 eventTime <= orderLine 的待定事件按窗口排出。
  #drainPartition(partition, rule, result) {
    const p = this.state.partitions.get(partition);
    const readyIds = [];
    const remaining = [];
    for (const id of p.pending) {
      const ev = this.state.events.get(id);
      if (ev.eventTime <= p.orderLine) readyIds.push(id);
      else remaining.push(id);
    }
    if (readyIds.length === 0) return;

    const ready = readyIds.map((id) => this.state.events.get(id));
    this.#sortEvents(ready, rule);
    // pending 是分区派生状态：只通过 PENDING_SETTLED 记录变更，保证快照与日志一致
    this.#append({ type: 'PENDING_SETTLED', partition, pending: remaining, released: readyIds });

    const groups = new Map();
    for (const ev of ready) {
      const start = windowStart(ev.eventTime, rule.windowSize);
      const key = windowKey(partition, start);
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(ev);
    }

    // 命令内连续序号游标：跨窗口分组也不能撞号；写回状态只发生在 ORDER_EMITTED 的 apply
    let cursor = this.state.orderIndexSeq.get(partition) ?? 0;
    for (const [key, events] of groups) {
      const start = Number(key.split('@')[1]);
      const existed = this.state.windows.has(key);
      if (!existed) this.#append({ type: 'WINDOW_OPENED', windowKey: key, start, ruleVersion: rule.version });

      const emitted = events.map((ev) => {
        cursor += 1;
        return { ev, orderIndex: cursor };
      });
      const refs = [
        ...(this.state.windows.get(key)?.events || []).map((r) => ({ ...r })),
        ...emitted.map(({ ev, orderIndex }) => this.#refOf(ev, orderIndex)),
      ];
      this.#sortRefs(refs, rule);

      this.state.revisionSeq += 1;
      this.#append({
        type: 'WINDOW_REVISED',
        windowKey: key,
        revision: (this.state.windows.get(key)?.revision || 0) + 1,
        globalRevision: this.state.revisionSeq,
        reason: existed ? 'BUFFER_FLUSH' : 'WATERMARK_PUBLISH',
        ruleVersion: rule.version,
        insertedEventId: null,
        order: refs,
        changed: true,
      });
      for (const { ev, orderIndex } of emitted) {
        this.#append({
          type: 'ORDER_EMITTED',
          partition,
          eventId: ev.eventId,
          orderIndex,
          windowKey: key,
          source: 'BUFFER_DRAIN',
          ruleVersion: rule.version,
        });
      }
      const win = this.state.windows.get(key);
      if (win && !result.windows.some((w) => w.windowKey === key)) {
        result.windows.push(this.#presentWindow(win));
      }
    }
  }

  // 确认线推进：锁定跨线事件；窗口事件全部锁定则关闭窗口。按分区窗口索引迭代，不扫全局。
  #confirmPartition(partition, rule, result) {
    const p = this.state.partitions.get(partition);
    const boundary = p.confirmLine;
    for (const key of [...p.windowKeys]) {
      const win = this.state.windows.get(key);
      if (!win || win.status === WINDOW_CONFIRMED) continue;

      const lockedIds = win.events
        .map((ref) => this.state.events.get(ref.eventId))
        .filter((ev) => ev && ev.status !== EVENT_CONFIRMED && ev.eventTime <= boundary)
        .map((ev) => ev.eventId);
      if (lockedIds.length === 0) continue;

      const allConfirmed = win.events.every(
        (ref) => this.state.events.get(ref.eventId)?.status === EVENT_CONFIRMED
      );
      // 注意：锁定记录中的“全部确认”要在应用锁定之后判断
      const willClose = win.events.every((ref) => {
        const ev = this.state.events.get(ref.eventId);
        return ev && (ev.status === EVENT_CONFIRMED || lockedIds.includes(ev.eventId));
      });

      this.#append({
        type: 'CONFIRM_BOUNDARY',
        partition,
        windowKey: key,
        confirmLine: boundary,
        lockedEventIds: lockedIds,
        windowClosed: willClose,
        ruleVersion: rule.version,
      });
      result.confirmations.push({
        windowKey: key,
        confirmLine: boundary,
        lockedEventIds: lockedIds,
        windowClosed: willClose,
      });
    }
  }

  heartbeat(partition, atArrival) {
    return this.#enqueue(() => {
      const arrival = atArrival ?? this.clock();
      this.state.systemTime = Math.max(this.state.systemTime, arrival);
      if (!this.state.partitions.has(partition)) {
        this.#append({ type: 'PARTITION_REGISTERED', partition, at: arrival });
      }
      const stored = this.#append({ type: 'HEARTBEAT', partition, arrivalTime: arrival });
      this.#idleSweep(partition, arrival);
      return this.#presentPartition(this.state.partitions.get(partition), stored._offset);
    });
  }

  tick(atArrival) {
    return this.#enqueue(() => {
      const now = atArrival ?? this.clock();
      this.state.systemTime = Math.max(this.state.systemTime, now);
      const changed = [];
      for (const partition of [...this.state.partitions.keys()]) {
        if (this.#idleSweep(partition, now)) changed.push(partition);
      }
      this.#append({ type: 'TICK', systemTime: now });
      return { systemTime: now, activityChanged: changed, resultView: this.resultView() };
    });
  }

  // 返回 true 表示该分区发生了 ACTIVE/IDLE 状态切换
  #idleSweep(partition, now) {
    const p = this.state.partitions.get(partition);
    if (!p) return false;
    const rule = this.currentRule;
    if (p.status !== PARTITION_IDLE && now - p.lastActivity >= rule.idleTimeout) {
      this.#append({ type: 'PARTITION_IDLE', partition, since: now });
      return true;
    }
    if (p.status === PARTITION_IDLE && now - p.lastActivity < rule.idleTimeout) {
      this.#append({ type: 'PARTITION_ACTIVE', partition, at: now });
      return true;
    }
    return false;
  }

  activateRule(changes, { expectedRuleVersion, reason = 'MANUAL' } = {}) {
    return this.#enqueue(() => {
      const current = this.currentRule;
      if (expectedRuleVersion !== undefined && expectedRuleVersion !== current.version) {
        throw new KernelError(409, 'RULE_VERSION_CONFLICT', '规则版本已变化，请基于最新版本重试', {
          expected: expectedRuleVersion,
          current: current.version,
        });
      }
      const rule = {
        ruleVersion: current.version + 1,
        allowedLateness: num(changes.allowedLateness, current.allowedLateness),
        windowSize: num(changes.windowSize, current.windowSize),
        idleTimeout: num(changes.idleTimeout, current.idleTimeout),
        orderBy: changes.orderBy ?? current.orderBy,
      };
      if (rule.allowedLateness <= 0 || rule.windowSize <= 0 || rule.idleTimeout <= 0) {
        throw new KernelError(400, 'BAD_RULE', 'allowedLateness/windowSize/idleTimeout 必须为正数');
      }
      if (![ORDER_BY_EVENT_TIME_SEQ, ORDER_BY_SEQUENCE].includes(rule.orderBy)) {
        throw new KernelError(400, 'BAD_RULE', `未知 orderBy: ${rule.orderBy}`);
      }

      // 先记录哪些窗口将被标记过期（在 RULE_ACTIVATED 应用前判定）
      const staleKeys = [...this.state.windows.values()]
        .filter((w) => w.status !== WINDOW_CONFIRMED)
        .map((w) => w.key);

      this.#append({
        type: 'RULE_ACTIVATED',
        rule,
        reason,
        replacedVersion: current.version,
        staleWindowKeys: staleKeys,
      });

      const result = { ingested: [], duplicates: [], conflicts: [], lateDiagnostics: [], windows: [], confirmations: [] };
      const now = Math.max(this.clock(), this.state.systemTime);
      this.state.systemTime = now;
      for (const partition of [...this.state.partitions.keys()]) {
        const p = this.state.partitions.get(partition);
        if (p.head < 0) {
          this.#idleSweep(partition, now);
          continue;
        }
        const newOrderLine = p.head - rule.allowedLateness;
        const newConfirmLine = p.head - 2 * rule.allowedLateness;
        if (newOrderLine > p.orderLine || newConfirmLine > p.confirmLine) {
          this.#append({
            type: 'WATERMARK_ADVANCED',
            partition,
            head: p.head,
            orderLine: Math.max(p.orderLine, newOrderLine),
            confirmLine: Math.max(p.confirmLine, newConfirmLine),
            reason: 'RULE_CHANGE',
            ruleVersion: rule.version,
          });
        }
        this.#drainPartition(partition, rule, result);
        this.#confirmPartition(partition, rule, result);
        this.#idleSweep(partition, now);
      }

      return { rule: this.#presentRule(this.currentRule), staleWindows: staleKeys, affected: result };
    });
  }

  recomputeWindow(key, { expectedRuleVersion } = {}) {
    return this.#enqueue(() => {
      const win = this.state.windows.get(key);
      if (!win) throw new KernelError(404, 'WINDOW_NOT_FOUND', `窗口不存在: ${key}`);
      if (win.status === WINDOW_CONFIRMED) {
        throw new KernelError(
          409,
          'WINDOW_CONFIRMED_LOCKED',
          '窗口已确认，不能重算；迟到事件只能留诊断',
          { windowKey: key, confirmedAtOffset: win.confirmedAtOffset }
        );
      }
      const rule = this.currentRule;
      if (expectedRuleVersion !== undefined && expectedRuleVersion !== rule.version) {
        throw new KernelError(409, 'RULE_VERSION_CONFLICT', '规则版本已变化，旧重算请求被拒绝', {
          expected: expectedRuleVersion,
          current: rule.version,
        });
      }

      const p = this.state.partitions.get(win.partition);
      const memberIds = new Set(
        win.events
          .map((r) => r.eventId)
          .concat(
            p.pending.filter((id) => {
              const ev = this.state.events.get(id);
              return ev && ev.eventTime >= win.start && ev.eventTime < win.end;
            })
          )
      );
      const members = [...memberIds]
        .map((id) => this.state.events.get(id))
        .filter((ev) => ev && ev.status !== EVENT_LATE_DIAG);
      const promoted = members.filter((ev) => ev.status === EVENT_PENDING);

      const beforeIds = win.events.map((r) => r.eventId);
      const baseIndex = this.state.orderIndexSeq.get(win.partition) ?? 0;
      const emitted = promoted.map((ev, i) => ({ ev, orderIndex: baseIndex + i + 1 }));
      const refs = members.map((ev) => {
        const hit = emitted.find((x) => x.ev === ev);
        return this.#refOf(ev, hit ? hit.orderIndex : ev.orderIndex);
      });
      this.#sortRefs(refs, rule);
      const afterIds = refs.map((r) => r.eventId);
      const changed = JSON.stringify(beforeIds) !== JSON.stringify(afterIds);

      if (promoted.length > 0) {
        this.#append({
          type: 'PENDING_SETTLED',
          partition: win.partition,
          pending: p.pending.filter((id) => !promoted.some((ev) => ev.eventId === id)),
          released: promoted.map((ev) => ev.eventId),
        });
      }
      for (const { ev, orderIndex } of emitted) {
        this.#append({
          type: 'ORDER_EMITTED',
          partition: win.partition,
          eventId: ev.eventId,
          orderIndex,
          windowKey: key,
          source: 'MANUAL_RECOMPUTE_PROMOTE',
          ruleVersion: rule.version,
        });
      }

      this.state.revisionSeq += 1;
      this.#append({
        type: 'WINDOW_REVISED',
        windowKey: key,
        revision: win.revision + 1,
        globalRevision: this.state.revisionSeq,
        reason: 'MANUAL_RECOMPUTE',
        ruleVersion: rule.version,
        insertedEventId: null,
        order: refs,
        changed,
      });

      const result = { ingested: [], duplicates: [], conflicts: [], lateDiagnostics: [], windows: [], confirmations: [] };
      this.#confirmPartition(win.partition, rule, result);

      return {
        window: this.#presentWindow(this.state.windows.get(key)),
        changed,
        promotedEventIds: promoted.map((ev) => ev.eventId),
        confirmations: result.confirmations,
      };
    });
  }

  snapshot() {
    return this.#enqueue(() => {
      const offset = this.store.nextOffset - 1;
      this.store.saveSnapshot(offset, this.#serializeState());
      return { offset, events: this.state.events.size };
    });
  }

  close() {
    this.store.close();
  }

  #refOf(ev, orderIndex) {
    return {
      eventId: ev.eventId,
      eventTime: ev.eventTime,
      arrivalTime: ev.arrivalTime,
      sequence: ev.sequence,
      orderIndex,
      contentHash: ev.contentHash,
    };
  }

  #sortEvents(list, rule) {
    list.sort((a, b) =>
      rule.orderBy === ORDER_BY_SEQUENCE
        ? a.sequence - b.sequence || a.eventTime - b.eventTime
        : a.eventTime - b.eventTime || a.sequence - b.sequence
    );
  }

  #sortRefs(refs, rule) {
    refs.sort((a, b) =>
      rule.orderBy === ORDER_BY_SEQUENCE
        ? a.sequence - b.sequence || a.eventTime - b.eventTime
        : a.eventTime - b.eventTime || a.sequence - b.sequence
    );
  }

  // —— 状态记录应用：导入与重放走同一套逻辑 ——
  #apply(entry) {
    switch (entry.type) {
      case 'RULE_ACTIVATED': {
        if (!this.state.rules.some((r) => r.version === entry.rule.ruleVersion)) {
          if (entry.replacedVersion !== null) {
            const old = this.state.rules.find((r) => r.version === entry.replacedVersion);
            if (old) old.status = RULE_SUPERSEDED;
          }
          this.state.rules.push({
            version: entry.rule.ruleVersion,
            allowedLateness: entry.rule.allowedLateness,
            windowSize: entry.rule.windowSize,
            idleTimeout: entry.rule.idleTimeout,
            orderBy: entry.rule.orderBy,
            status: RULE_ACTIVE,
            reason: entry.reason,
            activatedAtOffset: entry._offset ?? null,
          });
          this.state.currentRuleVersion = entry.rule.ruleVersion;
        }
        for (const key of entry.staleWindowKeys || []) {
          const win = this.state.windows.get(key);
          if (win && win.status !== WINDOW_CONFIRMED) win.staleForRule = entry.rule.ruleVersion;
        }
        break;
      }
      case 'PARTITION_REGISTERED': {
        if (!this.state.partitions.has(entry.partition)) {
          this.state.partitions.set(entry.partition, {
            name: entry.partition,
            registeredAt: entry.at,
            lastActivity: entry.at,
            head: -1,
            orderLine: -Infinity,
            confirmLine: -Infinity,
            status: PARTITION_ACTIVE,
            idleSince: null,
            pending: [],
            windowKeys: [],
          });
        }
        break;
      }
      case 'PARTITION_IDLE': {
        const p = this.state.partitions.get(entry.partition);
        if (p) {
          p.status = PARTITION_IDLE;
          p.idleSince = entry.since;
        }
        break;
      }
      case 'PARTITION_ACTIVE': {
        const p = this.state.partitions.get(entry.partition);
        if (p) {
          p.status = PARTITION_ACTIVE;
          p.idleSince = null;
          p.lastActivity = Math.max(p.lastActivity, entry.at);
        }
        break;
      }
      case 'HEARTBEAT': {
        const p = this.state.partitions.get(entry.partition);
        if (p) p.lastActivity = Math.max(p.lastActivity, entry.arrivalTime);
        this.state.systemTime = Math.max(this.state.systemTime, entry.arrivalTime);
        break;
      }
      case 'TICK': {
        this.state.systemTime = Math.max(this.state.systemTime, entry.systemTime);
        break;
      }
      case 'EVENT_IMPORTED': {
        const e = entry.event;
        if (!this.state.partitions.has(e.partition)) {
          this.state.partitions.set(e.partition, {
            name: e.partition,
            registeredAt: e.arrivalTime,
            lastActivity: e.arrivalTime,
            head: -1,
            orderLine: -Infinity,
            confirmLine: -Infinity,
            status: PARTITION_ACTIVE,
            idleSince: null,
            pending: [],
            windowKeys: [],
          });
        }
        const p = this.state.partitions.get(e.partition);
        p.lastActivity = Math.max(p.lastActivity, e.arrivalTime);
        this.state.systemTime = Math.max(this.state.systemTime, e.arrivalTime);
        this.state.events.set(e.eventId, {
          eventId: e.eventId,
          partition: e.partition,
          eventTime: e.eventTime,
          arrivalTime: e.arrivalTime,
          sequence: e.sequence,
          payload: e.payload,
          contentHash: e.contentHash,
          status: null,
          windowKey: null,
          orderIndex: null,
          admittedBy: null,
          lifecycle: [
            { atOffset: entry._offset ?? null, transition: 'IMPORTED', note: '事件导入' },
          ],
        });
        break;
      }
      case 'EVENT_BUFFERED': {
        const ev = this.state.events.get(entry.eventId);
        const p = this.state.partitions.get(entry.partition);
        if (ev && p && !p.pending.includes(entry.eventId)) {
          ev.status = EVENT_PENDING;
          sortedInsert(p.pending, entry.eventId, (a, b) => {
            const ea = this.state.events.get(a);
            const eb = this.state.events.get(b);
            return ea.eventTime - eb.eventTime || ea.sequence - eb.sequence;
          });
          ev.lifecycle.push({
            atOffset: entry._offset ?? null,
            transition: 'BUFFERED',
            note: '进入待定区（eventTime 晚于排序线）',
          });
        }
        break;
      }
      case 'PENDING_SETTLED': {
        const p = this.state.partitions.get(entry.partition);
        if (p) p.pending = entry.pending.slice();
        break;
      }
      case 'EVENT_LATE_DIAGNOSTIC': {
        const ev = this.state.events.get(entry.eventId);
        if (ev) {
          ev.status = EVENT_LATE_DIAG;
          ev.lifecycle.push({
            atOffset: entry._offset ?? null,
            transition: 'LATE_DIAGNOSTIC',
            note: entry.note ?? '晚于确认线，仅诊断不改写已确认结果',
            meta: { orderLine: entry.orderLine, confirmLine: entry.confirmLine },
          });
        }
        this.state.anomalies.push({
          kind: 'LATE_DIAGNOSTIC',
          atOffset: entry._offset ?? null,
          eventId: entry.eventId,
          partition: entry.partition,
          eventTime: entry.eventTime,
          arrivalTime: entry.arrivalTime,
          lateness: entry.lateness ?? entry.arrivalTime - entry.eventTime,
          orderLine: entry.orderLine,
          confirmLine: entry.confirmLine,
          ruleVersion: entry.ruleVersion,
          note: entry.note ?? null,
        });
        break;
      }
      case 'WATERMARK_ADVANCED': {
        const p = this.state.partitions.get(entry.partition);
        if (p) {
          p.head = entry.head;
          p.orderLine = entry.orderLine;
          p.confirmLine = entry.confirmLine;
        }
        break;
      }
      case 'WINDOW_OPENED': {
        if (!this.state.windows.has(entry.windowKey)) {
          const [partition, startStr] = entry.windowKey.split('@');
          const start = entry.start ?? Number(startStr);
          const rule = this.ruleAt(entry.ruleVersion) || this.currentRule;
          const win = {
            key: entry.windowKey,
            partition,
            start,
            end: start + rule.windowSize,
            status: WINDOW_OPEN,
            events: [],
            revision: 0,
            revisions: [],
            publishedRuleVersion: entry.ruleVersion,
            confirmedRuleVersion: null,
            confirmedAtOffset: null,
            staleForRule: null,
            openedAtOffset: entry._offset ?? null,
          };
          this.state.windows.set(entry.windowKey, win);
          const p = this.state.partitions.get(partition);
          if (p && !p.windowKeys.includes(entry.windowKey)) p.windowKeys.push(entry.windowKey);
        }
        break;
      }
      case 'WINDOW_REVISED': {
        const win = this.state.windows.get(entry.windowKey);
        if (win) {
          win.status = WINDOW_ORDERED;
          win.publishedRuleVersion = entry.ruleVersion;
          win.staleForRule = null;
          win.revision = entry.revision;
          win.events = entry.order.map((r) => ({ ...r }));
          win.revisions.push({
            revision: entry.revision,
            globalRevision: entry.globalRevision,
            reason: entry.reason,
            ruleVersion: entry.ruleVersion,
            insertedEventId: entry.insertedEventId ?? null,
            order: entry.order.map((r) => ({ ...r })),
            changed: entry.changed ?? true,
            atOffset: entry._offset ?? null,
          });
        }
        // 重放时从记录派生全局修订号，保证重启后继续递增、不复用旧号
        if (typeof entry.globalRevision === 'number') {
          this.state.revisionSeq = Math.max(this.state.revisionSeq, entry.globalRevision);
        }
        break;
      }
      case 'ORDER_EMITTED': {
        const ev = this.state.events.get(entry.eventId);
        if (ev) {
          ev.orderIndex = entry.orderIndex;
          ev.windowKey = entry.windowKey;
          if (ev.status !== EVENT_CONFIRMED && ev.status !== EVENT_LATE_DIAG) ev.status = EVENT_ORDERED;
          ev.admittedBy = entry.source;
          ev.lifecycle.push({
            atOffset: entry._offset ?? null,
            transition: 'ORDERED',
            note: `按规则 v${entry.ruleVersion} 排序发布（${entry.source}）`,
            meta: { orderIndex: entry.orderIndex, source: entry.source },
          });
        }
        const cur = this.state.orderIndexSeq.get(entry.partition) ?? 0;
        if (entry.orderIndex > cur) this.state.orderIndexSeq.set(entry.partition, entry.orderIndex);
        break;
      }
      case 'CONFIRM_BOUNDARY': {
        const p = this.state.partitions.get(entry.partition);
        if (p) p.confirmLine = entry.confirmLine;
        for (const id of entry.lockedEventIds) {
          const ev = this.state.events.get(id);
          if (ev && ev.status !== EVENT_CONFIRMED) {
            ev.status = EVENT_CONFIRMED;
            ev.lifecycle.push({
              atOffset: entry._offset ?? null,
              transition: 'CONFIRMED',
              note: `确认边界 ${entry.confirmLine} 锁定（规则 v${entry.ruleVersion}）`,
            });
          }
        }
        if (entry.windowClosed) {
          const win = this.state.windows.get(entry.windowKey);
          if (win) {
            win.status = WINDOW_CONFIRMED;
            win.confirmedAtOffset = entry._offset ?? null;
            win.confirmedRuleVersion = win.publishedRuleVersion;
          }
        }
        break;
      }
      case 'DUPLICATE_OBSERVED': {
        const ev = this.state.events.get(entry.eventId);
        if (ev) {
          ev.lifecycle.push({
            atOffset: entry._offset ?? null,
            transition: 'DUPLICATE_IGNORED',
            note: `相同内容重传（arrival=${entry.arrivalTime}），幂等忽略`,
          });
        }
        const p = this.state.partitions.get(entry.partition);
        if (p) p.lastActivity = Math.max(p.lastActivity, entry.arrivalTime);
        this.state.systemTime = Math.max(this.state.systemTime, entry.arrivalTime);
        this.state.anomalies.push({
          kind: 'DUPLICATE',
          atOffset: entry._offset ?? null,
          eventId: entry.eventId,
          partition: entry.partition,
          eventTime: entry.eventTime,
          arrivalTime: entry.arrivalTime,
          sequence: entry.sequence,
          contentHash: entry.contentHash,
        });
        break;
      }
      case 'CONFLICT_OBSERVED': {
        this.state.anomalies.push({
          kind: 'CONFLICT',
          atOffset: entry._offset ?? null,
          eventId: entry.eventId,
          partition: entry.partition,
          eventTime: entry.eventTime,
          arrivalTime: entry.arrivalTime,
          sequence: entry.sequence,
          winnerContentHash: entry.winnerContentHash,
          rejectedContentHash: entry.rejectedContentHash,
          rejectedPayload: entry.rejectedPayload,
          rejectedArrivalTime: entry.rejectedArrivalTime,
        });
        const ev = this.state.events.get(entry.eventId);
        if (ev) {
          ev.lifecycle.push({
            atOffset: entry._offset ?? null,
            transition: 'CONFLICT_REJECTED',
            note: '同序列号不同内容，首写获胜；拒绝内容见冲突诊断',
          });
        }
        const p = this.state.partitions.get(entry.partition);
        if (p) p.lastActivity = Math.max(p.lastActivity, entry.arrivalTime);
        this.state.systemTime = Math.max(this.state.systemTime, entry.arrivalTime);
        break;
      }
      case 'LATE_REVISION': {
        this.state.anomalies.push({
          kind: 'LATE_REVISION',
          atOffset: entry._offset ?? null,
          windowKey: entry.windowKey,
          partition: entry.partition,
          eventId: entry.eventId,
          eventTime: entry.eventTime,
          arrivalTime: entry.arrivalTime,
          lateness: entry.lateness,
          revision: entry.revision,
          changed: entry.changed,
        });
        break;
      }
      default:
        throw new KernelError(500, 'UNKNOWN_ENTRY', `未知状态记录类型: ${entry.type}`);
    }
  }

  #serializeState() {
    return {
      systemTime: this.state.systemTime,
      rules: this.state.rules,
      currentRuleVersion: this.state.currentRuleVersion,
      partitions: [...this.state.partitions.values()],
      events: [...this.state.events.values()],
      windows: [...this.state.windows.values()],
      anomalies: this.state.anomalies,
      revisionSeq: this.state.revisionSeq,
      orderIndexSeq: [...this.state.orderIndexSeq.entries()],
    };
  }

  static restoreState(data) {
    const s = freshState();
    s.systemTime = data.systemTime;
    s.rules = data.rules;
    s.currentRuleVersion = data.currentRuleVersion;
    for (const p of data.partitions) s.partitions.set(p.name, p);
    for (const e of data.events) s.events.set(e.eventId, e);
    for (const w of data.windows) s.windows.set(w.key, w);
    s.anomalies = data.anomalies;
    s.revisionSeq = data.revisionSeq;
    s.orderIndexSeq = new Map(data.orderIndexSeq);
    return s;
  }

  // —— 查询投影 ——
  #presentEvent(ev) {
    return {
      eventId: ev.eventId,
      partition: ev.partition,
      sequence: ev.sequence,
      eventTime: ev.eventTime,
      arrivalTime: ev.arrivalTime,
      networkDelay: ev.arrivalTime - ev.eventTime,
      status: ev.status,
      windowKey: ev.windowKey,
      orderIndex: ev.orderIndex,
      contentHash: ev.contentHash,
      admittedBy: ev.admittedBy,
      lifecycle: ev.lifecycle.map((l) => ({ ...l })),
      payload: ev.payload,
    };
  }

  #presentRule(rule) {
    return {
      ruleVersion: rule.version,
      allowedLateness: rule.allowedLateness,
      windowSize: rule.windowSize,
      idleTimeout: rule.idleTimeout,
      orderBy: rule.orderBy,
      status: rule.status,
      reason: rule.reason,
      activatedAtOffset: rule.activatedAtOffset,
    };
  }

  #presentPartition(p, atOffset = null) {
    if (!p) return null;
    return {
      partition: p.name,
      status: p.status,
      head: p.head,
      orderLine: p.head < 0 ? null : p.orderLine,
      confirmLine: p.head < 0 ? null : p.confirmLine,
      lastActivity: p.lastActivity,
      idleSince: p.idleSince,
      pendingCount: p.pending.length,
      windowCount: p.windowKeys.length,
      contributesToGlobalWatermark: p.status === PARTITION_ACTIVE,
      observedAtOffset: atOffset ?? this.offset,
    };
  }

  #presentWindow(win) {
    const p = this.state.partitions.get(win.partition);
    const confirmLine = p ? p.confirmLine : -Infinity;
    return {
      windowKey: win.key,
      partition: win.partition,
      start: win.start,
      end: win.end,
      status: win.status,
      staleForRule: win.staleForRule,
      revision: win.revision,
      publishedRuleVersion: win.publishedRuleVersion,
      confirmedRuleVersion: win.confirmedRuleVersion,
      confirmedAtOffset: win.confirmedAtOffset,
      events: win.events.map((ref) => {
        const ev = this.state.events.get(ref.eventId);
        return {
          ...ref,
          status: ev ? ev.status : null,
          confirmed: ev ? ev.status === EVENT_CONFIRMED : false,
          locked: ev ? ev.eventTime <= confirmLine : false,
          networkDelay: ev ? ev.arrivalTime - ev.eventTime : null,
        };
      }),
      revisions: win.revisions.map((r) => ({
        ...r,
        order: r.order.map((x) => (typeof x === 'string' ? x : x.eventId)),
      })),
      confirmedEventIds: win.events
        .map((ref) => this.state.events.get(ref.eventId))
        .filter((ev) => ev && ev.status === EVENT_CONFIRMED)
        .map((ev) => ev.eventId),
    };
  }

  #globalWatermark() {
    const active = [...this.state.partitions.values()].filter(
      (p) => p.status === PARTITION_ACTIVE && p.head >= 0
    );
    if (active.length === 0) {
      return {
        orderLine: null,
        confirmLine: null,
        contributors: [],
        idlePartitions: [...this.state.partitions.values()]
          .filter((p) => p.status === PARTITION_IDLE)
          .map((p) => p.name),
      };
    }
    return {
      orderLine: Math.min(...active.map((p) => p.orderLine)),
      confirmLine: Math.min(...active.map((p) => p.confirmLine)),
      contributors: active.map((p) => p.name),
      idlePartitions: [...this.state.partitions.values()]
        .filter((p) => p.status === PARTITION_IDLE)
        .map((p) => p.name),
    };
  }

  resultView({ partition } = {}) {
    const windows = [...this.state.windows.values()]
      .filter((w) => !partition || w.partition === partition)
      .sort((a, b) => a.start - b.start || a.partition.localeCompare(b.partition))
      .map((w) => this.#presentWindow(w));

    return {
      asOfOffset: this.offset,
      systemTime: this.state.systemTime,
      currentRule: this.currentRule ? this.#presentRule(this.currentRule) : null,
      globalWatermark: this.#globalWatermark(),
      partitions: [...this.state.partitions.values()]
        .filter((p) => !partition || p.name === partition)
        .map((p) => this.#presentPartition(p)),
      windows,
      counts: {
        events: this.state.events.size,
        pending: [...this.state.partitions.values()]
          .filter((p) => !partition || p.name === partition)
          .reduce((n, p) => n + p.pending.length, 0),
        ordered: windows.reduce(
          (n, w) => n + w.events.filter((e) => e.status === EVENT_ORDERED).length,
          0
        ),
        confirmed: windows.reduce((n, w) => n + w.confirmedEventIds.length, 0),
        lateDiagnostic: this.state.anomalies.filter(
          (a) => a.kind === 'LATE_DIAGNOSTIC' && (!partition || a.partition === partition)
        ).length,
      },
    };
  }

  pendingView({ partition } = {}) {
    const out = [];
    for (const p of this.state.partitions.values()) {
      if (partition && p.name !== partition) continue;
      for (const id of p.pending) {
        const ev = this.state.events.get(id);
        if (ev) {
          out.push({
            ...this.#presentEvent(ev),
            waitingReason:
              ev.eventTime > p.orderLine
                ? `eventTime ${ev.eventTime} > 排序线 ${p.orderLine}，等待水位推进`
                : '等待排空',
            windowKeyWouldBe: windowKey(p.name, windowStart(ev.eventTime, this.currentRule.windowSize)),
          });
        }
      }
    }
    out.sort((a, b) => a.eventTime - b.eventTime || a.partition.localeCompare(b.partition));
    return { asOfOffset: this.offset, pending: out };
  }

  timelineView({ fromOffset = 0, limit = 100, partition } = {}) {
    const all = this.store.readEntries();
    const filtered = all.filter((e) => {
      if (e._offset < fromOffset) return false;
      if (!partition) return true;
      const inPartition = e.partition === partition;
      const inWindow =
        typeof e.windowKey === 'string' && e.windowKey.startsWith(`${partition}@`);
      const isEvent = e.event?.partition === partition;
      return inPartition || inWindow || isEvent;
    });
    return {
      asOfOffset: this.offset,
      entries: filtered.slice(0, limit).map((e) => this.#presentTimelineEntry(e)),
      hasMore: filtered.length > limit,
    };
  }

  #presentTimelineEntry(e) {
    const base = { offset: e._offset, type: e.type };
    switch (e.type) {
      case 'RULE_ACTIVATED':
        return {
          ...base,
          ruleVersion: e.rule.ruleVersion,
          changed: {
            allowedLateness: e.rule.allowedLateness,
            windowSize: e.rule.windowSize,
            idleTimeout: e.rule.idleTimeout,
            orderBy: e.rule.orderBy,
          },
          replacedVersion: e.replacedVersion,
          reason: e.reason,
        };
      case 'EVENT_IMPORTED':
        return {
          ...base,
          eventId: e.event.eventId,
          partition: e.event.partition,
          eventTime: e.event.eventTime,
          arrivalTime: e.event.arrivalTime,
          sequence: e.event.sequence,
        };
      case 'WINDOW_REVISED':
        return {
          ...base,
          windowKey: e.windowKey,
          revision: e.revision,
          reason: e.reason,
          ruleVersion: e.ruleVersion,
          changed: e.changed,
          order: e.order.map((x) => (typeof x === 'string' ? x : x.eventId)),
        };
      case 'CONFIRM_BOUNDARY':
        return {
          ...base,
          partition: e.partition,
          windowKey: e.windowKey,
          confirmLine: e.confirmLine,
          lockedEventIds: e.lockedEventIds,
          windowClosed: e.windowClosed,
        };
      case 'WATERMARK_ADVANCED':
        return {
          ...base,
          partition: e.partition,
          head: e.head,
          orderLine: e.orderLine,
          confirmLine: e.confirmLine,
          ruleVersion: e.ruleVersion,
        };
      case 'ORDER_EMITTED':
        return {
          ...base,
          partition: e.partition,
          eventId: e.eventId,
          orderIndex: e.orderIndex,
          windowKey: e.windowKey,
          source: e.source,
        };
      default:
        return { ...base, ...e, _offset: undefined };
    }
  }

  replayView({ fromOffset = 0, toOffset, limit = 200 } = {}) {
    const all = this.store.readEntries();
    const slice = all.filter(
      (e) =>
        e._offset >= fromOffset && (toOffset === undefined || e._offset <= toOffset)
    );
    return {
      asOfOffset: this.offset,
      fromOffset,
      toOffset: toOffset ?? this.offset,
      entries: slice.slice(0, limit),
      hasMore: slice.length > limit,
    };
  }

  anomaliesView({ kind, partition } = {}) {
    return {
      asOfOffset: this.offset,
      anomalies: this.state.anomalies.filter(
        (a) => (!kind || a.kind === kind) && (!partition || a.partition === partition)
      ),
    };
  }

  eventDetailView(id) {
    const ev = this.state.events.get(id);
    if (!ev) throw new KernelError(404, 'EVENT_NOT_FOUND', `事件不存在: ${id}`);
    const related = this.state.anomalies.filter((a) => a.eventId === id);
    const win = ev.windowKey ? this.state.windows.get(ev.windowKey) : null;
    return {
      event: this.#presentEvent(ev),
      anomalies: related,
      window: win ? this.#presentWindow(win) : null,
    };
  }

  partitionView(name) {
    const p = this.state.partitions.get(name);
    if (!p) throw new KernelError(404, 'PARTITION_NOT_FOUND', `分区不存在: ${name}`);
    return {
      partition: this.#presentPartition(p),
      rule: this.#presentRule(this.currentRule),
      windows: p.windowKeys
        .map((k) => this.state.windows.get(k))
        .filter(Boolean)
        .map((w) => this.#presentWindow(w)),
      pending: this.pendingView({ partition: name }).pending,
      events: [...this.state.events.values()]
        .filter((e) => e.partition === name)
        .sort((a, b) => a.eventTime - b.eventTime || a.sequence - b.sequence)
        .map((e) => this.#presentEvent(e)),
    };
  }
}

function num(value, fallback) {
  if (value === undefined) return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}
