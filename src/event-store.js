/**
 * event-store.js — 事件存储
 *
 * 两类记录必须分开保存：
 *
 *  1. arrivals：所有“到达过”的原始记录（按到达时间登记），包括
 *     accepted / duplicate / conflict / expired-late 四类。
 *     即使迟到事件无法改写已确认结果，它的原始事实仍然可追溯。
 *
 *  2. 分区有序索引：只有 accepted 事件进入。按 eventTime 升序排列，
 *     并重排时用二分定位区间读取，避免每次从头扫描全部事件。
 *
 * 计数器（stats）让“服务端不能每次从头扫描全部事件”成为可验证的事实：
 *  - rangeReads / fullScans 会暴露给 /debug/stats。
 */

import { lowerBound, upperBound, fingerprintEvent, businessFingerprint } from './util.js';

export class EventStore {
  constructor() {
    /** eventId -> arrival 原始记录（含状态） */
    this.arrivals = new Map();
    /** (partition + '#' + seq) -> 第一次 accepted 的 eventId，用于同序号冲突检测 */
    this.seqIndex = new Map();
    /** partition -> { byTime: [event,...] 按eventTime升序, seqSet:Set } */
    this.partitions = new Map();
    /** batchId -> { eventIds: [...], outcome: 'applied'|'ignored', at } */
    this.batches = new Map();

    this.stats = {
      totalArrivals: 0,
      acceptedCount: 0,
      duplicateCount: 0,
      conflictCount: 0,
      expiredLateCount: 0,
      rangeReads: 0,
      fullScans: 0,
    };
  }

  _partition(partition) {
    let p = this.partitions.get(partition);
    if (!p) {
      p = { byTime: [], seqSet: new Set() };
      this.partitions.set(partition, p);
    }
    return p;
  }

  /**
   * 登记一次到达。调用方（kernel）先做分类，再调用对应方法。
   * 返回登记后的 arrival 记录。
   */
  registerArrival(event, status, detail = {}) {
    // 相同 eventId 再次到达：首次事实不可变，只在原记录上追加重传收据
    const prior = this.arrivals.get(event.eventId);
    if (prior) {
      prior.repeats = prior.repeats ?? [];
      prior.repeats.push({
        arrivalTime: event.arrivalTime,
        fingerprint: fingerprintEvent(event),
        status,
        detail,
      });
      this.stats.totalArrivals += 1;
      if (status === 'DUPLICATE') this.stats.duplicateCount += 1;
      else if (status === 'CONFLICT') this.stats.conflictCount += 1;
      else if (status === 'EXPIRED_LATE') this.stats.expiredLateCount += 1;
      return prior;
    }
    const at = {
      eventId: event.eventId,
      partition: event.partition,
      seq: event.seq,
      eventTime: event.eventTime,
      arrivalTime: event.arrivalTime,
      data: event.data ?? null,
      fingerprint: fingerprintEvent(event),
      fingerprintBusiness: businessFingerprint(event),
      status, // ACCEPTED | DUPLICATE | CONFLICT | EXPIRED_LATE
      detail,
    };
    this.arrivals.set(event.eventId, at);
    this.stats.totalArrivals += 1;
    if (status === 'ACCEPTED') this.stats.acceptedCount += 1;
    else if (status === 'DUPLICATE') this.stats.duplicateCount += 1;
    else if (status === 'CONFLICT') this.stats.conflictCount += 1;
    else if (status === 'EXPIRED_LATE') this.stats.expiredLateCount += 1;
    return at;
  }

  getArrival(eventId) {
    return this.arrivals.get(eventId) ?? null;
  }

  hasArrival(eventId) {
    return this.arrivals.has(eventId);
  }

  /** accepted 事件进入有序索引 */
  indexAccepted(event) {
    const p = this._partition(event.partition);
    const insertAt = lowerBound(p.byTime, event.eventTime, (e) => e.eventTime);
    // 同 eventTime 下按 seq 再排，保证顺序稳定
    let pos = insertAt;
    while (
      pos < p.byTime.length &&
      p.byTime[pos].eventTime === event.eventTime &&
      p.byTime[pos].seq < event.seq
    ) {
      pos += 1;
    }
    p.byTime.splice(pos, 0, event);
    p.seqSet.add(event.seq);
    this.seqIndex.set(`${event.partition}#${event.seq}`, event.eventId);
  }

  acceptedEventAtSeq(partition, seq) {
    const id = this.seqIndex.get(`${partition}#${seq}`);
    return id ? this.arrivals.get(id) : null;
  }

  getKnownPartitions() {
    return [...this.partitions.keys()].sort();
  }

  /**
   * 区间读取 accepted 事件：eventTime ∈ [from, to)
   * 使用二分定位，不触碰其他分区、其他窗口。
   */
  readRange(partition, from, to) {
    const p = this.partitions.get(partition);
    this.stats.rangeReads += 1;
    if (!p) return [];
    const start = lowerBound(p.byTime, from, (e) => e.eventTime);
    const end = upperBound(p.byTime, to - 1, (e) => e.eventTime);
    return p.byTime.slice(start, end);
  }

  /**
   * 从某起点开始读取（重算 tail 时用）。只扫描受影响分区，从指定 eventTime 起。
   * bounded=true 时必须给出 to。
   */
  readFrom(partition, from, to = null) {
    const p = this.partitions.get(partition);
    this.stats.rangeReads += 1;
    if (!p) return [];
    const start = lowerBound(p.byTime, from, (e) => e.eventTime);
    if (to === null) return p.byTime.slice(start);
    const end = upperBound(p.byTime, to - 1, (e) => e.eventTime);
    return p.byTime.slice(start, end);
  }

  /** 仅供诊断/快照使用的全量读取，会计入 fullScans */
  readAllAccepted() {
    this.stats.fullScans += 1;
    const out = [];
    for (const p of this.partitions.values()) out.push(...p.byTime);
    return out;
  }

  registerBatch(batchId, eventIds, outcome, at) {
    this.batches.set(batchId, { batchId, eventIds: [...eventIds], outcome, at });
  }

  getBatch(batchId) {
    return this.batches.get(batchId) ?? null;
  }

  resetStats() {
    const snapshot = { ...this.stats };
    this.stats.rangeReads = 0;
    this.stats.fullScans = 0;
    return snapshot;
  }

  /** 序列化为快照（供 journal snapshot 使用）。
   *  rangeReads/fullScans 是性能观测计数，不属于业务状态，不进快照、不参与回放比对。 */
  toJSON() {
    const { rangeReads, fullScans, ...businessStats } = this.stats;
    return {
      arrivals: [...this.arrivals.values()],
      seqIndex: [...this.seqIndex.entries()],
      partitions: [...this.partitions.entries()].map(([k, p]) => [
        k,
        { byTime: p.byTime, seqs: [...p.seqSet] },
      ]),
      batches: [...this.batches.values()],
      stats: businessStats,
    };
  }

  static fromJSON(data) {
    const s = new EventStore();
    if (!data) return s;
    for (const a of data.arrivals ?? []) s.arrivals.set(a.eventId, a);
    for (const [k, v] of data.seqIndex ?? []) s.seqIndex.set(k, v);
    for (const [k, p] of data.partitions ?? []) {
      s.partitions.set(k, { byTime: p.byTime, seqSet: new Set(p.seqs) });
    }
    for (const b of data.batches ?? []) s.batches.set(b.batchId, b);
    s.stats = { ...s.stats, ...(data.stats ?? {}) };
    return s;
  }
}
