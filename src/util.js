/**
 * util.js — 纯函数基础工具
 *
 * 设计原则：
 *  - 所有时间戳均为整数毫秒（Epoch ms），事件时间(eventTime)与到达时间(arrivalTime)
 *    在数据结构里始终是两个独立字段，绝不混用。
 *  - hash 只用于生成确定性的内容指纹（冲突检测、回放幂等标识），不参与安全场景。
 */

/** 64 位 FNV-1a 风格内容指纹，输出十六进制字符串，足够用于内容比对 */
export function contentHash(payload) {
  const str = typeof payload === 'string' ? payload : JSON.stringify(payload);
  // 用 BigInt 模拟 64 位 FNV-1a
  let h = 0xcbf29ce484222325n;
  const PRIME = 0x100000001b3n;
  const MASK = 0xffffffffffffffffn;
  for (let i = 0; i < str.length; i++) {
    h ^= BigInt(str.charCodeAt(i));
    h = (h * PRIME) & MASK;
  }
  return h.toString(16).padStart(16, '0');
}

/** 对事件负载做指纹（不含到达时间，因为同一业务事件重传时到达时间必然不同） */
export function fingerprintEvent(ev) {
  return contentHash({
    eventId: ev.eventId,
    partition: ev.partition,
    seq: ev.seq,
    eventTime: ev.eventTime,
    data: ev.data ?? null,
  });
}

/**
 * 业务内容指纹：用于判断“同 (partition,seq) 但 eventId 不同”的两个事件
 * 是否是同一业务事件的重传。刻意排除 eventId（传输层身份）与 arrivalTime。
 */
export function businessFingerprint(ev) {
  return contentHash({
    partition: ev.partition,
    seq: ev.seq,
    eventTime: ev.eventTime,
    data: ev.data ?? null,
  });
}

/**
 * 有序数组上的二分查找：
 *  返回第一个 >= target 的下标（插入位）。arr 必须按 keyFn 升序。
 */
export function lowerBound(arr, target, keyFn) {
  let lo = 0;
  let hi = arr.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (keyFn(arr[mid]) < target) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

/** 返回第一个 > target 的下标 */
export function upperBound(arr, target, keyFn) {
  let lo = 0;
  let hi = arr.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (keyFn(arr[mid]) <= target) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

/** 稳定的事件排序比较器：同一规则版本下，发布顺序必须可重现 */
export function eventOrderCompare(a, b) {
  if (a.partition !== b.partition) return a.partition < b.partition ? -1 : 1;
  if (a.seq !== b.seq) return a.seq < b.seq ? -1 : 1;
  if (a.eventTime !== b.eventTime) return a.eventTime - b.eventTime;
  if (a.arrivalTime !== b.arrivalTime) return a.arrivalTime - b.arrivalTime;
  return a.eventId < b.eventId ? -1 : a.eventId > b.eventId ? 1 : 0;
}

/** 事件时间落在哪个窗口 [start, end) */
export function windowStartOf(eventTime, windowSizeMs, originMs = 0) {
  return Math.floor((eventTime - originMs) / windowSizeMs) * windowSizeMs + originMs;
}

/** 深拷贝（状态全部是 JSON 可序列化的） */
export function clone(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

/** 生成确定性 ID：前缀 + 内容/键的哈希，保证实时与回放生成相同标识 */
export function deterministicId(parts) {
  return contentHash(parts.map((p) => String(p)).join('|'));
}

export function assert(cond, message, statusCode = 400) {
  if (!cond) {
    const err = new Error(message);
    err.statusCode = statusCode;
    throw err;
  }
}

/** 把时钟封装成可注入对象，测试里用可控时钟，生产环境用系统时钟 */
export function createClock(nowFn) {
  return { now: nowFn ?? (() => Date.now()) };
}
