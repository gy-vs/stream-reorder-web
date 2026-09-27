/**
 * rules.js — 规则注册表（规则是有版本的，处理过程中规则可以变化）
 *
 * 规则语义：
 *  - windowSizeMs：事件时间窗口大小，窗口 = [start, start+windowSizeMs)
 *  - allowedLatenessMs：允许迟到量。
 *      水位 wm(partition) = max(seen eventTime) − allowedLatenessMs
 *      当 wm >= window.end 时，该分区的这个窗口可以确认。
 *  - orderBy：窗口内重排顺序（'partition-seq' | 'event-time'），
 *      改变它会让尚未确认的草稿换一种排列后重算。
 *  - idlePartitionTimeoutMs：某分区到达时间超过该时长无数据，则标记 IDLE，
 *      不再阻塞全局发布边界；分区再次到达自动复活。
 *
 * 已确认窗口永远冻结在“确认那一刻的规则版本”上；新规则只影响未确认草稿。
 */

import { contentHash, assert } from './util.js';

export const DEFAULT_RULE = {
  windowSizeMs: 1000,
  allowedLatenessMs: 500,
  orderBy: 'partition-seq',
  idlePartitionTimeoutMs: 5000,
};

export class RuleRegistry {
  constructor(initialRule = DEFAULT_RULE) {
    this.rules = new Map();
    const v1 = this._build('v1', initialRule, null, 0);
    this.rules.set('v1', v1);
    this.currentVersion = 'v1';
  }

  _build(version, spec, basedOn, createdAt) {
    const rule = {
      version,
      basedOn,
      createdAt,
      windowSizeMs: spec.windowSizeMs,
      allowedLatenessMs: spec.allowedLatenessMs,
      orderBy: spec.orderBy ?? 'partition-seq',
      idlePartitionTimeoutMs: spec.idlePartitionTimeoutMs ?? 5000,
    };
    rule.fingerprint = contentHash({
      windowSizeMs: rule.windowSizeMs,
      allowedLatenessMs: rule.allowedLatenessMs,
      orderBy: rule.orderBy,
      idlePartitionTimeoutMs: rule.idlePartitionTimeoutMs,
    });
    return rule;
  }

  /** 注册新版本（可以基于旧版本）。返回新版本号（由调用方指定或自动生成）。 */
  register(spec, { version = null, now = 0 } = {}) {
    assert(Number.isInteger(spec.windowSizeMs) && spec.windowSizeMs > 0, 'windowSizeMs 必须是正整数');
    assert(
      Number.isInteger(spec.allowedLatenessMs) && spec.allowedLatenessMs >= 0,
      'allowedLatenessMs 必须是非负整数'
    );
    assert(['partition-seq', 'event-time'].includes(spec.orderBy ?? 'partition-seq'),
      "orderBy 只能是 'partition-seq' 或 'event-time'");
    let versionId = version;
    if (!versionId) {
      const num = this.rules.size + 1;
      versionId = `v${num}`;
      while (this.rules.has(versionId)) versionId = `v${versionId.slice(1)}x`;
    }
    assert(!this.rules.has(versionId), `规则版本 ${versionId} 已存在`, 409);
    const rule = this._build(versionId, spec, this.currentVersion, now);
    this.rules.set(versionId, rule);
    this.currentVersion = versionId;
    return rule;
  }

  get(version) {
    const r = this.rules.get(version ?? this.currentVersion);
    assert(r, `未知规则版本: ${version}`, 404);
    return r;
  }

  current() {
    return this.get(this.currentVersion);
  }

  list() {
    return [...this.rules.values()];
  }

  toJSON() {
    return { currentVersion: this.currentVersion, rules: this.list() };
  }

  static fromJSON(data) {
    const reg = new RuleRegistry();
    reg.rules = new Map();
    if (data?.rules) {
      for (const r of data.rules) reg.rules.set(r.version, r);
      reg.currentVersion = data.currentVersion;
    }
    return reg;
  }
}
