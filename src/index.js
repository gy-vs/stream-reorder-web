// index.js —— 模块公开出口：内核、存储、状态常量
export {
  ReorderKernel,
  KernelError,
  DEFAULT_RULE,
  freshState,
  eventId,
  windowStart,
  windowKey,
  RULE_ACTIVE,
  RULE_SUPERSEDED,
  PARTITION_ACTIVE,
  PARTITION_IDLE,
  WINDOW_OPEN,
  WINDOW_ORDERED,
  WINDOW_CONFIRMED,
  EVENT_PENDING,
  EVENT_LATE_TOL,
  EVENT_ORDERED,
  EVENT_CONFIRMED,
  EVENT_LATE_DIAG,
  ORDER_BY_EVENT_TIME_SEQ,
  ORDER_BY_SEQUENCE,
} from './kernel.js';

export { JournalStore, hashContent, stableStringify } from './persistence.js';
