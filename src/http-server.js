/**
 * http-server.js — 本地 HTTP 接口
 *
 * 用 Node 原生 http，无第三方依赖。所有写操作最终都走进 Kernel 的串行命令队列，
 * 因此“新导入和旧查询式重算并发”时不会互相覆盖；期望规则版本不匹配由内核
 * 返回 409 STALE_RULE，旧的重排选择无法落到新规则上。
 *
 * 所有响应统一信封 { data | error }。事件相关的输出永远把
 * eventTime 与 arrivalTime 平级返回。
 */

import http from 'node:http';
import { URL } from 'node:url';

export class HttpError extends Error {
  constructor(statusCode, code, message, extra = {}) {
    super(message);
    this.statusCode = statusCode;
    this.code = code;
    this.extra = extra;
  }
}

export function createHttpServer(kernel) {
  const server = http.createServer((req, res) => {
    handle(req, res, kernel).catch((err) => {
      const status = err.statusCode ?? 500;
      send(res, status, {
        error: {
          code: err.code || 'INTERNAL',
          message: err.message,
          ...(err.extra || {}),
        },
      });
    });
  });
  return server;
}

async function readJson(req) {
  if (req.method === 'GET') return {};
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  const body = Buffer.concat(chunks).toString('utf8');
  if (!body) return {};
  try {
    return JSON.parse(body);
  } catch {
    throw new HttpError(400, 'BAD_JSON', '请求体不是合法 JSON');
  }
}

function send(res, status, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(body),
  });
  res.end(body);
}

function intParam(urlSearchParams, name, { required = false } = {}) {
  const raw = urlSearchParams.get(name);
  if (raw === null) {
    if (required) throw new HttpError(400, 'BAD_PARAM', `缺少参数 ${name}`);
    return undefined;
  }
  const v = Number(raw);
  if (!Number.isInteger(v)) throw new HttpError(400, 'BAD_PARAM', `${name} 必须是整数毫秒`);
  return v;
}

async function handle(req, res, kernel) {
  const url = new URL(req.url, 'http://localhost');
  const p = url.pathname;
  const q = url.searchParams;
  const body = await readJson(req);

  // ---- 写路径（串行命令队列） ----------------------------------------------
  if (req.method === 'POST' && p === '/events/import') {
    const events = Array.isArray(body.events) ? body.events : [body.event ?? null].filter(Boolean);
    if (!events.length) throw new HttpError(400, 'EMPTY_BATCH', 'events 至少包含一个事件');
    const out = await kernel.importEvents(events, {
      batchId: body.batchId ?? null,
      expectedRuleVersion: body.expectedRuleVersion ?? undefined,
    });
    return send(res, 200, { data: out });
  }

  if (req.method === 'POST' && p === '/rules') {
    const out = await kernel.registerRule(
      {
        windowSizeMs: body.windowSizeMs,
        allowedLatenessMs: body.allowedLatenessMs,
        orderBy: body.orderBy,
        idlePartitionTimeoutMs: body.idlePartitionTimeoutMs,
      },
      { version: body.version ?? null, expectedRuleVersion: body.expectedRuleVersion }
    );
    return send(res, 200, { data: out });
  }

  if (req.method === 'POST' && p === '/recompute') {
    const out = await kernel.recompute({
      partition: body.partition,
      windowStart: body.windowStart ?? null,
      expectedRuleVersion: body.expectedRuleVersion,
      reason: body.reason ?? 'http',
    });
    return send(res, 200, { data: out });
  }

  if (req.method === 'POST' && p === '/tick') {
    const out = await kernel.tick(body.now ?? intParam(q, 'now') ?? null);
    return send(res, 200, { data: out });
  }

  if (req.method === 'POST' && p === '/checkpoint') {
    const out = await kernel.checkpoint();
    return send(res, 200, { data: out ?? { note: '未配置持久化日志' } });
  }

  // ---- 读路径（一致快照，不经过命令队列） ------------------------------------
  if (req.method === 'GET' && p === '/timeline') {
    return send(
      res,
      200,
      {
        data: kernel.timeline({
          partition: q.get('partition'),
          basis: q.get('basis') ?? 'event',
          from: intParam(q, 'from'),
          to: intParam(q, 'to'),
          limit: intParam(q, 'limit') ?? 500,
        }),
      }
    );
  }

  if (req.method === 'GET' && p === '/pending') {
    return send(res, 200, { data: kernel.pending({ partition: q.get('partition') }) });
  }

  if (req.method === 'GET' && p === '/results') {
    return send(res, 200, { data: kernel.results({ partition: q.get('partition') }) });
  }

  if (req.method === 'GET' && p === '/boundaries') {
    return send(res, 200, { data: { boundaries: kernel.boundaries() } });
  }

  const boundaryMatch = p.match(/^\/boundaries\/([^/]+)$/);
  if (req.method === 'GET' && boundaryMatch) {
    return send(res, 200, { data: kernel.boundary(decodeURIComponent(boundaryMatch[1])) });
  }

  const eventMatch = p.match(/^\/events\/([^/]+)$/);
  if (req.method === 'GET' && eventMatch) {
    return send(res, 200, { data: kernel.event(decodeURIComponent(eventMatch[1])) });
  }

  if (req.method === 'GET' && p === '/partitions') {
    return send(res, 200, { data: { partitions: kernel.partitions() } });
  }

  if (req.method === 'GET' && p === '/rules') {
    return send(res, 200, { data: kernel.rules() });
  }

  if (req.method === 'GET' && p === '/diagnostics') {
    return send(
      res,
      200,
      { data: { diagnostics: kernel.diagnostics({ partition: q.get('partition'), kind: q.get('kind') }) } }
    );
  }

  if (req.method === 'GET' && p === '/journal') {
    return send(
      res,
      200,
      {
        data: kernel.journalView({
          since: intParam(q, 'since') ?? 0,
          limit: intParam(q, 'limit') ?? 1000,
        }),
      }
    );
  }

  if (req.method === 'GET' && p === '/debug/replay') {
    return send(res, 200, { data: kernel.verifyReplay() });
  }

  if (req.method === 'GET' && p === '/debug/stats') {
    return send(res, 200, { data: kernel.stats() });
  }

  if (req.method === 'GET' && p === '/health') {
    return send(res, 200, { data: { ok: true } });
  }

  throw new HttpError(404, 'NOT_FOUND', `没有该接口: ${req.method} ${p}`);
}
