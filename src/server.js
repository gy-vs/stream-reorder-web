// server.js —— 本地 HTTP 接口：把内核、存储和回放查询真正连起来
//
// 启动：node src/server.js --data ./data --port 3000
// 所有 GET 查询只读已提交快照（asOfOffset 标明数据版本）；
// 所有 POST 写操作进入内核串行队列，旧客户端携带 expectedRuleVersion 做乐观并发控制。
import { createServer } from 'node:http';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ReorderKernel, KernelError } from './kernel.js';
import { JournalStore } from './persistence.js';

export async function createApp(options = {}) {
  const dataDir =
    options.dataDir ||
    mkdtempSync(join(tmpdir(), 'reorder-workbench-'));
  const store = new JournalStore(dataDir, { autoSnapshotEvery: options.autoSnapshotEvery ?? 100 });
  const kernel = new ReorderKernel(store, {
    clock: options.clock,
    rule: options.rule,
  });
  await kernel.initialize();

  const server = createServer((req, res) => {
    handle(req, res, kernel).catch((err) => {
      if (err instanceof KernelError) {
        sendJson(res, err.status, { error: { code: err.code, message: err.message, details: err.details } });
      } else {
        sendJson(res, 500, { error: { code: 'INTERNAL', message: err.message } });
      }
    });
  });

  return {
    server,
    kernel,
    store,
    dataDir,
    close: () =>
      new Promise((resolve) => {
        server.close(() => {
          kernel.close();
          if (options.cleanup !== false && !options.dataDir) rmSync(dataDir, { recursive: true, force: true });
          resolve();
        });
      }),
  };
}

async function handle(req, res, kernel) {
  const url = new URL(req.url, 'http://localhost');
  const path = url.pathname;
  const method = req.method;
  const q = url.searchParams;
  const partition = q.get('partition') || undefined;

  if (method === 'GET' && path === '/health') {
    return sendJson(res, 200, { ok: true, asOfOffset: kernel.offset, systemTime: kernel.state.systemTime });
  }

  if (method === 'GET' && path === '/api/results') {
    return sendJson(res, 200, kernel.resultView({ partition }));
  }

  if (method === 'GET' && path === '/api/pending') {
    return sendJson(res, 200, kernel.pendingView({ partition }));
  }

  if (method === 'GET' && path === '/api/timeline') {
    return sendJson(res, 200,
      kernel.timelineView({
        fromOffset: int(q, 'fromOffset', 0),
        limit: int(q, 'limit', 100),
        partition,
      }));
  }

  if (method === 'GET' && path === '/api/replay') {
    return sendJson(res, 200,
      kernel.replayView({
        fromOffset: int(q, 'fromOffset', 0),
        toOffset: q.has('toOffset') ? int(q, 'toOffset', 0) : undefined,
        limit: int(q, 'limit', 200),
      }));
  }

  if (method === 'GET' && path === '/api/anomalies') {
    return sendJson(res, 200, kernel.anomaliesView({ kind: q.get('kind') || undefined, partition }));
  }

  if (method === 'GET' && path === '/api/partitions') {
    return sendJson(res, 200, kernel.resultView({ partition }));
  }

  if (method === 'POST' && path === '/api/events') {
    const body = await readJson(req);
    const arrivalTime = body.arrivalTime !== undefined ? Number(body.arrivalTime) : undefined;
    const events = body.events || (body.event ? [body.event] : null);
    const result = await kernel.ingestBatch(events, { arrivalTime });
    return sendJson(res, 201, result);
  }

  if (method === 'POST' && path === '/api/tick') {
    const body = await readJson(req).catch(() => ({}));
    const result = await kernel.tick(body.arrivalTime !== undefined ? Number(body.arrivalTime) : undefined);
    return sendJson(res, 200, result);
  }

  if (method === 'POST' && path === '/api/heartbeat') {
    const body = await readJson(req);
    if (!body.partition) throw new KernelError(400, 'BAD_REQUEST', 'heartbeat 需要 partition');
    const result = await kernel.heartbeat(body.partition, body.arrivalTime !== undefined ? Number(body.arrivalTime) : undefined);
    return sendJson(res, 200, result);
  }

  if (method === 'GET' && path === '/api/rules') {
    return sendJson(res, 200, {
      current: kernel.currentRule,
      versions: kernel.state.rules,
      asOfOffset: kernel.offset,
    });
  }

  if (method === 'POST' && path === '/api/rules') {
    const body = await readJson(req);
    const result = await kernel.activateRule(body, {
      expectedRuleVersion: body.expectedRuleVersion,
      reason: body.reason || 'HTTP',
    });
    return sendJson(res, 200, result);
  }

  const recomputeMatch = path.match(/^\/api\/windows\/(.+)\/recompute$/);
  if (method === 'POST' && recomputeMatch) {
    const body = await readJson(req).catch(() => ({}));
    const result = await kernel.recomputeWindow(decodeURIComponent(recomputeMatch[1]), {
      expectedRuleVersion: body.expectedRuleVersion,
    });
    return sendJson(res, 200, result);
  }

  const windowMatch = path.match(/^\/api\/windows\/(.+)$/);
  if (method === 'GET' && windowMatch) {
    const key = decodeURIComponent(windowMatch[1]);
    const win = kernel.getWindow(key);
    if (!win) throw new KernelError(404, 'WINDOW_NOT_FOUND', `窗口不存在: ${key}`);
    return sendJson(res, 200, presentViaKernel(kernel, win, key));
  }

  const eventMatch = path.match(/^\/api\/events\/(.+)$/);
  if (method === 'GET' && eventMatch) {
    return sendJson(res, 200, kernel.eventDetailView(decodeURIComponent(eventMatch[1])));
  }

  const partitionMatch = path.match(/^\/api\/partitions\/([^/]+)$/);
  if (method === 'GET' && partitionMatch) {
    return sendJson(res, 200, kernel.partitionView(decodeURIComponent(partitionMatch[1])));
  }

  if (method === 'POST' && path === '/api/snapshot') {
    return sendJson(res, 200, await kernel.snapshot());
  }

  sendJson(res, 404, { error: { code: 'NOT_FOUND', message: `没有这个接口: ${method} ${path}` } });
}

// 窗口详情直接走内核投影（通过 resultView 中的同一窗口，保证字段一致）
function presentViaKernel(kernel, win, key) {
  const view = kernel.resultView();
  return view.windows.find((w) => w.windowKey === key);
}

function sendJson(res, status, payload) {
  const body = JSON.stringify(payload, null, 2);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
  });
  res.end(body);
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (chunk) => {
      data += chunk;
      if (data.length > 4 * 1024 * 1024) {
        reject(new KernelError(413, 'BODY_TOO_LARGE', '请求体过大'));
        req.destroy();
      }
    });
    req.on('end', () => {
      if (!data) return resolve({});
      try {
        resolve(JSON.parse(data));
      } catch {
        reject(new KernelError(400, 'BAD_JSON', '请求体不是合法 JSON'));
      }
    });
    req.on('error', reject);
  });
}

function int(q, name, fallback) {
  if (!q.has(name)) return fallback;
  const n = Number(q.get(name));
  return Number.isInteger(n) ? n : fallback;
}

// 直接启动：node src/server.js --port 3000 --data ./data
const invokedDirectly = process.argv[1] && process.argv[1].endsWith('server.js');
if (invokedDirectly) {
  const args = process.argv.slice(2);
  const getArg = (flag, fallback) => {
    const i = args.indexOf(flag);
    return i >= 0 ? args[i + 1] : fallback;
  };
  const port = Number(getArg('--port', process.env.PORT || 3000));
  const dataDir = getArg('--data', join(process.cwd(), 'data'));
  createApp({ dataDir }).then(({ server }) => {
    server.listen(port, () => {
      console.log(`[reorder-workbench] http://localhost:${port}  data=${dataDir}`);
    });
  });
}
