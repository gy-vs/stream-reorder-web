#!/usr/bin/env node
/**
 * server.js — 启动本地 HTTP 服务
 *
 * 用法：
 *   node server.js                     # 内存模式，端口 3000
 *   DATA_DIR=./data node server.js     # 日志+快照持久化到 ./data
 *   PORT=8080 node server.js
 */

import { createApp } from './src/app.js';

const port = Number(process.env.PORT ?? 3000);
const dataDir = process.env.DATA_DIR ?? null;

const app = await createApp({ dataDir });
await new Promise((resolve) => app.server.listen(port, '127.0.0.1', resolve));
console.log(JSON.stringify({
  service: 'reorder-confirm-workbench',
  listening: `http://127.0.0.1:${port}`,
  persistence: dataDir ? `file:${dataDir}` : 'memory',
}));

for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, async () => {
    await app.journal.flush?.();
    app.server.close(() => process.exit(0));
  });
}
