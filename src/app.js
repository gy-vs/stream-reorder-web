/**
 * app.js — 组装内核与 HTTP 服务（测试和 server.js 共用）
 */

import { Kernel } from './kernel.js';
import { FileJournal, MemoryJournal } from './persistence.js';
import { createHttpServer } from './http-server.js';

export async function createApp({
  dataDir = null,
  memory = false,
  clock = null,
  restore = true,
} = {}) {
  let journal = null;
  let kernel;
  if (dataDir) {
    journal = new FileJournal(dataDir);
    await journal.init();
    if (restore) {
      kernel = await Kernel.restore(journal, { clock });
    } else {
      kernel = new Kernel({ clock, journal });
    }
  } else {
    journal = new MemoryJournal();
    kernel = new Kernel({ clock, journal });
  }
  const server = createHttpServer(kernel);
  return {
    kernel,
    journal,
    server,
    listen: (port = 0, host = '127.0.0.1') =>
      new Promise((resolve) => {
        server.listen(port, host, () => resolve(server.address()));
      }),
    close: () =>
      new Promise((resolve) => {
        server.close(() => resolve());
      }),
  };
}
