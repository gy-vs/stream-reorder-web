// query-path.test.js —— 验证服务端不会为一次确认/查询从头扫描全部事件
import { test, describe, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { makeKernel, ev, cleanup } from './helpers.js';

let dirs = [];
afterEach(() => dirs.forEach(cleanup));

describe('查询路径不全量扫描', () => {
  test('确认/重算只遍历相关分区窗口与其引用事件，不触碰其他分区事件', async () => {
    const h = await makeKernel();
    dirs.push(h.dir);
    const k = h.kernel;

    // 背景噪音：其他分区大量已确认历史事件
    const noise = [];
    for (let i = 0; i < 500; i++) {
      noise.push(ev('noise', i, i * 10, i * 10));
    }
    await k.ingestBatch(noise);
    await k.ingestOne(ev('noise', 999, 200000, 200000)); // 推动 noise 分区大量确认

    // 监控确认路径对事件 Map 的访问：只应访问窗口引用的少量事件，与 500 条噪音无关
    let getCount = 0;
    const origEvents = k.state.events;
    class CountingMap extends Map {
      get(key) {
        getCount += 1;
        return super.get(key);
      }
    }
    const counting = new CountingMap();
    for (const [kk, vv] of origEvents) counting.set(kk, vv);
    k.state.events = counting;

    // 目标分区两个事件，确认时事件 get 次数应为小规模常数级，与 2000 条噪音无关
    await k.ingestOne(ev('hot', 1, 100));
    const before = getCount;
    await k.ingestOne(ev('hot', 2, 4000));
    const used = getCount - before;

    assert.equal(k.getWindow('hot@0').status, 'CONFIRMED');
    // 确认 hot@0 只访问该窗口内极少量事件 + hot 分区事件，数量远小于噪音量
    assert.ok(used < 50, `确认路径访问事件 ${used} 次，疑似全量扫描了 500+ 条噪音`);

    // 查询分区视图只包含该分区
    const view = k.partitionView('hot');
    assert.ok(view.events.every((e) => e.partition === 'hot'));
    assert.equal(view.windows.length, k.getPartition('hot').windowKeys.length);
  });

  test('回放/时间线按偏移窗口读取，不重放全部记录', async () => {
    const h = await makeKernel();
    dirs.push(h.dir);
    const k = h.kernel;
    const batch = [];
    for (let i = 0; i < 100; i++) batch.push(ev('p', i, i * 10, i * 10));
    await k.ingestBatch(batch);

    const all = k.timelineView({ limit: 1000 });
    const total = all.entries.length;
    const recent = k.timelineView({ fromOffset: total - 5, limit: 10 });
    assert.equal(recent.entries.length, 5);
    assert.equal(recent.entries[0].offset, total - 5);

    const replay = k.replayView({ fromOffset: 2, toOffset: 4 });
    assert.deepEqual(replay.entries.map((e) => e._offset), [2, 3, 4]);
  });
});
