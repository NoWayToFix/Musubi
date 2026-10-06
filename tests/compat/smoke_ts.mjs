#!/usr/bin/env node
/**
 * 多语言一致性冒烟测试（TypeScript SDK 版）。
 *
 * 与 smoke_py.py、smoke_go/main.go 执行完全相同的 10 步断言序列：
 *  1  engine.info 协商协议版本 / 传输能力
 *  2  engine.ping
 *  3  node.list / node.get
 *  4  command.send 异步接受 + 事件驱动等待终态
 *  5  command.send 幂等键去重
 *  6  command.batch 同步批量（成功 + 失败混合）
 *  7  task.create 异步创建 + 事件驱动等待终态
 *  8  错误码 node.not_found (-32010)
 *  9  订阅释放 (event.unsubscribe)
 * 10  c.close()
 *
 * 事件等待说明：TS SDK 的事件分发按事件类型/Dispatch 给监听器，不会缓存；
 * 因此 waiter 在 RPC 之前就先挂 '*' 监听并把事件收进 inbox，等待时可先查
 * inbox 再挂新监听，杜绝「事件先于等待到达」的竞态。
 *
 * 用法: node tests/compat/smoke_ts.mjs   （需先构建 sdk/ts）
 * 环境: HUB_URL 覆盖默认地址
 */
import { HubClient } from "../../sdk/ts/dist/index.mjs";

const URL = process.env.HUB_URL || "http://127.0.0.1:8080";
const IK = "smoke-ik-ts-" + process.pid;
let passed = 0;

function check(name, cond, detail = "") {
  if (!cond) {
    console.error("FAIL " + name + ": " + detail);
    process.exit(1);
  }
  passed++;
  console.log("PASS", name);
}

// createWaiter pre-registers a '*' listener (inbox) and lets wait() replay
// already-seen events before falling back to a fresh listener.
function createWaiter(c) {
  const inbox = [];
  const off = c.on("*", (ev) => inbox.push(ev));
  return {
    wait(pred, timeoutMs = 15000) {
      const found = inbox.find(pred);
      if (found) return Promise.resolve(found);
      return new Promise((resolve, reject) => {
        const off2 = c.on("*", (ev) => {
          if (pred(ev)) {
            clearTimeout(t);
            off2();
            resolve(ev);
          }
        });
        const t = setTimeout(() => {
          off2();
          reject(new Error("等待事件超时"));
        }, timeoutMs);
      });
    },
    close() {
      off();
    },
  };
}

async function main() {
  const c = new HubClient({ url: URL, requestTimeoutMs: 10000 });
  await c.open();
  const { wait, close: waitClose } = createWaiter(c);

  // 1. 版本协商与能力发现
  const info = await c.info({
    name: "smoke-ts", version: "1.0.0",
    protocol_versions: ["1.0"], encodings: ["json"],
    transports: ["ws", "http"],
  });
  check("engine.info 协商", info.protocol_version === "1.0", info.protocol_version);
  check("传输能力含 ws/http",
    info.transports.includes("ws") && info.transports.includes("http"),
    JSON.stringify(info.transports));

  // 2. engine.ping
  const ping = await c.ping();
  check("engine.ping", ping.pong === true, JSON.stringify(ping));

  // 3. 节点
  const nodes = await c.nodeList();
  check("node.list 返回 3 个节点", nodes.nodes.length === 3, JSON.stringify(nodes));
  const node = await c.nodeGet("n-1");
  check("node.get", node.node.id === "n-1", JSON.stringify(node));
  check("node.get 能力非空", (node.node.capabilities || []).length >= 1, JSON.stringify(node));

  // 4. command.send 异步：订阅先行，等待 command.result
  const sub = await c.eventSubscribe({
    event_types: ["command.result", "command.failed"],
    node_id: "n-1",
  });
  const sent = await c.commandSend({
    node_id: "n-1", method: "echo", params: { text: "hi" }, timeout: 5000,
  });
  const cmdId = sent.command.id;
  const ev = await wait((e) => e.type === "command.result" && e.command_id === cmdId);
  check("command.send 事件终态 succeeded",
    ev.data.status === "succeeded" && ev.data.result && ev.data.result.text === "hi",
    JSON.stringify(ev));

  // 5. 幂等键：同键二次下发复用首次结果
  const first = await c.commandSend({
    node_id: "n-1", method: "echo", params: { text: "dedupe" }, idempotency_key: IK,
  });
  await wait((e) => e.type === "command.result" && e.command_id === first.command.id);
  const dup = await c.commandSend({
    node_id: "n-1", method: "echo", params: { text: "changed" }, idempotency_key: IK,
  });
  check("command.send 幂等去重",
    dup.command.id === first.command.id && dup.command.result && dup.command.result.text === "dedupe",
    JSON.stringify(dup));

  // 6. command.batch 同步返回终态（含失败项，不抛异常）
  const batch = await c.commandBatch([
    { node_id: "n-2", method: "echo", params: { text: "b" } },
    { node_id: "n-2", method: "fail" },
  ]);
  const st = batch.commands.map((cmd) => cmd.status);
  check("command.batch 混合终态", JSON.stringify(st) === JSON.stringify(["succeeded", "failed"]), JSON.stringify(batch));

  // 7. task.create 异步：等待 task.completed
  await c.eventSubscribe({ event_types: ["task.completed", "task.failed"] });
  const task = await c.taskCreate({
    name: "smoke-task-ts",
    commands: [{ node_id: "n-3", method: "echo" }],
  });
  const taskId = task.task.id;
  const tev = await wait((e) =>
    e.task_id === taskId && (e.type === "task.completed" || e.type === "task.failed"));
  check("task.create 终态 succeeded", tev.data.status === "succeeded", JSON.stringify(tev));
  const got = await c.taskGet(taskId);
  check("task.get 与事件一致", got.task.status === "succeeded", JSON.stringify(got));

  // 8. 错误码：未知节点
  try {
    await c.nodeGet("n-999");
    check("node.not_found -32010", false, "未抛错");
  } catch (e) {
    check("node.not_found -32010", e.code === -32010, String(e.message));
  }

  // 9. 订阅释放
  await c.eventUnsubscribe(sub.subscription_id);
  check("event.unsubscribe", true, "");

  // 10. 关闭客户端
  waitClose();
  await c.close();
  check("client.close", true, "");

  console.log("TS SDK SMOKE: ALL PASS (%d)", passed);
}

main().catch((e) => { console.error("FATAL", e); process.exit(1); });