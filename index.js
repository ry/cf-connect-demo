// A chat server: one Durable Object per channel, on a self-hosted celld
// fleet. Each channel keeps its history in SQLite and holds its sockets
// with the hibernation API. Every second it beats to the other channels
// with its node and head count, and pushes the fleet map down its sockets
// when it changes. Power off a node and its channels move to the others.

// Private addresses on the fleet's VPC.
const NODES = [
  { name: "node-a", ip: "10.42.0.3" },
  { name: "node-b", ip: "10.42.0.4" },
  { name: "node-c", ip: "10.42.0.5" },
];
const CHANNELS = [
  "connect", "general", "random", "rust", "agents", "js", "infra", "storage",
  "wasm",
];
const NS = "s20"; // bump to start every channel fresh (ops/reset.sh)
const BEAT_MS = 1000;
const TICK_MS = 2000; // keepalive down every socket; clients watch for it
const STALE_MS = 6000; // a channel silent this long is moving
const GIVEUP_MS = 20000; // then any node may claim it, not just its heir
const OBSERVER = "~observer"; // a socket that watches the map, not the chat

export class Channel {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.sql = ctx.storage.sql;
    this.sql.exec(`create table if not exists msgs (
      id integer primary key, nick text, text text, ts integer)`);
    this.peers = new Map(); // channel -> { node, conns, seen }
    this.fails = new Map(); // node ip -> health probes missed in a row
    this.ip = null;
    this.sent = "";
    this.ticked = 0;
    this.whoami();
    ctx.blockConcurrencyWhile(async () => {
      this.name = await ctx.storage.get("name");
      this.ns = await ctx.storage.get("ns");
      this.armed = (await ctx.storage.get("armed")) ?? false;
    });
  }

  // Learn our node's public IP. Never block a request on it.
  whoami() {
    fetch(this.env.WHOAMI_URL, { signal: AbortSignal.timeout(2000) })
      .then((r) => r.text())
      .then((s) => (this.ip = s.trim()))
      .catch(() => setTimeout(() => this.whoami(), 2000));
  }

  async adopt(name) {
    if (this.stale()) return;
    if (this.name !== name) {
      Object.assign(this, { name, ns: NS });
      await this.ctx.storage.put({ name, ns: NS });
    }
    if (this.timer) return;
    this.timer = setInterval(() => this.beat(), BEAT_MS);
    await this.ctx.storage.setAlarm(Date.now() + 30000);
  }

  // The alarm restarts the beat if the channel was evicted.
  async alarm() {
    if (this.stale() || !this.name) return;
    await this.ctx.storage.setAlarm(Date.now() + 30000);
    await this.adopt(this.name);
  }

  // A channel from an older NS wakes with the new code; keep it quiet, or
  // it beats the current channels into existence on its own node.
  stale() {
    if (this.ns === NS || this.ns === undefined) return false;
    clearInterval(this.timer);
    this.ctx.storage.deleteAlarm();
    return true;
  }

  beat() {
    if (this.stale()) return;
    const now = Date.now();
    const body = JSON.stringify({
      c: this.name, node: this.ip, conns: this.conns(),
    });
    for (const c of CHANNELS) {
      const p = this.peers.get(c);
      const age = p ? now - p.seen : Infinity;
      // A beat creates its target on our node if it lives nowhere. Before
      // ops/place.ts arms the fleet, beat only peers we have heard from.
      // After, an orphan is left to its heir so a dead node's channels
      // split across the survivors.
      if (c === this.name || !this.ip || (!this.armed && !p)) continue;
      const orphan = age >= STALE_MS && age < GIVEUP_MS;
      if (orphan && this.heir(c) !== this.ip) continue;
      channel(this.env, c)
        .fetch(`http://fleet/channel/${c}/beat`, {
          method: "POST",
          body,
          // A beat stuck on a dead owner stalls this cell, so beats to
          // live peers time out. An heir's beat must finish to claim.
          signal: age < STALE_MS ? AbortSignal.timeout(1500) : undefined,
        })
        // Read the body: celld 0.6.2 pins a tunnel per unread response.
        .then((r) => r.arrayBuffer())
        .catch(() => {});
    }
    if (now - this.ticked >= TICK_MS) {
      this.ticked = now;
      this.broadcast('{"type":"tick"}');
      this.probe();
    }
    this.push();
  }

  // A node is down after two missed health checks in a row.
  probe() {
    for (const { ip } of NODES) {
      fetch(`http://${ip}:8080/health`, { signal: AbortSignal.timeout(1500) })
        .then((r) => r.arrayBuffer())
        .then(() => this.fails.set(ip, 0))
        .catch(() => this.fails.set(ip, (this.fails.get(ip) ?? 0) + 1));
    }
  }

  down() {
    return NODES.map((n) => n.ip).filter((ip) => this.fails.get(ip) >= 2);
  }

  // Rendezvous hashing: an orphan's heir is the live node scoring highest
  // for it, so every survivor computes the same answer.
  heir(c) {
    const score = (s) => {
      let h = 2166136261;
      for (const ch of s) h = Math.imul(h ^ ch.charCodeAt(0), 16777619);
      return h >>> 0;
    };
    const down = this.down();
    const live = NODES.map((n) => n.ip).filter((ip) => !down.includes(ip));
    return live.reduce((a, b) => (score(c + b) > score(c + a) ? b : a));
  }

  async fetch(req) {
    const url = new URL(req.url);
    const [, , name, verb] = url.pathname.split("/");
    await this.adopt(name);
    if (verb === "beat") {
      const b = await req.json();
      this.peers.set(b.c, { node: b.node, conns: b.conns, seen: Date.now() });
      this.push();
      return new Response(null, { status: 204 });
    }
    if (verb === "arm") {
      this.armed = true;
      await this.ctx.storage.put("armed", true);
      return new Response("armed");
    }
    if (verb === "whereami") {
      return Response.json({ node: this.ip, conns: this.conns() });
    }
    if (!req.headers.get("Upgrade")) {
      return new Response("use a websocket", { status: 426 });
    }
    const observer = url.searchParams.has("observe");
    const nick = observer
      ? OBSERVER
      : (url.searchParams.get("nick") || "guest").slice(0, 24);
    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server, [nick]);
    const history = observer ? [] : this.sql
      .exec("select nick, text from msgs order by id desc limit 50")
      .toArray()
      .reverse();
    server.send(JSON.stringify({
      type: "hello", node: this.ip, nodes: NODES, history,
      fleet: this.view(), down: this.down(),
    }));
    return new Response(null, { status: 101, webSocket: client });
  }

  chatters() {
    return this.ctx.getWebSockets()
      .filter((ws) => this.ctx.getTags(ws)[0] !== OBSERVER);
  }

  conns() {
    return this.chatters().length;
  }

  view() {
    const now = Date.now();
    return CHANNELS.map((c) => {
      if (c === this.name) {
        return { c, node: this.ip, conns: this.conns(), up: !!this.ip };
      }
      const p = this.peers.get(c);
      const up = now - p?.seen < STALE_MS;
      return { c, node: p?.node, conns: p?.conns ?? 0, up };
    });
  }

  push() {
    const fleet = this.view();
    const m = JSON.stringify({ type: "fleet", fleet, down: this.down() });
    if (m !== this.sent) this.broadcast((this.sent = m));
  }

  broadcast(m, to = this.ctx.getWebSockets()) {
    for (const ws of to) {
      try {
        ws.send(m);
      } catch {}
    }
  }

  webSocketMessage(ws, raw) {
    const nick = this.ctx.getTags(ws)[0];
    let text;
    try {
      text = String(JSON.parse(raw).text ?? "").trim().slice(0, 280);
    } catch {}
    if (!text || nick === OBSERVER) return;
    this.sql.exec(
      "insert into msgs (nick, text, ts) values (?, ?, ?)",
      nick, text, Date.now(),
    );
    this.sql.exec(
      "delete from msgs where id <= (select max(id) - 500 from msgs)",
    );
    const m = JSON.stringify({ type: "msg", nick, text });
    this.broadcast(m, this.chatters());
  }

  webSocketClose(ws, code) {
    try {
      ws.close(code);
    } catch {}
  }
}

const channel = (env, c) =>
  env.CHANNEL.get(env.CHANNEL.idFromName(`${NS}:${c}`));

export default {
  fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/health") return new Response(`ok ${NS}`);
    const m = url.pathname.match(/^\/(ws|channel)\/([a-z]+)(\/\w+)?$/);
    if (m && CHANNELS.includes(m[2])) return channel(env, m[2]).fetch(req);
    return env.ASSETS.fetch(req);
  },
};
