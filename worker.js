import { connect } from "cloudflare:sockets";

const WS_PATH = "/home/go.html";
const PROXY_IP = "proxyip.cmliussss.net";
const CONNECT_TIMEOUT = 7000;
const FALLBACK_TIMEOUT = 10000;
const WINDOW = 4194304; // or const WINDOW = 2097152;
const BUDGET = 83886080;
const QUEUE_LIMIT = 4194304;
const FLUSH_SIZE = 65536;
const FLUSH_MS = 8;
const budgetWaiters = new Set();
let total = 0;

const wakeBudget = () => {
  if (total > BUDGET || budgetWaiters.size === 0) return;
  for (const w of budgetWaiters) w();
  budgetWaiters.clear();
};

const UUID_BYTES = Uint8Array.of(
  0xd3, 0x42, 0xd1, 0x1e,
  0xd4, 0x24, 0x45, 0x83,
  0xb3, 0x6e, 0x52, 0x4a,
  0xb1, 0xf0, 0xaf, 0xa4
);

const RESPONSE_HEADER = new Uint8Array(2);
const HTML_HEADERS = { "Content-Type": "text/html; charset=utf-8", Server: "nginx" };
const decoder = new TextDecoder();

const NGINX_PAGE = `<!DOCTYPE html>
<html>
<head>
<title>Welcome to nginx!</title>
<style>
html { color-scheme: light dark; }
body { width: 35em; margin: 0 auto; font-family: Tahoma, Verdana, Arial, sans-serif; }
</style>
</head>
<body>
<h1>Welcome to nginx!</h1>
<p>If you see this page, the nginx web server is successfully installed and working.</p>
</body>
</html>\r\n`;

function parseUrlParts(raw) {
  const schemeEnd = raw.indexOf("://");
  const hostStart = schemeEnd >= 0 ? schemeEnd + 3 : 0;
  const q = raw.indexOf("?", hostStart);
  const slash = raw.indexOf("/", hostStart);
  const pathStart = slash >= 0 && (q < 0 || slash < q) ? slash : -1;
  const pathname = pathStart >= 0 ? raw.slice(pathStart, q >= 0 ? q : raw.length) : "/";
  return { pathname, query: q >= 0 ? raw.slice(q + 1) : "" };
}

function getQueryParam(query, name) {
  if (!query) return null;
  const prefix = `${name}=`;
  let start = 0;
  while (start <= query.length) {
    const amp = query.indexOf("&", start);
    const end = amp >= 0 ? amp : query.length;
    if (query.startsWith(prefix, start)) {
      const val = query.slice(start + prefix.length, end);
      try {
        return decodeURIComponent(val);
      } catch {
        return val;
      }
    }
    if (amp < 0) break;
    start = amp + 1;
  }
  return null;
}

function parseHostPort(target, fallbackPort) {
  if (!target) return { hostname: "", port: fallbackPort };
  if (target[0] === "[") {
    const closeIdx = target.indexOf("]");
    if (closeIdx > 1) {
      const hostname = target.slice(1, closeIdx);
      const colonIdx = target.indexOf(":", closeIdx + 1);
      const parsedPort = colonIdx > closeIdx ? +target.slice(colonIdx + 1) : 0;
      return { hostname, port: parsedPort > 0 ? parsedPort : fallbackPort };
    }
  }
  const colonIdx = target.indexOf(":");
  if (colonIdx > 0 && target.indexOf(":", colonIdx + 1) < 0) {
    const parsedPort = +target.slice(colonIdx + 1);
    return {
      hostname: target.slice(0, colonIdx),
      port: parsedPort > 0 ? parsedPort : fallbackPort
    };
  }
  return { hostname: target, port: fallbackPort };
}

const DEFAULT_PROXY = parseHostPort(PROXY_IP, 0);

export default {
  fetch(req) {
    const { pathname, query } = parseUrlParts(req.url);

    if (pathname === WS_PATH && req.headers.get("Upgrade")?.toLowerCase() === "websocket") {
      const protocol = req.headers.get("Sec-WebSocket-Protocol");
      const { 0: client, 1: server } = new WebSocketPair();
      server.accept();
      server.binaryType = "arraybuffer";
      server.send(RESPONSE_HEADER);
      handleTraffic(server, getQueryParam(query, "proxyip"), protocol ? decodeEarly(protocol) : null);
      return new Response(null, {
        status: 101,
        webSocket: client,
        headers: protocol ? { "Sec-WebSocket-Protocol": protocol } : undefined
      });
    }

    return pathname === "/"
      ? new Response(NGINX_PAGE, { headers: HTML_HEADERS })
      : new Response("Not Found", { status: 404, headers: HTML_HEADERS });
  }
};

function decodeEarly(value) {
  try {
    const bin = atob(value.replace(/-/g, "+").replace(/_/g, "/"));
    if (bin.length < 18) return null;
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < out.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  } catch {
    return null;
  }
}

async function dial(hostname, port, ms) {
  const socket = connect({ hostname, port });
  let timer;
  try {
    await Promise.race([
      socket.opened,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error("Connect timeout")), ms);
      })
    ]);
    return socket;
  } catch (error) {
    socket.close().catch(() => {});
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

function handleTraffic(ws, customProxy, early) {
  let tcp = null;
  let reader = null;
  let writer = null;
  let closed = false;
  let started = !!early;
  let sent = 0;
  let acked = 0;
  let credit = false;
  let acct = 0;
  let queued = 0;
  let buf = [];
  let len = 0;
  let timer = 0;
  let wake = null;
  let globalWait = null;
  const queue = [];

  const wakeStream = () => {
    if (wake) {
      const r = wake;
      wake = null;
      r();
    }
  };

  const sync = () => {
    const d = (credit ? Math.max(0, sent - acked) : 0) - acct;
    if (!d) return;
    acct += d;
    total += d;
    if (d < 0) wakeBudget();
  };

  const flush = () => {
    clearTimeout(timer);
    timer = 0;
    const parts = buf;
    const n = len;
    buf = [];
    len = 0;
    if (!n || closed || ws.readyState !== 1) return;
    let out = parts[0];
    if (parts.length > 1) {
      out = new Uint8Array(n);
      let o = 0;
      for (let i = 0; i < parts.length; i++) {
        const c = parts[i];
        out.set(c, o);
        o += c.byteLength;
      }
    }
    try {
      ws.send(out);
    } catch {
      return close();
    }
    sent += n;
    if (credit || acct !== 0) sync();
  };

  const close = () => {
    if (closed) return;
    closed = true;
    clearTimeout(timer);
    timer = 0;
    try {
      if (ws.readyState === 1 || ws.readyState === 0) ws.close();
    } catch {}
    if (reader) {
      try {
        reader.cancel().catch(() => {});
      } catch {}
      reader = null;
    }
    if (tcp) {
      tcp.close().catch(() => {});
      tcp = null;
    }
    writer = null;
    queue.length = 0;
    queued = 0;
    buf = [];
    len = 0;
    credit = false;
    sync();
    wakeStream();
    if (globalWait) {
      budgetWaiters.delete(globalWait);
      const r = globalWait;
      globalWait = null;
      r();
    }
    wakeBudget();
  };

  const start = async (u8) => {
    let host;
    let port;
    let offset;

    try {
      const byteLength = u8.byteLength;
      if (byteLength < 22) return close();
      for (let i = 0; i < 16; i++) {
        if (u8[1 + i] !== UUID_BYTES[i]) return close();
      }

      const pIdx = 19 + u8[17];
      if (pIdx + 3 > byteLength) return close();
      port = (u8[pIdx] << 8) | u8[pIdx + 1];
      const type = u8[pIdx + 2];
      offset = pIdx + 3;

      if (type === 1) {
        if (offset + 4 > byteLength) return close();
        host = `${u8[offset]}.${u8[offset + 1]}.${u8[offset + 2]}.${u8[offset + 3]}`;
        offset += 4;
      } else if (type === 2) {
        if (offset >= byteLength) return close();
        const dLen = u8[offset++];
        if (offset + dLen > byteLength) return close();
        host = decoder.decode(u8.subarray(offset, offset + dLen));
        offset += dLen;
      } else if (type === 3) {
        if (offset + 16 > byteLength) return close();
        const parts = new Array(8);
        for (let i = 0; i < 8; i++, offset += 2) {
          parts[i] = ((u8[offset] << 8) | u8[offset + 1]).toString(16);
        }
        host = parts.join(":");
      } else {
        return close();
      }
    } catch {
      return close();
    }

    let dialed;
    try {
      dialed = await dial(host, port, CONNECT_TIMEOUT);
    } catch {
      if (closed || ws.readyState !== 1) return close();
      const target = customProxy ? parseHostPort(customProxy, port) : DEFAULT_PROXY;
      try {
        dialed = await dial(target.hostname, target.port || port, FALLBACK_TIMEOUT);
      } catch {
        return close();
      }
    }

    if (closed || ws.readyState !== 1) {
      dialed.close().catch(() => {});
      return close();
    }
    tcp = dialed;
    writer = tcp.writable.getWriter();

    if (offset < u8.byteLength) {
      writer.write(u8.subarray(offset)).catch(close);
    }

    for (let i = 0; i < queue.length; i++) {
      if (closed) return;
      writer.write(queue[i]).catch(close);
    }
    queue.length = 0;
    queued = 0;

    (async () => {
      reader = tcp.readable.getReader();
      try {
        while (!closed && ws.readyState === 1) {
          while (!closed && ws.readyState === 1) {
            const streamBlocked = credit && sent + len - acked > WINDOW;
            const globalBlocked = !streamBlocked && credit && total > BUDGET;
            if (!streamBlocked && !globalBlocked) break;
            flush();
            if (closed || ws.readyState !== 1) break;
            await new Promise((r) => {
              if (streamBlocked) {
                wake = r;
              } else {
                const gw = () => {
                  budgetWaiters.delete(gw);
                  if (globalWait === gw) globalWait = null;
                  r();
                };
                globalWait = gw;
                budgetWaiters.add(gw);
              }
            });
          }
          if (closed || ws.readyState !== 1) break;
          const { done, value } = await reader.read();
          if (done) break;
          buf.push(value);
          len += value.byteLength;
          if (len >= FLUSH_SIZE) flush();
          else if (!timer) timer = setTimeout(flush, FLUSH_MS);
        }
        flush();
      } catch {}
      close();
    })();
  };

  ws.addEventListener("message", ({ data }) => {
    if (closed) return;
    try {
      if (typeof data === "string") {
        const n = Number(data);
        if (Number.isFinite(n) && n >= 0) {
          credit = true;
          if (n > acked) acked = n;
          sync();
          if (sent + len - acked <= WINDOW) wakeStream();
        }
        return;
      }
      const chunk =
        data instanceof ArrayBuffer
          ? new Uint8Array(data)
          : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);

      if (writer) {
        writer.write(chunk).catch(close);
      } else if (started) {
        queued += chunk.byteLength;
        if (queued > QUEUE_LIMIT) return close();
        queue.push(chunk);
      } else {
        started = true;
        start(chunk).catch(close);
      }
    } catch {
      close();
    }
  });

  ws.addEventListener("close", close);
  ws.addEventListener("error", close);

  if (early) start(early).catch(close);
}
