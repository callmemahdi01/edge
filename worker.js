import { connect } from "cloudflare:sockets";

const WS_PATH = "/home/go.html";
const PROXY_IP = "proxyip.cmliussss.net";
const CONNECT_TIMEOUT = 7000;
const FALLBACK_TIMEOUT = 10000;
const WS_BUFFER_LIMIT = 1048576; // 1 MB backpressure high watermark
const WS_BUFFER_RESUME = 262144; // 256 KB resume watermark

const U0 = 0xd342d11e;
const U1 = 0xd4244583;
const U2 = 0xb36e524a;
const U3 = 0xb1f0afa4;

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

export default {
  fetch(req) {
    const url = new URL(req.url);

    if (url.pathname === WS_PATH && req.headers.get("Upgrade")?.toLowerCase() === "websocket") {
      const protocol = req.headers.get("Sec-WebSocket-Protocol");
      const { 0: client, 1: server } = new WebSocketPair();
      server.accept();
      server.binaryType = "arraybuffer";
      server.send(RESPONSE_HEADER);
      handleTraffic(server, url.searchParams.get("proxyip"), protocol ? decodeEarly(protocol) : null);
      return new Response(null, {
        status: 101,
        webSocket: client,
        headers: protocol ? { "Sec-WebSocket-Protocol": protocol } : undefined
      });
    }

    return url.pathname === "/"
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
    return out.buffer;
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
  let writer = null;
  let started = !!early;
  const queue = [];

  const close = () => {
    try {
      if (ws.readyState === 1 || ws.readyState === 0) ws.close();
    } catch {}
    if (tcp) {
      tcp.close().catch(() => {});
      tcp = null;
    }
  };

  const start = async (data) => {
    let host;
    let port;
    let offset;

    try {
      const buffer = data instanceof ArrayBuffer ? data : data.buffer;
      const byteOffset = data.byteOffset || 0;
      const view = new DataView(buffer, byteOffset, data.byteLength);

      if (
        view.getUint32(1) !== U0 ||
        view.getUint32(5) !== U1 ||
        view.getUint32(9) !== U2 ||
        view.getUint32(13) !== U3
      ) {
        return close();
      }

      const pIdx = 19 + view.getUint8(17);
      port = view.getUint16(pIdx);
      const type = view.getUint8(pIdx + 2);
      offset = pIdx + 3;

      if (type === 1) {
        host = `${view.getUint8(offset)}.${view.getUint8(offset + 1)}.${view.getUint8(offset + 2)}.${view.getUint8(offset + 3)}`;
        offset += 4;
      } else if (type === 2) {
        const len = view.getUint8(offset++);
        host = decoder.decode(new Uint8Array(buffer, byteOffset + offset, len));
        offset += len;
      } else if (type === 3) {
        const parts = new Array(8);
        for (let i = 0; i < 8; i++, offset += 2) parts[i] = view.getUint16(offset).toString(16);
        host = parts.join(":");
      } else {
        return close();
      }
    } catch {
      return close();
    }

    try {
      tcp = await dial(host, port, CONNECT_TIMEOUT);
    } catch {
      const proxyTarget = customProxy || PROXY_IP;
      const [proxyHost, proxyPort] = proxyTarget.split(":");
      try {
        tcp = await dial(proxyHost, proxyPort ? +proxyPort : port, FALLBACK_TIMEOUT);
      } catch {
        return close();
      }
    }

    if (ws.readyState !== 1) return close();

    writer = tcp.writable.getWriter();

    if (offset < data.byteLength) {
      const initialChunk = new Uint8Array(
        data instanceof ArrayBuffer ? data : data.buffer,
        (data.byteOffset || 0) + offset,
        data.byteLength - offset
      );
      writer.write(initialChunk).catch(close);
    }

    for (const chunk of queue) {
      writer.write(chunk).catch(close);
    }
    queue.length = 0;

    // Outbound pipe from TCP upstream to Client WebSocket with flow-control backpressure
    (async () => {
      const reader = tcp.readable.getReader();
      try {
        while (ws.readyState === 1) {
          const { done, value } = await reader.read();
          if (done) break;
          ws.send(value);

          // Flow control: throttle TCP read if client WebSocket buffer is saturated
          if (typeof ws.bufferedAmount === "number" && ws.bufferedAmount > WS_BUFFER_LIMIT) {
            while (ws.bufferedAmount > WS_BUFFER_RESUME && ws.readyState === 1) {
              await new Promise((r) => setTimeout(r, 15));
            }
          }
        }
      } catch {}
      close();
    })();
  };

  ws.addEventListener("message", ({ data }) => {
    if (typeof data === "string") return;
    const chunk = data instanceof ArrayBuffer ? new Uint8Array(data) : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);

    if (writer) {
      writer.write(chunk).catch(close);
    } else if (started) {
      queue.push(chunk);
    } else {
      started = true;
      start(chunk).catch(close);
    }
  });

  ws.addEventListener("close", close);
  ws.addEventListener("error", close);

  if (early) start(new Uint8Array(early)).catch(close);
}
