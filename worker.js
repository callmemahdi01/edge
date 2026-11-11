import { connect } from "cloudflare:sockets";

const NGINX_HTML = `<!DOCTYPE html>
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
<p>If you see this page, the nginx web server is successfully installed and working. Further configuration is required.</p>
<p>For online documentation and support please refer to <a href="http://nginx.org/">nginx.org</a>.<br/>
Commercial support is available at <a href="http://nginx.com/">nginx.com</a>.</p>
<p><em>Thank you for using nginx.</em></p>
</body>
</html>`;

function decodeBase64Url(str) {
  try {
    const b64 = str.replace(/-/g, "+").replace(/_/g, "/");
    const pad = b64.padEnd(b64.length + ((4 - (b64.length % 4)) % 4), "=");
    const bin = atob(pad);
    const arr = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) arr[i] = bin.charCodeAt(i);
    return arr;
  } catch {
    return null;
  }
}

function getHeaderLength(b) {
  if (b.length < 18) return -1;
  const m = b[17];
  const optOffset = 18 + m;
  if (b.length < optOffset + 4) return -1;
  const addrType = b[optOffset + 3];
  let addrLen = 0;
  if (addrType === 1) addrLen = 4;
  else if (addrType === 2) {
    if (b.length < optOffset + 5) return -1;
    addrLen = 1 + b[optOffset + 4];
  } else if (addrType === 3) addrLen = 16;
  else return -2;
  const total = optOffset + 4 + addrLen;
  return b.length >= total ? total : -1;
}

async function handleSession(server, earlyData, env) {
  let socket = null;
  try {
    const wsStream = new ReadableStream({
      start(controller) {
        server.addEventListener("message", (e) => {
          controller.enqueue(e.data);
        });
        server.addEventListener("close", () => {
          try { controller.close(); } catch {}
        });
        server.addEventListener("error", (err) => {
          try { controller.error(err); } catch {}
        });
        if (earlyData) {
          controller.enqueue(earlyData);
        }
      },
      cancel() {
        try { server.close(); } catch {}
      }
    });

    const reader = wsStream.getReader();
    let buf = new Uint8Array(0);
    let headerLen = -1;

    while (headerLen < 0) {
      const { value, done } = await reader.read();
      if (done) {
        server.close(1002);
        return;
      }
      const chunk = value instanceof Uint8Array ? value : new Uint8Array(value);
      if (buf.length === 0) {
        buf = chunk;
      } else {
        const temp = new Uint8Array(buf.length + chunk.length);
        temp.set(buf, 0);
        temp.set(chunk, buf.length);
        buf = temp;
      }
      headerLen = getHeaderLength(buf);
      if (headerLen === -2) {
        server.close(1003);
        return;
      }
    }

    const expectedUuid = (env.UUID || "").replace(/-/g, "").toLowerCase();
    let uuidHex = "";
    for (let i = 1; i <= 16; i++) {
      uuidHex += (buf[i] < 16 ? "0" : "") + buf[i].toString(16);
    }
    if (!expectedUuid || uuidHex !== expectedUuid) {
      server.close(1008);
      return;
    }

    const optOffset = 18 + buf[17];
    const cmd = buf[optOffset];
    if (cmd !== 1) {
      server.close(1003);
      return;
    }

    const port = (buf[optOffset + 1] << 8) | buf[optOffset + 2];
    const addrType = buf[optOffset + 3];
    let address = "";

    if (addrType === 1) {
      const o = optOffset + 4;
      address = `${buf[o]}.${buf[o + 1]}.${buf[o + 2]}.${buf[o + 3]}`;
    } else if (addrType === 2) {
      const l = buf[optOffset + 4];
      const o = optOffset + 5;
      address = new TextDecoder().decode(buf.subarray(o, o + l));
    } else if (addrType === 3) {
      const o = optOffset + 4;
      const parts = [];
      for (let i = 0; i < 8; i++) {
        const idx = o + i * 2;
        parts.push(((buf[idx] << 8) | buf[idx + 1]).toString(16));
      }
      address = parts.join(":");
    }

    const initialPayload = buf.subarray(headerLen);

    try {
      socket = connect({ hostname: address, port });
      await socket.opened;
    } catch {
      server.close(1011);
      return;
    }

    const writer = socket.writable.getWriter();
    if (initialPayload.length > 0) {
      await writer.write(initialPayload);
    }

    const upstream = async () => {
      try {
        while (true) {
          const { value, done } = await reader.read();
          if (done) break;
          await writer.write(value instanceof Uint8Array ? value : new Uint8Array(value));
        }
      } catch {}
      try { await writer.close(); } catch {}
    };

    const downstream = async () => {
      let hasSentHeader = false;
      try {
        await socket.readable.pipeTo(
          new WritableStream({
            write(chunk) {
              if (!hasSentHeader) {
                hasSentHeader = true;
                const resp = new Uint8Array(2 + chunk.length);
                resp.set(chunk, 2);
                server.send(resp);
              } else {
                server.send(chunk);
              }
            },
          })
        );
        if (!hasSentHeader) {
          server.send(new Uint8Array([0, 0]));
        }
      } catch {}
      try { server.close(); } catch {}
    };

    await Promise.all([upstream(), downstream()]);
  } finally {
    try { socket?.close(); } catch {}
    try { server.close(); } catch {}
  }
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === "/") {
      return new Response(NGINX_HTML, {
        headers: { "Content-Type": "text/html; charset=utf-8" },
      });
    }

    if (url.pathname !== "/ws" || request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response(null, { status: 404 });
    }

    const [client, server] = Object.values(new WebSocketPair());
    server.accept();

    const protocol = request.headers.get("Sec-WebSocket-Protocol");
    const earlyData = protocol ? decodeBase64Url(protocol) : null;
    const responseHeaders = protocol ? { "Sec-WebSocket-Protocol": protocol } : undefined;

    if (ctx?.waitUntil) {
      ctx.waitUntil(handleSession(server, earlyData, env));
    } else {
      handleSession(server, earlyData, env);
    }

    return new Response(null, {
      status: 101,
      webSocket: client,
      headers: responseHeaders,
    });
  },
};
