import { connect } from "cloudflare:sockets";

const WS_PATH = "/vless-ws";
const PROXY_IP = "proxyip.cmliussss.net";

const UUID_HI = 0xd342d11ed4244583n;
const UUID_LO = 0xb36e524ab1f0afa4n;

const NGINX_PAGE = `<!DOCTYPE html>
<html>
<head>
<title>Welcome to nginx!</title>
<style>
html { color-scheme: light dark; }
body { width: 35em; margin: 0 auto;
font-family: Tahoma, Verdana, Arial, sans-serif; }
</style>
</head>
<body>
<h1>Welcome to nginx!</h1>
<p>If you see this page, the nginx web server is successfully installed and
working. Further configuration is required.</p>

<p>For online documentation and support please refer to
<a href="http://nginx.org/">nginx.org</a>.<br/>
Commercial support is available at
<a href="http://nginx.com/">nginx.com</a>.</p>

<p><em>Thank you for using nginx.</em></p>
</body>
</html>\r\n`;

const NGINX_404 = `<html>
<head><title>404 Not Found</title></head>
<body>
<center><h1>404 Not Found</h1></center>
<hr><center>nginx</center>
</body>
</html>\r\n`;

export default {
  async fetch(req) {
    const url = new URL(req.url);
    const isWs = req.headers.get("Upgrade") === "websocket";

    if (isWs && url.pathname === WS_PATH) {
      const [client, server] = Object.values(new WebSocketPair());
      server.accept();
      server.binaryType = "arraybuffer";
      server.send(new Uint8Array([0, 0]));

      handleTraffic(server, url.searchParams.get("proxyip"));
      return new Response(null, { status: 101, webSocket: client });
    }

    if (url.pathname === "/") {
      return new Response(NGINX_PAGE, {
        status: 200,
        headers: {
          "Content-Type": "text/html",
          "Server": "nginx"
        }
      });
    }

    return new Response(NGINX_404, {
      status: 404,
      headers: {
        "Content-Type": "text/html",
        "Server": "nginx"
      }
    });
  }
};

function handleTraffic(ws, customProxy) {
  let tcp, writer;

  ws.addEventListener("message", async ({ data }) => {
    if (writer) {
      await writer.write(data);
      return;
    }

    if (data.byteLength < 18) return ws.close();

    const view = new DataView(data);
    if (view.getBigUint64(1) !== UUID_HI || view.getBigUint64(9) !== UUID_LO) {
      return ws.close();
    }

    const u8 = new Uint8Array(data);
    const pIdx = 19 + u8[17];
    const port = view.getUint16(pIdx);
    const type = u8[pIdx + 2];
    let offset = pIdx + 3;
    let host = "";

    if (type === 1) {
      host = `${u8[offset++]}.${u8[offset++]}.${u8[offset++]}.${u8[offset++]}`;
    } else if (type === 2) {
      const len = u8[offset++];
      host = String.fromCharCode(...u8.subarray(offset, (offset += len)));
    } else if (type === 3) {
      const parts = [];
      for (let i = 0; i < 8; i++, offset += 2) parts.push(view.getUint16(offset).toString(16));
      host = parts.join(":");
    } else {
      return ws.close();
    }

    const dial = async (h, p) => {
      const s = connect({ hostname: h, port: p });
      await s.opened;
      return s;
    };

    try {
      tcp = await Promise.race([
        dial(host, port),
        new Promise((_, r) => setTimeout(r, 1000))
      ]);
    } catch {
      const [pHost, pPort = port] = (customProxy || PROXY_IP).split(":");
      try {
        tcp = await dial(pHost, +pPort);
      } catch {
        return ws.close();
      }
    }

    writer = tcp.writable.getWriter();
    if (offset < u8.byteLength) await writer.write(u8.subarray(offset));

    tcp.readable.pipeTo(new WritableStream({
      write(chunk) {
        if (ws.readyState === 1) ws.send(chunk);
      },
      close() { ws.close(); },
      abort() { ws.close(); }
    })).catch(() => ws.close());
  });

  ws.addEventListener("close", () => tcp?.close());
  ws.addEventListener("error", () => tcp?.close());
}