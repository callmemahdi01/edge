import { connect } from "cloudflare:sockets";

const WS_PATH = "/vless-ws";
const PROXY_IP = "proxyip.cmliussss.net";

const RAW_HEX = "d342d11ed4244583b36e524ab1f0afa4";
const UUID_HI = BigInt("0x" + RAW_HEX.slice(0, 16));
const UUID_LO = BigInt("0x" + RAW_HEX.slice(16, 32));

const NGINX_PAGE = `<!DOCTYPE html><html><head><title>Welcome to nginx!</title><style>html{color-scheme:light dark}body{width:35em;margin:0 auto;font-family:Tahoma,Verdana,Arial,sans-serif}</style></head><body><h1>Welcome to nginx!</h1><p>If you see this page, the nginx web server is successfully installed and working.</p></body></html>`;

export default {
  async fetch(req) {
    if (req.headers.get("Upgrade") !== "websocket") {
      const { pathname } = new URL(req.url);
      return new Response(NGINX_PAGE, {
        status: pathname === "/" ? 200 : 404,
        headers: { "Content-Type": "text/html; charset=UTF-8" }
      });
    }

    const url = new URL(req.url);
    if (url.pathname !== WS_PATH) return new Response(null, { status: 404 });

    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    server.binaryType = "arraybuffer";
    server.send(new Uint8Array([0, 0]));

    fastPipe(server, url.searchParams.get("proxyip"));
    return new Response(null, { status: 101, webSocket: client });
  }
};

function fastPipe(ws, proxy) {
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

    const connectDirect = async () => {
      const s = connect({ hostname: host, port });
      await s.opened;
      return s;
    };

    const timeout = new Promise((_, reject) => setTimeout(reject, 1200));

    try {
      tcp = await Promise.race([connectDirect(), timeout]);
    } catch {
      const [pHost, pPort = port] = (proxy || PROXY_IP).split(":");
      try {
        tcp = connect({ hostname: pHost, port: +pPort });
        await tcp.opened;
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