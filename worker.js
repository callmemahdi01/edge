import { connect } from "cloudflare:sockets";

const WS_PATH = "/vless-ws";
const PROXY_IP = "proxyip.cmliussss.net";

const UUID_BYTES = new Uint8Array(
  "d342d11ed4244583b36e524ab1f0afa4".match(/.{2}/g).map((b) => parseInt(b, 16))
);

const NGINX_PAGE = `<!DOCTYPE html><html><head><title>Welcome to nginx!</title><style>html{color-scheme:light dark}body{width:35em;margin:0 auto;font-family:Tahoma,Verdana,Arial,sans-serif}</style></head><body><h1>Welcome to nginx!</h1><p>If you see this page, the nginx web server is successfully installed and working.</p></body></html>`;

const TEXT_DEC = new TextDecoder();

export default {
  async fetch(req) {
    const url = new URL(req.url);
    if (req.headers.get("Upgrade") === "websocket" && url.pathname === WS_PATH) {
      const [client, server] = Object.values(new WebSocketPair());
      server.accept();
      server.binaryType = "arraybuffer";
      server.send(new Uint8Array([0, 0]));
      pipeSession(server, url.searchParams.get("proxyip"));
      return new Response(null, { status: 101, webSocket: client });
    }

    return new Response(NGINX_PAGE, {
      status: url.pathname === "/" ? 200 : 404,
      headers: { "Content-Type": "text/html; charset=UTF-8" }
    });
  }
};

function pipeSession(ws, proxy) {
  let tcp, writer;

  ws.addEventListener("message", async ({ data }) => {
    if (writer) {
      await writer.write(data);
      return;
    }

    const u8 = new Uint8Array(data);
    if (u8.byteLength < 18) return ws.close();

    for (let i = 0; i < 16; i++) {
      if (u8[1 + i] !== UUID_BYTES[i]) return ws.close();
    }

    const view = new DataView(data);
    const pIdx = 19 + u8[17];
    const port = view.getUint16(pIdx);
    const type = u8[pIdx + 2];
    let offset = pIdx + 3;
    let host = "";

    if (type === 1) {
      host = `${u8[offset++]}.${u8[offset++]}.${u8[offset++]}.${u8[offset++]}`;
    } else if (type === 2) {
      const len = u8[offset++];
      host = TEXT_DEC.decode(u8.subarray(offset, offset += len));
    } else if (type === 3) {
      const hex = [];
      for (let i = 0; i < 8; i++, offset += 2) hex.push(view.getUint16(offset).toString(16));
      host = hex.join(":");
    } else {
      return ws.close();
    }

    try {
      tcp = connect({ hostname: host, port });
      await tcp.opened;
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
      write(chunk) { ws.send(chunk); },
      close() { ws.close(); },
      abort() { ws.close(); }
    })).catch(() => ws.close());
  });

  ws.addEventListener("close", () => tcp?.close());
  ws.addEventListener("error", () => tcp?.close());
}