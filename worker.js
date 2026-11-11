import { connect } from "cloudflare:sockets";

const USER_ID = "d342d11e-d424-4583-b36e-524ab1f0afa4";
const WS_PATH = "/vless-ws";
const PROXY_IP = "proxyip.cmliussss.net";

const BYTE_HEX = Array.from({ length: 256 }, (_, i) => (i + 256).toString(16).slice(1));

const NGINX_HTML = `<!DOCTYPE html>
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
</html>`;

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const upgradeHeader = request.headers.get("Upgrade");

    if (upgradeHeader === "websocket" && url.pathname === WS_PATH) {
      const [client, server] = Object.values(new WebSocketPair());
      server.accept();
      handleVlessSession(server);
      return new Response(null, { status: 101, webSocket: client });
    }

    if (url.pathname === "/") {
      return new Response(NGINX_HTML, {
        status: 200,
        headers: { "Content-Type": "text/html; charset=UTF-8" }
      });
    }

    return new Response(NGINX_HTML, {
      status: 404,
      headers: { "Content-Type": "text/html; charset=UTF-8" }
    });
  }
};

function handleVlessSession(webSocket) {
  let tcpSocket;
  let isFirstChunk = true;
  let queue = Promise.resolve();
  let writer;

  webSocket.addEventListener("message", (event) => {
    queue = queue.then(async () => {
      if (isFirstChunk) {
        isFirstChunk = false;
        await parseVlessHeader(event.data);
      } else if (writer) {
        await writer.write(event.data);
      }
    });
  });

  webSocket.addEventListener("close", () => {
    try { tcpSocket?.close(); } catch {}
  });

  webSocket.addEventListener("error", () => {
    try { tcpSocket?.close(); } catch {}
  });

  async function parseVlessHeader(buffer) {
    if (buffer.byteLength < 18) {
      webSocket.close();
      return;
    }

    const clientUuid = stringifyUuid(new Uint8Array(buffer.slice(1, 17)));
    if (clientUuid !== USER_ID) {
      webSocket.close();
      return;
    }

    const protoVersion = new Uint8Array(buffer)[0];
    const addonLen = new Uint8Array(buffer)[17];
    const portIndex = 18 + addonLen + 1;
    const destPort = new DataView(buffer.slice(portIndex, portIndex + 2)).getUint16(0);

    const addrTypeIndex = portIndex + 2;
    const addrType = new Uint8Array(buffer.slice(addrTypeIndex, addrTypeIndex + 1))[0];

    let addrLength = 0;
    let destAddr = "";
    let addrValueIndex = addrTypeIndex + 1;

    switch (addrType) {
      case 1:
        addrLength = 4;
        destAddr = new Uint8Array(buffer.slice(addrValueIndex, addrValueIndex + addrLength)).join(".");
        break;
      case 2:
        addrLength = new Uint8Array(buffer.slice(addrValueIndex, addrValueIndex + 1))[0];
        addrValueIndex += 1;
        destAddr = new TextDecoder().decode(buffer.slice(addrValueIndex, addrValueIndex + addrLength));
        break;
      case 3:
        addrLength = 16;
        const view = new DataView(buffer.slice(addrValueIndex, addrValueIndex + addrLength));
        const parts = [];
        for (let i = 0; i < 8; i++) {
          parts.push(view.getUint16(i * 2).toString(16));
        }
        destAddr = parts.join(":");
        break;
      default:
        webSocket.close();
        return;
    }

    const payload = buffer.slice(addrValueIndex + addrLength);

    try {
      tcpSocket = connect({ hostname: destAddr, port: destPort });
      await tcpSocket.opened;
    } catch {
      try {
        const [proxyHost, proxyPort = destPort] = PROXY_IP.split(":");
        tcpSocket = connect({ hostname: proxyHost, port: Number(proxyPort) });
        await tcpSocket.opened;
      } catch {
        webSocket.close();
        return;
      }
    }

    writer = tcpSocket.writable.getWriter();
    webSocket.send(new Uint8Array([protoVersion, 0]));

    if (payload.byteLength > 0) {
      await writer.write(payload);
    }

    tcpSocket.readable.pipeTo(
      new WritableStream({
        write(chunk) {
          if (webSocket.readyState === 1) {
            webSocket.send(chunk);
          }
        },
        close() {
          webSocket.close();
        },
        abort() {
          webSocket.close();
        }
      })
    ).catch(() => {
      webSocket.close();
    });
  }

  function stringifyUuid(arr, offset = 0) {
    return (
      BYTE_HEX[arr[offset + 0]] +
      BYTE_HEX[arr[offset + 1]] +
      BYTE_HEX[arr[offset + 2]] +
      BYTE_HEX[arr[offset + 3]] +
      "-" +
      BYTE_HEX[arr[offset + 4]] +
      BYTE_HEX[arr[offset + 5]] +
      "-" +
      BYTE_HEX[arr[offset + 6]] +
      BYTE_HEX[arr[offset + 7]] +
      "-" +
      BYTE_HEX[arr[offset + 8]] +
      BYTE_HEX[arr[offset + 9]] +
      "-" +
      BYTE_HEX[arr[offset + 10]] +
      BYTE_HEX[arr[offset + 11]] +
      BYTE_HEX[arr[offset + 12]] +
      BYTE_HEX[arr[offset + 13]] +
      BYTE_HEX[arr[offset + 14]] +
      BYTE_HEX[arr[offset + 15]]
    ).toLowerCase();
  }
}