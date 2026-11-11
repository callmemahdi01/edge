import { connect } from "cloudflare:sockets";

const USER_ID = "d342d11e-d424-4583-b36e-524ab1f0afa4";
const WS_PATH = "/vless-ws";
const PROXY_IPS = ["proxyip.cmliussss.net"];

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
    const isWebSocket = upgradeHeader === "websocket";

    if (isWebSocket && url.pathname === WS_PATH) {
      const customProxy = url.searchParams.get("proxyip");
      return await upgradeWebSocket(customProxy);
    }

    if (url.pathname === "/") {
      return new Response(NGINX_HTML, {
        status: 200,
        headers: { "Content-Type": "text/html; charset=UTF-8" },
      });
    }

    return new Response(NGINX_HTML, {
      status: 404,
      headers: { "Content-Type": "text/html; charset=UTF-8" },
    });
  },
};

async function upgradeWebSocket(customProxy) {
  const [client, webSocket] = Object.values(new WebSocketPair());
  webSocket.accept();
  webSocket.binaryType = "arraybuffer";
  webSocket.send(new Uint8Array([0, 0]));
  setupPipeline(webSocket, customProxy);
  return new Response(null, { status: 101, webSocket: client });
}

async function setupPipeline(webSocket, customProxy) {
  let tcpSocket;
  let isFirstChunk = true;
  let queue = Promise.resolve();
  let writer;

  webSocket.addEventListener("message", (event) => {
    queue = queue.then(async () => {
      if (isFirstChunk) {
        isFirstChunk = false;
        await parseVlessHeader(event.data);
      } else {
        await writer.write(event.data);
      }
    });
  });

  async function parseVlessHeader(vlessData) {
    if (checkVlessId(new Uint8Array(vlessData.slice(1, 17))) !== USER_ID) {
      return;
    }

    const addonLen = new Uint8Array(vlessData)[17];
    const portIndex = 18 + addonLen + 1;
    const portBuffer = vlessData.slice(portIndex, portIndex + 2);
    const destPort = new DataView(portBuffer).getUint16(0);

    const addrIndex = portIndex + 2;
    const addrBuffer = new Uint8Array(vlessData.slice(addrIndex, addrIndex + 1));
    const addrType = addrBuffer[0];

    let addrLen = 0;
    let destAddr = "";
    let addrDataIndex = addrIndex + 1;

    switch (addrType) {
      case 1:
        addrLen = 4;
        destAddr = new Uint8Array(vlessData.slice(addrDataIndex, addrDataIndex + addrLen)).join(".");
        break;
      case 2:
        addrLen = new Uint8Array(vlessData.slice(addrDataIndex, addrDataIndex + 1))[0];
        addrDataIndex += 1;
        destAddr = new TextDecoder().decode(vlessData.slice(addrDataIndex, addrDataIndex + addrLen));
        break;
      case 3:
        addrLen = 16;
        const dataView = new DataView(vlessData.slice(addrDataIndex, addrDataIndex + addrLen));
        const ipv6 = [];
        for (let i = 0; i < 8; i++) {
          ipv6.push(dataView.getUint16(i * 2).toString(16));
        }
        destAddr = ipv6.join(":");
        break;
      default:
        return;
    }

    const initialData = vlessData.slice(addrDataIndex + addrLen);

    try {
      tcpSocket = connect({ hostname: destAddr, port: destPort });
      await tcpSocket.opened;
    } catch {
      const chosenProxy = customProxy || PROXY_IPS[Math.floor(Math.random() * PROXY_IPS.length)] || "proxyip.cmliussss.net";
      const [proxyIp, proxyPort = destPort] = chosenProxy.split(":");
      tcpSocket = connect({ hostname: proxyIp, port: Number(proxyPort) });
      await tcpSocket.opened;
    }

    createTransferPipeline(initialData);
  }

  function checkVlessId(arr, offset = 0) {
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

  async function createTransferPipeline(initialData) {
    writer = tcpSocket.writable.getWriter();

    if (initialData?.byteLength > 0) {
      await writer.write(initialData);
    }

    tcpSocket.readable.pipeTo(
      new WritableStream({
        write(chunk) {
          webSocket.send(chunk);
        },
      })
    );
  }
}