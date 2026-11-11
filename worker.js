import { connect } from "cloudflare:sockets";

const userID = "7b102311-43fd-4e8f-877e-8090623c101d";
const proxyIPs = [
  "proxyip.cmliussss.net",
  "proxyip.us.fxxk.dedyn.io",
  "proxyip.sg.fxxk.dedyn.io"
];
let proxyIP = proxyIPs[Math.floor(Math.random() * proxyIPs.length)];

const WS_READY_STATE_OPEN = 1;
const WS_READY_STATE_CLOSING = 2;

const nginxPage = `<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Welcome to nginx!</title>
<style>
body { background: #fff; color: #000; font-family: -apple-system, sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }
.box { border: 1px solid #ccc; padding: 30px 40px; max-width: 550px; text-align: center; }
h1 { font-size: 22px; margin: 0 0 16px; }
p { font-size: 14px; line-height: 1.6; margin: 0 0 12px; }
</style>
</head>
<body>
<div class="box">
<h1>Welcome to nginx!</h1>
<p>If you see this page, the nginx web server is successfully installed and working. Further configuration is required.</p>
<p><em>Thank you for using nginx.</em></p>
</div>
</body>
</html>`;

export default {
  async fetch(request, env) {
    try {
      const upgradeHeader = request.headers.get("Upgrade");
      if (!upgradeHeader || upgradeHeader.toLowerCase() !== "websocket") {
        return new Response(nginxPage, {
          status: 200,
          headers: { "Content-Type": "text/html;charset=utf-8" },
        });
      }
      return await vlessOverWSHandler(request);
    } catch (err) {
      return new Response(err.toString(), { status: 500 });
    }
  },
};

async function vlessOverWSHandler(request) {
  const webSocketPair = new WebSocketPair();
  const [client, webSocket] = Object.values(webSocketPair);
  webSocket.accept();

  let address = "";
  let portWithRandomLog = "";
  const earlyDataHeader = request.headers.get("sec-websocket-protocol") || "";

  const readableWebSocketStream = makeReadableWebSocketStream(webSocket, earlyDataHeader);

  let remoteSocketWapper = { value: null };
  let udpStreamWrite = null;
  let isDns = false;

  readableWebSocketStream.pipeTo(new WritableStream({
    async write(chunk, controller) {
      if (isDns && udpStreamWrite) {
        return udpStreamWrite(chunk);
      }
      if (remoteSocketWapper.value) {
        const writer = remoteSocketWapper.value.writable.getWriter();
        await writer.write(chunk);
        writer.releaseLock();
        return;
      }

      const {
        hasError,
        message,
        addressType,
        portRemote = 443,
        addressRemote = "",
        rawDataIndex,
        vlessVersion = new Uint8Array([0, 0]),
        isUDP,
      } = processVlessHeader(chunk, userID);

      if (hasError) {
        throw new Error(message);
      }

      if (isUDP) {
        if (portRemote === 53) {
          isDns = true;
        } else {
          throw new Error("UDP proxy only enabled for DNS (port 53)");
        }
      }

      const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
      const rawClientData = chunk.slice(rawDataIndex);

      if (isDns) {
        const { write } = await handleUDPOutBound(webSocket, vlessResponseHeader);
        udpStreamWrite = write;
        udpStreamWrite(rawClientData);
        return;
      }

      handleTCPOutBound(
        remoteSocketWapper,
        addressRemote,
        portRemote,
        rawClientData,
        webSocket,
        vlessResponseHeader
      );
    },
    close() {},
    abort(reason) {},
  })).catch((err) => {});

  return new Response(null, {
    status: 101,
    webSocket: client,
  });
}

async function handleTCPOutBound(remoteSocket, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader) {
  async function connectAndWrite(address, port) {
    const tcpSocket = connect({ hostname: address, port: port });
    remoteSocket.value = tcpSocket;
    const writer = tcpSocket.writable.getWriter();
    await writer.write(rawClientData);
    writer.releaseLock();
    return tcpSocket;
  }

  async function retry() {
    const tcpSocket = await connectAndWrite(proxyIP || addressRemote, portRemote);
    tcpSocket.closed.catch(() => {}).finally(() => {
      safeCloseWebSocket(webSocket);
    });
    remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, null);
  }

  try {
    const tcpSocket = await connectAndWrite(addressRemote, portRemote);
    remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, retry);
  } catch (err) {
    await retry();
  }
}

async function remoteSocketToWS(remoteSocket, webSocket, vlessResponseHeader, retry) {
  let vlessHeader = vlessResponseHeader;
  let hasIncomingData = false;

  await remoteSocket.readable.pipeTo(
    new WritableStream({
      start() {},
      async write(chunk, controller) {
        hasIncomingData = true;
        if (webSocket.readyState !== WS_READY_STATE_OPEN) {
          controller.error("webSocket is not open");
        }
        if (vlessHeader) {
          webSocket.send(await new Blob([vlessHeader, chunk]).arrayBuffer());
          vlessHeader = null;
        } else {
          webSocket.send(chunk);
        }
      },
      close() {},
      abort(reason) {},
    })
  ).catch((error) => {
    safeCloseWebSocket(webSocket);
  });

  if (hasIncomingData === false && retry) {
    await retry();
  }
}

function makeReadableWebSocketStream(webSocketServer, earlyDataHeader) {
  let readableStreamCancel = false;
  return new ReadableStream({
    start(controller) {
      webSocketServer.addEventListener("message", (event) => {
        if (readableStreamCancel) return;
        controller.enqueue(event.data);
      });

      webSocketServer.addEventListener("close", () => {
        safeCloseWebSocket(webSocketServer);
        if (!readableStreamCancel) controller.close();
      });

      webSocketServer.addEventListener("error", (err) => {
        controller.error(err);
      });

      const { earlyData, error } = base64ToArrayBuffer(earlyDataHeader);
      if (error) {
        controller.error(error);
      } else if (earlyData) {
        controller.enqueue(earlyData);
      }
    },
    cancel() {
      readableStreamCancel = true;
      safeCloseWebSocket(webSocketServer);
    },
  });
}

function processVlessHeader(vlessBuffer, userID) {
  if (vlessBuffer.byteLength < 24) {
    return { hasError: true, message: "invalid data" };
  }
  const version = new Uint8Array(vlessBuffer.slice(0, 1));
  let isValidUser = false;
  let slicedBuffer = new Uint8Array(vlessBuffer.slice(1, 17));
  const slicedBufferString = stringify(slicedBuffer);
  isValidUser = slicedBufferString === userID;

  if (!isValidUser) {
    return { hasError: true, message: "invalid user" };
  }

  const optLength = new Uint8Array(vlessBuffer.slice(17, 18))[0];
  const command = new Uint8Array(vlessBuffer.slice(18 + optLength, 18 + optLength + 1))[0];
  const isUDP = command === 2;

  const portIndex = 18 + optLength + 1;
  const portBuffer = vlessBuffer.slice(portIndex, portIndex + 2);
  const portRemote = new DataView(portBuffer).getUint16(0);

  let addressIndex = portIndex + 2;
  const addressBuffer = new Uint8Array(vlessBuffer.slice(addressIndex, addressIndex + 1));
  const addressType = addressBuffer[0];
  let addressLength = 0;
  let addressValueIndex = addressIndex + 1;
  let addressValue = "";

  switch (addressType) {
    case 1:
      addressLength = 4;
      addressValue = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)).join(".");
      break;
    case 2:
      addressLength = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + 1))[0];
      addressValueIndex += 1;
      addressValue = new TextDecoder().decode(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength));
      break;
    case 3:
      addressLength = 16;
      const dataView = new DataView(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength));
      const ipv6 = [];
      for (let i = 0; i < 8; i++) {
        ipv6.push(dataView.getUint16(i * 2).toString(16));
      }
      addressValue = ipv6.join(":");
      break;
    default:
      return { hasError: true, message: `invalid addressType is ${addressType}` };
  }

  return {
    hasError: false,
    addressRemote: addressValue,
    addressType,
    portRemote,
    rawDataIndex: addressValueIndex + addressLength,
    vlessVersion: version,
    isUDP,
  };
}

async function handleUDPOutBound(webSocket, vlessResponseHeader) {
  let isVlessHeaderSent = false;
  const transformStream = new TransformStream({
    async transform(chunk, controller) {
      if (chunk.byteLength < 2) return;
      const udpDataLength = new DataView(chunk.slice(0, 2)).getUint16(0);
      const udpData = chunk.slice(2, 2 + udpDataLength);
      
      try {
        const resp = await fetch("https://1.1.1.1/dns-query", {
          method: "POST",
          headers: { "content-type": "application/dns-message" },
          body: udpData,
        });
        const dnsQueryResult = await resp.arrayBuffer();
        const udpSize = dnsQueryResult.byteLength;
        const udpSizeBuffer = new Uint8Array([(udpSize >> 8) & 0xff, udpSize & 0xff]);

        if (webSocket.readyState === WS_READY_STATE_OPEN) {
          if (isVlessHeaderSent) {
            webSocket.send(await new Blob([udpSizeBuffer, dnsQueryResult]).arrayBuffer());
          } else {
            webSocket.send(await new Blob([vlessResponseHeader, udpSizeBuffer, dnsQueryResult]).arrayBuffer());
            isVlessHeaderSent = true;
          }
        }
      } catch (e) {}
    },
  });

  const writer = transformStream.writable.getWriter();
  return {
    write(chunk) {
      writer.write(chunk);
    },
  };
}

function base64ToArrayBuffer(base64Str) {
  if (!base64Str) return { error: null };
  try {
    base64Str = base64Str.replace(/-/g, "+").replace(/_/g, "/");
    const decode = atob(base64Str);
    const arbuf = new Uint8Array(decode.length);
    for (let i = 0; i < decode.length; i++) {
      arbuf[i] = decode.charCodeAt(i);
    }
    return { earlyData: arbuf.buffer, error: null };
  } catch (error) {
    return { error };
  }
}

const byteToHex = [];
for (let i = 0; i < 256; ++i) {
  byteToHex.push((i + 256).toString(16).slice(1));
}

function stringify(arr, offset = 0) {
  return (
    byteToHex[arr[offset + 0]] + byteToHex[arr[offset + 1]] + byteToHex[arr[offset + 2]] + byteToHex[arr[offset + 3]] + "-" +
    byteToHex[arr[offset + 4]] + byteToHex[arr[offset + 5]] + "-" +
    byteToHex[arr[offset + 6]] + byteToHex[arr[offset + 7]] + "-" +
    byteToHex[arr[offset + 8]] + byteToHex[arr[offset + 9]] + "-" +
    byteToHex[arr[offset + 10]] + byteToHex[arr[offset + 11]] + byteToHex[arr[offset + 12]] + byteToHex[arr[offset + 13]] + byteToHex[arr[offset + 14]] + byteToHex[arr[offset + 15]]
  ).toLowerCase();
}

function safeCloseWebSocket(socket) {
  try {
    if (socket.readyState === WS_READY_STATE_OPEN || socket.readyState === WS_READY_STATE_CLOSING) {
      socket.close();
    }
  } catch (error) {}
}
