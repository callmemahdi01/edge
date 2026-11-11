import { connect } from 'cloudflare:sockets';

const PAGE = '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Welcome to nginx!</title><style>html{color-scheme:light}body{width:35em;margin:0 auto;font-family:Tahoma,Verdana,Arial,sans-serif;background:#fff;color:#000}</style></head><body><h1>Welcome to nginx!</h1><p>If you see this page, the nginx web server is successfully installed and working. Further configuration is required.</p><p>For online documentation and support please refer to <a href="http://nginx.org/">nginx.org</a>.<br>Commercial support is available at <a href="http://nginx.com/">nginx.com</a>.</p><p><em>Thank you for using nginx.</em></p></body></html>';

let rawUUID;
let uuidBytes;

function getUUID(raw) {
  if (raw !== rawUUID) {
    rawUUID = raw;
    const h = typeof raw === 'string' ? raw.trim().replace(/-/g, '') : '';
    if (/^[0-9a-fA-F]{32}$/.test(h)) {
      uuidBytes = new Uint8Array(16);
      for (let i = 0; i < 16; i++) uuidBytes[i] = parseInt(h.substr(i * 2, 2), 16);
    } else {
      uuidBytes = null;
    }
  }
  return uuidBytes;
}

function parseHeader(b, id) {
  if (b.length < 22) return null;
  for (let i = 0; i < 16; i++) if (b[1 + i] !== id[i]) return null;
  let o = 18 + b[17];
  if (o + 4 > b.length || b[o] !== 1) return null;
  const port = (b[o + 1] << 8) | b[o + 2];
  const type = b[o + 3];
  o += 4;
  let host;
  if (type === 1) {
    if (o + 4 > b.length) return null;
    host = b.subarray(o, o + 4).join('.');
    o += 4;
  } else if (type === 2) {
    const l = b[o++];
    if (!l || o + l > b.length) return null;
    host = new TextDecoder().decode(b.subarray(o, o + l));
    o += l;
  } else if (type === 3) {
    if (o + 16 > b.length) return null;
    const dv = new DataView(b.buffer, b.byteOffset + o, 16);
    const parts = [];
    for (let i = 0; i < 8; i++) parts.push(dv.getUint16(i * 2).toString(16));
    host = parts.join(':');
    o += 16;
  } else {
    return null;
  }
  return { ver: b[0], host, port, offset: o };
}

function decodeEarly(p) {
  if (!p) return null;
  try {
    const bin = atob(p.replace(/-/g, '+').replace(/_/g, '/'));
    return Uint8Array.from(bin, (c) => c.charCodeAt(0));
  } catch {
    return null;
  }
}

function tunnel(ws, id, early) {
  let socket;
  let writer;
  let done = false;
  let chain = Promise.resolve();

  const shut = () => {
    if (done) return;
    done = true;
    try { socket?.close(); } catch {}
    try { ws.close(1000); } catch {}
  };

  const onData = async (data) => {
    if (done) return;
    const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);
    if (writer) return writer.write(bytes);
    const h = parseHeader(bytes, id);
    if (!h) throw new Error('bad header');
    socket = connect({ hostname: h.host, port: h.port });
    let timer;
    await Promise.race([
      socket.opened,
      new Promise((_, rej) => { timer = setTimeout(rej, 10000); }),
    ]).finally(() => clearTimeout(timer));
    writer = socket.writable.getWriter();
    const head = new Uint8Array([h.ver, 0]);
    let first = true;
    socket.readable
      .pipeTo(new WritableStream({
        write(chunk) {
          if (first) {
            first = false;
            const m = new Uint8Array(2 + chunk.length);
            m.set(head);
            m.set(chunk, 2);
            ws.send(m);
          } else {
            ws.send(chunk);
          }
        },
      }))
      .catch(() => {})
      .finally(shut);
    if (h.offset < bytes.length) await writer.write(bytes.subarray(h.offset));
  };

  const push = (d) => { chain = chain.then(() => onData(d)).catch(shut); };

  ws.addEventListener('message', (e) => {
    if (typeof e.data === 'string') shut();
    else push(e.data);
  });
  ws.addEventListener('close', shut);
  ws.addEventListener('error', shut);
  if (early) push(early);
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === '/') {
      if (req.method !== 'GET' && req.method !== 'HEAD') return new Response(null, { status: 405, headers: { allow: 'GET, HEAD' } });
      return new Response(req.method === 'HEAD' ? null : PAGE, { headers: { 'content-type': 'text/html; charset=utf-8' } });
    }
    if (url.pathname !== '/ws') return new Response('Not Found', { status: 404 });
    if (req.method !== 'GET') return new Response(null, { status: 405, headers: { allow: 'GET' } });
    if ((req.headers.get('upgrade') || '').toLowerCase() !== 'websocket') return new Response('Upgrade Required', { status: 426 });
    const id = getUUID(env.UUID);
    if (!id) return new Response('Server Error', { status: 500 });
    const proto = req.headers.get('sec-websocket-protocol');
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    tunnel(server, id, decodeEarly(proto));
    return new Response(null, {
      status: 101,
      webSocket: client,
      headers: proto ? { 'sec-websocket-protocol': proto } : undefined,
    });
  },
};
