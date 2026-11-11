import { connect } from 'cloudflare:sockets';

const PAGE = '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Welcome to nginx!</title><style>html{color-scheme:light}body{width:35em;margin:0 auto;font-family:Tahoma,Verdana,Arial,sans-serif;background:#fff;color:#000}</style></head><body><h1>Welcome to nginx!</h1><p>If you see this page, the nginx web server is successfully installed and working. Further configuration is required.</p><p>For online documentation and support please refer to <a href="http://nginx.org/">nginx.org</a>.<br>Commercial support is available at <a href="http://nginx.com/">nginx.com</a>.</p><p><em>Thank you for using nginx.</em></p></body></html>';
const DNS_HOST = '8.8.4.4';
const DIAL_TIMEOUT = 3000;
const MAX_QUEUE = 8 << 20;
const MAX_REPLAY = 64 << 10;

let rawId;
let idBytes;

function getId(raw) {
  if (raw !== rawId) {
    rawId = raw;
    const h = typeof raw === 'string' ? raw.trim().replace(/-/g, '') : '';
    idBytes = /^[0-9a-fA-F]{32}$/.test(h) ? Uint8Array.from(h.match(/../g), (x) => parseInt(x, 16)) : null;
  }
  return idBytes;
}

function parseProxy(v) {
  const m = typeof v === 'string' && /^(?:\[([^\]]+)\]|([^:\s]+))(?::(\d+))?$/.exec(v.trim());
  return m ? { hostname: m[1] || m[2], port: +m[3] || 0 } : null;
}

function parseHeader(b, id) {
  if (b.length < 22) return null;
  for (let i = 0; i < 16; i++) if (b[1 + i] !== id[i]) return null;
  let o = 18 + b[17];
  if (o + 4 > b.length) return null;
  const cmd = b[o];
  if (cmd !== 1 && cmd !== 2) return null;
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
  if (cmd === 2) return port === 53 ? { ver: b[0], host: DNS_HOST, port: 53, offset: o } : null;
  return { ver: b[0], host, port, offset: o };
}

function decodeEarly(p, id) {
  if (!p || p.length > 11000) return null;
  try {
    const b = Uint8Array.from(atob(p.replace(/-/g, '+').replace(/_/g, '/')), (c) => c.charCodeAt(0));
    return b.length > 17 && id.every((v, i) => b[1 + i] === v) ? b : null;
  } catch {
    return null;
  }
}

function kill(s) {
  try { Promise.resolve(s.close()).catch(() => {}); } catch {}
}

async function dial(hostname, port) {
  const s = connect({ hostname, port });
  s.closed.catch(() => {});
  let timer;
  try {
    await Promise.race([
      s.opened,
      new Promise((_, rej) => { timer = setTimeout(() => rej(new Error('timeout')), DIAL_TIMEOUT); }),
    ]);
    return s;
  } catch (e) {
    kill(s);
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

function tunnel(ws, id, proxy, early) {
  let sock, writer, head, target;
  let sent = proxy ? [] : null;
  let sentBytes = 0, got = false, retried = false, redialing = false;
  let done = false, queued = 0, chain = Promise.resolve();

  const shut = () => {
    if (done) return;
    done = true;
    if (sock) kill(sock);
    try { ws.close(1000); } catch {}
  };

  const fail = (e) => {
    console.log(target ? target.host + ':' + target.port : '-', (e && e.message) || e);
    shut();
  };

  const pump = async (s) => {
    const reader = s.readable.getReader();
    let first = head;
    try {
      for (;;) {
        const { done: end, value } = await reader.read();
        if (end || done || s !== sock) break;
        if (!value.length) continue;
        got = true;
        sent = null;
        if (first) {
          const m = new Uint8Array(2 + value.length);
          m.set(first);
          m.set(value, 2);
          ws.send(m);
          first = null;
        } else {
          ws.send(value);
        }
      }
    } catch {}
    if (done || s !== sock) return;
    if (!got && sent && proxy && !retried) {
      retried = true;
      try { await redial(); return; } catch {}
    }
    if (!got) console.log(target.host + ':' + target.port, 'closed without data');
    shut();
  };

  const redial = async () => {
    redialing = true;
    const s = await dial(proxy.hostname, proxy.port || target.port);
    try {
      const w = s.writable.getWriter();
      for (let i = 0; sent && i < sent.length; i++) await w.write(sent[i]);
      if (!sent || done) throw new Error('replay aborted');
      kill(sock);
      sock = s;
      writer = w;
      redialing = false;
      pump(s).catch(fail);
    } catch (e) {
      kill(s);
      throw e;
    }
  };

  const write = async (bytes) => {
    if (sent) {
      sent.push(bytes);
      sentBytes += bytes.length;
      if (sentBytes > MAX_REPLAY) sent = null;
    }
    if (!redialing) await writer.write(bytes);
  };

  const open = async (bytes) => {
    const h = parseHeader(bytes, id);
    if (!h) throw new Error('bad header');
    target = h;
    head = new Uint8Array([h.ver, 0]);
    let s;
    try {
      s = await dial(h.host, h.port);
    } catch (e) {
      if (!proxy) throw e;
      retried = true;
      s = await dial(proxy.hostname, proxy.port || h.port);
    }
    if (done) return kill(s);
    sock = s;
    writer = s.writable.getWriter();
    pump(s).catch(fail);
    const first = bytes.subarray(h.offset);
    if (first.length) await write(first);
  };

  const push = (d) => {
    const size = d.byteLength ?? d.size;
    queued += size;
    if (queued > MAX_QUEUE) return shut();
    chain = chain.then(async () => {
      queued -= size;
      if (done) return;
      const bytes = d instanceof ArrayBuffer ? new Uint8Array(d) : new Uint8Array(await d.arrayBuffer());
      if (writer) await write(bytes);
      else await open(bytes);
    }).catch(fail);
  };

  ws.addEventListener('message', (e) => {
    if (typeof e.data === 'string') shut();
    else push(e.data);
  });
  ws.addEventListener('close', () => { chain = chain.then(shut); });
  ws.addEventListener('error', shut);
  if (early) push(early.buffer);
}

export default {
  async fetch(req, env) {
    const { pathname } = new URL(req.url);
    if (pathname === '/') {
      if (req.method !== 'GET' && req.method !== 'HEAD') return new Response(null, { status: 405, headers: { allow: 'GET, HEAD' } });
      return new Response(req.method === 'HEAD' ? null : PAGE, { headers: { 'content-type': 'text/html; charset=utf-8' } });
    }
    if (pathname !== '/ws') return new Response('Not Found', { status: 404 });
    if (req.method !== 'GET') return new Response(null, { status: 405, headers: { allow: 'GET' } });
    if ((req.headers.get('upgrade') || '').toLowerCase() !== 'websocket') return new Response('Upgrade Required', { status: 426 });
    const id = getId(env.UUID);
    if (!id) return new Response('Server Error', { status: 500 });
    const [client, server] = Object.values(new WebSocketPair());
    server.binaryType = 'arraybuffer';
    server.accept();
    tunnel(server, id, parseProxy(env.PROXYIP), decodeEarly(req.headers.get('sec-websocket-protocol'), id));
    return new Response(null, { status: 101, webSocket: client, headers: { 'Sec-WebSocket-Extensions': '' } });
  },
};
