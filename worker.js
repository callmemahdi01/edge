import { connect } from "cloudflare:sockets";

let مسیر_اشتراک = "مسیر-اشتراک";
let شناسه_تایید = "7b102311-43fd-4e8f-877e-8090623c101d";
let آی_پی_های_پروکسی = [
  "proxyip.cmliussss.net",
  "proxyip.us.fxxk.dedyn.io",
  "proxyip.sg.fxxk.dedyn.io"
];

const فرمت_کلید = Array.from({ length: 256 }, (_, i) => (i + 256).toString(16).slice(1));

const صفحه_اصلی_انجین‌ایکس = `<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Welcome to nginx!</title>
<style>
body {
    background-color: #ffffff;
    color: #000000;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    display: flex;
    justify-content: center;
    align-items: center;
    height: 100vh;
    margin: 0;
}
.box {
    border: 1px solid #cccccc;
    padding: 30px 40px;
    max-width: 550px;
    text-align: center;
}
h1 {
    font-size: 22px;
    margin: 0 0 16px 0;
}
p {
    font-size: 14px;
    line-height: 1.6;
    margin: 0 0 12px 0;
}
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
  async fetch(درخواست, env) {
    شناسه_تایید = env.UUID ?? شناسه_تایید;
    if (env.PROXY_IP) آی_پی_های_پروکسی = تفکیک_ارایه(env.PROXY_IP);

    const url = new URL(درخواست.url);
    const هدر_ارتقا = درخواست.headers.get("Upgrade");
    const درخواست_وب_سوکت = هدر_ارتقا == "websocket";

    const مسیر_جاری = decodeURIComponent(url.pathname);
    const مسیر_ریشه = مسیر_جاری === "/" || مسیر_جاری === "";

    if (!درخواست_وب_سوکت) {
      if (مسیر_ریشه) {
        return new Response(صفحه_اصلی_انجین‌ایکس, {
          status: 200,
          headers: { "Content-Type": "text/html;charset=utf-8" },
        });
      }
      return new Response("Not Found", { status: 404 });
    }

    if (درخواست_وب_سوکت) {
      const پروکسی_سفارشی = url.searchParams.get("proxyip");
      return await ارتقا_به_وب_سوکت(پروکسی_سفارشی);
    }
  },
};

async function ارتقا_به_وب_سوکت(پروکسی_سفارشی) {
  const [کلاینت, رابط_وب_سوکت] = Object.values(new WebSocketPair());
  رابط_وب_سوکت.accept();
  رابط_وب_سوکت.binaryType = "arraybuffer";
  رابط_وب_سوکت.send(new Uint8Array([0, 0]));
  راه‌اندازی_خط_لوله(رابط_وب_سوکت, پروکسی_سفارشی);
  return new Response(null, { status: 101, webSocket: کلاینت });
}

async function راه‌اندازی_خط_لوله(رابط_وب_سوکت, پروکسی_سفارشی) {
  let رابط_تی‌سی‌پی;
  let داده_اولیه = true;
  let صف_پردازش = Promise.resolve();
  let داده_انتقالی;

  رابط_وب_سوکت.addEventListener("message", (event) => {
    صف_پردازش = صف_پردازش.then(async () => {
      if (داده_اولیه) {
        داده_اولیه = false;
        await تجزیه_هدر_ویلس(event.data);
      } else {
        await داده_انتقالی.write(event.data);
      }
    });
  });

  async function تجزیه_هدر_ویلس(داده_ویلس) {
    if (بررسی_کلید_ویلس(new Uint8Array(داده_ویلس.slice(1, 17))) !== شناسه_تایید) {
      return;
    }

    const موقعیت_داده = new Uint8Array(داده_ویلس)[17];
    const شاخص_پورت = 18 + موقعیت_داده + 1;
    const بافر_پورت = داده_ویلس.slice(شاخص_پورت, شاخص_پورت + 2);
    const پورت_مقصد = new DataView(بافر_پورت).getUint16(0);

    const شاخص_آدرس = شاخص_پورت + 2;
    const بافر_آدرس = new Uint8Array(داده_ویلس.slice(شاخص_آدرس, شاخص_آدرس + 1));
    const نوع_آدرس = بافر_آدرس[0];

    let طول_آدرس = 0;
    let آدرس_مقصد = "";
    let شاخص_اطلاعات_آدرس = شاخص_آدرس + 1;

    switch (نوع_آدرس) {
      case 1:
        طول_آدرس = 4;
        آدرس_مقصد = new Uint8Array(داده_ویلس.slice(شاخص_اطلاعات_آدرس, شاخص_اطلاعات_آدرس + طول_آدرس)).join(".");
        break;
      case 2:
        طول_آدرس = new Uint8Array(داده_ویلس.slice(شاخص_اطلاعات_آدرس, شاخص_اطلاعات_آدرس + 1))[0];
        شاخص_اطلاعات_آدرس += 1;
        آدرس_مقصد = new TextDecoder().decode(داده_ویلس.slice(شاخص_اطلاعات_آدرس, شاخص_اطلاعات_آدرس + طول_آدرس));
        break;
      case 3:
        طول_آدرس = 16;
        const dataView = new DataView(داده_ویلس.slice(شاخص_اطلاعات_آدرس, شاخص_اطلاعات_آدرس + طول_آدرس));
        const ipv6 = [];
        for (let i = 0; i < 8; i++) {
          ipv6.push(dataView.getUint16(i * 2).toString(16));
        }
        آدرس_مقصد = ipv6.join(":");
        break;
      default:
        return;
    }

    const نوشتن_داده_اولیه = داده_ویلس.slice(شاخص_اطلاعات_آدرس + طول_آدرس);

    try {
      رابط_تی‌سی‌پی = connect({ hostname: آدرس_مقصد, port: پورت_مقصد });
      await رابط_تی‌سی‌پی.opened;
    } catch {
      const پروکسی_انتخابی = پروکسی_سفارشی || آی_پی_های_پروکسی[Math.floor(Math.random() * آی_پی_های_پروکسی.length)] || "proxyip.cmliussss.net";
      const [آدرس_آی_پی_پروکسی, پورت_آی_پی_پروکسی = پورت_مقصد] = پروکسی_انتخابی.split(":");
      رابط_تی‌سی‌پی = connect({ hostname: آدرس_آی_پی_پروکسی, port: Number(پورت_آی_پی_پروکسی) });
      await رابط_تی‌سی‌پی.opened;
    }

    ایجاد_خط_انتقال(نوشتن_داده_اولیه);
  }

  function بررسی_کلید_ویلس(arr, offset = 0) {
    const uuid = (فرمت_کلید[arr[offset + 0]] + فرمت_کلید[arr[offset + 1]] + فرمت_کلید[arr[offset + 2]] + فرمت_کلید[arr[offset + 3]] + "-" + فرمت_کلید[arr[offset + 4]] + فرمت_کلید[arr[offset + 5]] + "-" + فرمت_کلید[arr[offset + 6]] + فرمت_کلید[arr[offset + 7]] + "-" + فرمت_کلید[arr[offset + 8]] + فرمت_کلید[arr[offset + 9]] + "-" + فرمت_کلید[arr[offset + 10]] + فرمت_کلید[arr[offset + 11]] + فرمت_کلید[arr[offset + 12]] + فرمت_کلید[arr[offset + 13]] + فرمت_کلید[arr[offset + 14]] + فرمت_کلید[arr[offset + 15]]).toLowerCase();
    return uuid;
  }

  async function ایجاد_خط_انتقال(نوشتن_داده_اولیه) {
    داده_انتقالی = رابط_تی‌سی‌پی.writable.getWriter();

    if (نوشتن_داده_اولیه?.byteLength > 0) {
      await داده_انتقالی.write(نوشتن_داده_اولیه);
    }

    رابط_تی‌سی‌پی.readable.pipeTo(
      new WritableStream({
        write(chunk) {
          رابط_وب_سوکت.send(chunk);
        },
      })
    );
  }
}

function تفکیک_ارایه(ورودی) {
  if (Array.isArray(ورودی)) return ورودی;
  if (typeof ورودی === "string") {
    return ورودی.split(/[\r\n,]+/).map((item) => item.trim()).filter(Boolean);
  }
  return [];
}
