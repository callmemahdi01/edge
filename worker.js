import { connect } from "cloudflare:sockets";

const شناسه_تایید = "7b102311-43fd-4e8f-877e-8090623c101d";
const آی_پی_پروکسی_پیش‌فرض = "proxyip.cmliussss.net";

const صفحه_اصلی_انجین‌ایکس = `<!DOCTYPE html>
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
  async fetch(درخواست, env) {
    const هدر_ارتقا = درخواست.headers.get("Upgrade");
    if (!هدر_ارتقا || هدر_ارتقا.toLowerCase() !== "websocket") {
      return new Response(صفحه_اصلی_انجین‌ایکس, {
        status: 200,
        headers: { "Content-Type": "text/html;charset=utf-8" },
      });
    }

    const پروکسی_سفارشی = new URL(درخواست.url).searchParams.get("proxyip") || env.PROXY_IP || آی_پی_پروکسی_پیش‌فرض;
    const داده_اولیه_هدر = درخواست.headers.get("sec-websocket-protocol") || "";

    const [کلاینت, رابط_وب_سوکت] = Object.values(new WebSocketPair());
    رابط_وب_سوکت.accept();

    مدیریت_ارتباط_ویلس(رابط_وب_سوکت, داده_اولیه_هدر, پروکسی_سفارشی);

    return new Response(null, {
      status: 101,
      webSocket: کلاینت,
    });
  },
};

async function مدیریت_ارتباط_ویلس(رابط_وب_سوکت, داده_اولیه_هدر, پروکسی_سفارشی) {
  let رابط_تی‌سی‌پی = null;
  let نویسنده_تی‌سی‌پی = null;
  let هدر_پاسخ_ویلس = null;
  let اولین_بسته = true;

  const جریان_ورودی = ساخت_جریان_وب_سوکت(رابط_وب_سوکت, داده_اولیه_هدر);

  جریان_ورودی.pipeTo(
    new WritableStream({
      async write(قطعه_داده) {
        if (نویسنده_تی‌سی‌پی) {
          await نویسنده_تی‌سی‌پی.write(قطعه_داده);
          return;
        }

        if (اولین_بسته) {
          اولین_بسته = false;
          const نتیجه = تجزیه_هدر_ویلس(قطعه_داده);
          if (نتیجه.خطا) {
            بستن_امن_سوکت(رابط_وب_سوکت);
            return;
          }

          هدر_پاسخ_ویلس = new Uint8Array([نتیجه.نسخه, 0]);
          const داده_خام_کلاینت = قطعه_داده.slice(نتیجه.نقطه_شروع_داده);

          try {
            رابط_تی‌سی‌پی = connect({ hostname: نتیجه.آدرس_مقصد, port: نتیجه.پورت_مقصد });
            await رابط_تی‌سی‌پی.opened;
          } catch {
            const [آدرس_پروکسی, پورت_پروکسی = نتیجه.پورت_مقصد] = پروکسی_سفارشی.split(":");
            رابط_تی‌سی‌پی = connect({ hostname: آدرس_پروکسی, port: Number(پورت_پروکسی) });
            await رابط_تی‌سی‌پی.opened;
          }

          نویسنده_تی‌سی‌پی = رابط_تی‌سی‌پی.writable.getWriter();
          if (داده_خام_کلاینت.byteLength > 0) {
            await نویسنده_تی‌سی‌پی.write(داده_خام_کلاینت);
          }

          انتقال_داده_به_وب_سوکت(رابط_تی‌سی‌پی, رابط_وب_سوکت, هدر_پاسخ_ویلس, پروکسی_سفارشی, نتیجه, داده_خام_کلاینت);
        }
      },
      close() {
        if (رابط_تی‌سی‌پی) try { رابط_تی‌سی‌پی.close(); } catch {}
      },
      abort() {
        if (رابط_تی‌سی‌پی) try { رابط_تی‌سی‌پی.close(); } catch {}
      },
    })
  ).catch(() => بستن_امن_سوکت(رابط_وب_سوکت));
}

async function انتقال_داده_به_وب_سوکت(رابط_تی‌سی‌پی, رابط_وب_سوکت, هدر_پاسخ, پروکسی_سفارشی, اطلاعات_مقصد, داده_اولیه) {
  let هدر_ارسال_شده = false;
  let داده_ای_دریافت_شد = false;

  await رابط_تی‌سی‌پی.readable.pipeTo(
    new WritableStream({
      async write(قطعه) {
        داده_ای_دریافت_شد = true;
        if (!هدر_ارسال_شده) {
          هدر_ارسال_شده = true;
          const بسته_ترکیبی = await new Blob([هدر_پاسخ, قطعه]).arrayBuffer();
          rابط_وب_سوکت.send(بسته_ترکیبی);
        } else {
          رابط_وب_سوکت.send(قطعه);
        }
      },
    })
  ).catch(() => {});

  if (!داده_ای_دریافت_شد && پروکسی_سفارشی) {
    try {
      const [آدرس_پروکسی, پورت_پروکسی = اطلاعات_مقصد.پورت_مقصد] = پروکسی_سفارشی.split(":");
      const سوکت_مجدد = connect({ hostname: آدرس_پروکسی, port: Number(پورت_پروکسی) });
      await سوکت_مجدد.opened;
      const نویسنده = سوکت_مجدد.writable.getWriter();
      if (داده_اولیه.byteLength > 0) await نویسنده.write(داده_اولیه);

      let هدر_مجدد = false;
      await سوکت_مجدد.readable.pipeTo(
        new WritableStream({
          async write(قطعه) {
            if (!هدر_مجدد) {
              هدر_مجدد = true;
              const بسته_ترکیبی = await new Blob([هدر_پاسخ, قطعه]).arrayBuffer();
              رابط_وب_سوکت.send(بسته_ترکیبی);
            } else {
              رابط_وب_سوکت.send(قطعه);
            }
          },
        })
      );
    } catch {}
  }

  بستن_امن_سوکت(رابط_وب_سوکت);
}

function ساخت_جریان_وب_سوکت(سوکت, داده_اولیه_هدر) {
  return new ReadableStream({
    start(controller) {
      سوکت.addEventListener("message", (event) => controller.enqueue(event.data));
      سوکت.addEventListener("close", () => controller.close());
      سوکت.addEventListener("error", (err) => controller.error(err));

      if (داده_اولیه_هدر) {
        try {
          const متن_بیس۶۴ = داده_اولیه_هدر.replace(/-/g, "+").replace(/_/g, "/");
          const باینری = atob(متن_بیس۶۴);
          const بافر = new Uint8Array(باینری.length);
          for (let i = 0; i < باینری.length; i++) بافر[i] = باینری.charCodeAt(i);
          controller.enqueue(بافر.buffer);
        } catch {}
      }
    },
  });
}

function تجزیه_هدر_ویلس(بافر) {
  if (بافر.byteLength < 24) return { خطا: true };
  const نسخه = new Uint8Array(بافر.slice(0, 1))[0];
  const شناسه_دریافتی = تبدیل_به_شناسه(new Uint8Array(بافر.slice(1, 17)));

  if (شناسه_دریافتی !== شناسه_تایید) return { خطا: true };

  const طول_تنظیمات = new Uint8Array(بافر.slice(17, 18))[0];
  const شاخص_پورت = 18 + طول_تنظیمات + 1;
  const پورت_مقصد = new DataView(بافر.slice(شاخص_پورت, شاخص_پورت + 2)).getUint16(0);

  const شاخص_نوع_آدرس = شاخص_پورت + 2;
  const نوع_آدرس = new Uint8Array(بافر.slice(شاخص_نوع_آدرس, شاخص_نوع_آدرس + 1))[0];
  let طول_آدرس = 0;
  let شاخص_مقدار = شاخص_نوع_آدرس + 1;
  let آدرس_مقصد = "";

  switch (نوع_آدرس) {
    case 1:
      طول_آدرس = 4;
      آدرس_مقصد = new Uint8Array(بافر.slice(شاخص_مقدار, شاخص_مقدار + طول_آدرس)).join(".");
      break;
    case 2:
      طول_آدرس = new Uint8Array(بافر.slice(شاخص_مقدار, شاخص_مقدار + 1))[0];
      شاخص_مقدار += 1;
      آدرس_مقصد = new TextDecoder().decode(بافر.slice(شاخص_مقدار, شاخص_مقدار + طول_آدرس));
      break;
    case 3:
      طول_آدرس = 16;
      const دیتا_ویو = new DataView(بافر.slice(شاخص_مقدار, شاخص_مقدار + طول_آدرس));
      const بخش_ها = [];
      for (let i = 0; i < 8; i++) بخش_ها.push(دیتا_ویو.getUint16(i * 2).toString(16));
      آدرس_مقصد = بخش_ها.join(":");
      break;
    default:
      return { خطا: true };
  }

  return {
    خطا: false,
    نسخه,
    آدرس_مقصد,
    پورت_مقصد,
    نقطه_شروع_داده: شاخص_مقدار + طول_آدرس,
  };
}

const فرمت_هگز = Array.from({ length: 256 }, (_, i) => (i + 256).toString(16).slice(1));
function تبدیل_به_شناسه(آرایه) {
  return (
    فرمت_هگز[آرایه[0]] + فرمت_هگز[آرایه[1]] + فرمت_هگز[آرایه[2]] + فرمت_هگز[آرایه[3]] + "-" +
    فرمت_هگز[آرایه[4]] + فرمت_هگز[آرایه[5]] + "-" +
    فرمت_هگز[آرایه[6]] + فرمت_هگز[آرایه[7]] + "-" +
    فرمت_هگز[آرایه[8]] + فرمت_هگز[آرایه[9]] + "-" +
    فرمت_هگز[آرایه[10]] + فرمت_هگز[آرایه[11]] + فرمت_هگز[آرایه[12]] + فرمت_هگز[آرایه[13]] + فرمت_هگز[آرایه[14]] + فرمت_هگز[آرایه[15]]
  ).toLowerCase();
}

function بستن_امن_سوکت(سوکت) {
  try {
    if (سوکت.readyState === 1 || سوکت.readyState === 2) سوکت.close();
  } catch {}
}
