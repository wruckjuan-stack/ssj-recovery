require("dotenv").config();
const express = require("express");
const cors = require("cors");
const cron = require("node-cron");

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(__dirname + "/public"));

const CFG = {
  yampiAlias: process.env.YAMPI_ALIAS || "",
  yampiToken: process.env.YAMPI_TOKEN || "",
  yampiSecret: process.env.YAMPI_SECRET || "",
  waToken: process.env.WA_ACCESS_TOKEN || "",
  waPhoneId: process.env.WA_PHONE_NUMBER_ID || "",
  wabaId: process.env.WA_WABA_ID || "",
  waVersion: process.env.WA_API_VERSION || "v22.0",
  coupon: process.env.DEFAULT_COUPON || "VOLTECOMSSJ",
  coupon30: process.env.COUPON_30 || "VOLTESSJ10",
  coupon60: process.env.COUPON_60 || "VOLTESSJ15",
  coupon90: process.env.COUPON_90 || "VOLTESSJ20",
  port: process.env.PORT || 3001,
};

const DB = {
  history: [],
  sentMap: {},
  cronLog: [],
  stats: { totalSent: 0, totalDelivered: 0, totalRead: 0, totalFailed: 0, totalCartValue: 0, startedAt: new Date().toISOString() },
  // PIX recovery
  pixHistory: [],
  pixSentMap: {},
  pixCronLog: [],
  pixStats: { totalSent: 0, totalRecovered: 0, totalFailed: 0 },
  // Repurchase campaigns
  recompraHistory: [],
  recompraSentMap: {},
  recompraCronLog: [],
  recompraStats: { totalSent: 0, totalFailed: 0 },
  // Repurchase campaign settings
  recompraConfig: {
    enabled: true,
    intervals: [
      { days: 30, enabled: true, templateId: "recompra_30dias", coupon: CFG.coupon30 || "VOLTESSJ10" },
      { days: 60, enabled: true, templateId: "recompra_60dias", coupon: CFG.coupon60 || "VOLTESSJ15" },
      { days: 90, enabled: true, templateId: "recompra_90dias", coupon: CFG.coupon90 || "VOLTESSJ20" },
    ]
  }
};

// ===================== TEMPLATES =====================

const TEMPLATES = [
  { id: "carrinho_lembrete", name: "carrinho_lembrete", display: "Lembrete Gentil", timing: "30min", minH: 0, maxH: 1.5, lang: "pt_BR", vars: ["primeiro_nome", "link_carrinho"], preview: "Oi {{1}}, tudo bem? Vi que voce estava escolhendo umas pecas lindas aqui na SSJ Moda Fitness e nao finalizou. Seus itens continuam reservados!\n\nVolte quando quiser: {{2}}" },
  { id: "carrinho_apoio", name: "carrinho_apoio", display: "Apoio + Confianca", timing: "2h", minH: 1.5, maxH: 12, lang: "pt_BR", vars: ["primeiro_nome", "link_carrinho"], preview: "Oi {{1}}! Sei que escolher roupa fitness online pode gerar duvidas. Na SSJ, cada peca e pensada para mulheres que valorizam qualidade e conforto de verdade.\n\nSuas pecas continuam esperando: {{2}}" },
  { id: "carrinho_social", name: "carrinho_social", display: "Prova Social", timing: "24h", minH: 12, maxH: 36, lang: "pt_BR", vars: ["primeiro_nome", "link_carrinho"], preview: "{{1}}, as pecas que voce escolheu estao entre as mais amadas pelas nossas clientes!\n\nQue tal garantir as suas? {{2}}" },
  { id: "carrinho_cupom", name: "carrinho_cupom", display: "Urgencia + Cupom", timing: "48h", minH: 36, maxH: 96, lang: "pt_BR", vars: ["primeiro_nome", "cupom", "link_carrinho"], preview: "{{1}}, este e o nosso ultimo chamado!\n\nPreparamos 10% OFF exclusivo para voce: {{2}}\n\nUse o cupom e finalize: {{3}}\n\nValido por 24h!" },
];

// PIX/Boleto recovery templates
const PIX_TEMPLATES = [
  { id: "pix_expirado", name: "pix_expirado", display: "PIX Expirado", timing: "apos expirar", lang: "pt_BR", vars: ["primeiro_nome", "link_carrinho"], preview: "Oi {{1}}! Vi que seu pagamento via PIX expirou, mas nao se preocupe — seus itens ainda estao disponiveis.\n\nGere um novo PIX rapidinho aqui: {{2}}\n\nQualquer duvida, estamos aqui!" },
  { id: "boleto_expirado", name: "boleto_expirado", display: "Boleto Expirado", timing: "apos expirar", lang: "pt_BR", vars: ["primeiro_nome", "link_carrinho"], preview: "Oi {{1}}! Seu boleto venceu, mas calma — suas pecas favoritas continuam te esperando na SSJ.\n\nClique aqui para gerar um novo pagamento: {{2}}\n\nE se preferir pagar via PIX, e instantaneo!" },
];

// Repurchase campaign templates
const RECOMPRA_TEMPLATES = [
  { id: "recompra_30dias", name: "recompra_30dias", display: "Recompra 30 dias", timing: "30 dias", lang: "pt_BR", vars: ["primeiro_nome", "cupom"], preview: "Oi {{1}}! Ja faz um mes que voce comprou com a gente e esperamos que esteja amando suas pecas!\n\nQue tal renovar o look? Use o cupom {{2}} e ganhe 10% OFF na proxima compra.\n\nAcesse: ssjmodafitness.com.br" },
  { id: "recompra_60dias", name: "recompra_60dias", display: "Recompra 60 dias", timing: "60 dias", lang: "pt_BR", vars: ["primeiro_nome", "cupom"], preview: "{{1}}, sentimos sua falta! Ja faz 2 meses desde sua ultima compra na SSJ.\n\nTemos novidades incriveis e um cupom especial pra voce: {{2}} com 15% OFF!\n\nVem ver: ssjmodafitness.com.br" },
  { id: "recompra_90dias", name: "recompra_90dias", display: "Recompra 90 dias", timing: "90 dias", lang: "pt_BR", vars: ["primeiro_nome", "cupom"], preview: "{{1}}, faz tempo que nao nos vemos! Preparamos algo especial pra voce voltar:\n\n20% OFF com o cupom {{2}}\n\nSao pecas novas, confortaveis e com a qualidade SSJ que voce ja conhece.\n\nAproveite: ssjmodafitness.com.br" },
];

// ===================== YAMPI HELPERS =====================

async function yampiGet(path, params) {
  params = params || {};
  var url = new URL("https://api.dooki.com.br/v2/" + CFG.yampiAlias + path);
  Object.entries(params).forEach(function(e) { url.searchParams.set(e[0], e[1]); });
  var r = await fetch(url, { headers: { "User-Token": CFG.yampiToken, "User-Secret-Key": CFG.yampiSecret, "Content-Type": "application/json" } });
  if (!r.ok) throw new Error("Yampi " + r.status);
  return r.json();
}

function formatPhone(num, ddd) {
  var c = String(num).replace(/\D/g, "");
  if (c.length <= 9) c = (ddd || "41") + c;
  if (c.length <= 11) c = "55" + c;
  return c;
}

// ===================== CART FUNCTIONS (existing) =====================

async function fetchCarts() {
  var data = await yampiGet("/checkout/carts", { include: "customer,items", limit: "50", orderBy: "created_at", sortedBy: "desc" });
  return (data.data || []).map(function(cart) {
    var cust = cart.customer && cart.customer.data ? cart.customer.data : {};
    var ph = (cust.phone && cust.phone.full_number) || (cust.spreadsheet && cust.spreadsheet.data && cust.spreadsheet.data.phone_number) || "";
    var ddd = (cust.phone && cust.phone.area_code) || (cust.spreadsheet && cust.spreadsheet.data && cust.spreadsheet.data.phone_code) || "";
    var created = (cart.created_at && cart.created_at.date) || cart.created_at || null;
    var hoursAgo = created ? Math.round((Date.now() - new Date(created).getTime()) / 3600000) : 0;
    var items = Array.isArray(cart.items && cart.items.data) ? cart.items.data.map(function(i) { return (i.sku && i.sku.data && i.sku.data.title) || i.name || "Produto"; }) : [];
    var rec = TEMPLATES.find(function(t) { return hoursAgo >= t.minH && hoursAgo < t.maxH; });
    var lastTxStatus = cart.last_transaction_status || cart.transaction_status || null;
    return {
      id: cart.id, name: cust.name || cust.first_name || "Cliente",
      firstName: cust.first_name || (cust.name || "").split(" ")[0] || "Cliente",
      email: cust.email || "", phone: formatPhone(ph, ddd),
      total: (cart.totalizers && cart.totalizers.total_formated) || "R$ " + ((cart.totalizers && cart.totalizers.total) || 0).toFixed(2),
      totalRaw: (cart.totalizers && cart.totalizers.total) || 0,
      items: items.join(", ") || "Itens no carrinho", itemCount: items.length,
      simUrl: cart.simulate_url || cart.unauth_simulate_url || "",
      hoursAgo: hoursAgo, createdAt: created,
      recommended: rec ? rec.id : TEMPLATES[3].id,
      alreadySent: DB.sentMap[cart.id] || [],
      lastTxStatus: lastTxStatus
    };
  });
}

// ===================== ORDERS FUNCTIONS (new) =====================

async function fetchOrders(params) {
  params = params || {};
  params.include = "customer";
  params.limit = params.limit || "50";
  params.orderBy = params.orderBy || "created_at";
  params.sortedBy = params.sortedBy || "desc";
  var data = await yampiGet("/orders", params);
  return (data.data || []).map(function(order) {
    var cust = order.customer && order.customer.data ? order.customer.data : {};
    var ph = (cust.phone && cust.phone.full_number) || (cust.spreadsheet && cust.spreadsheet.data && cust.spreadsheet.data.phone_number) || "";
    var ddd = (cust.phone && cust.phone.area_code) || (cust.spreadsheet && cust.spreadsheet.data && cust.spreadsheet.data.phone_code) || "";
    var created = (order.created_at && order.created_at.date) || order.created_at || null;
    var daysAgo = created ? Math.round((Date.now() - new Date(created).getTime()) / 86400000) : 0;
    return {
      id: order.id,
      number: order.number || order.id,
      name: cust.name || cust.first_name || "Cliente",
      firstName: cust.first_name || (cust.name || "").split(" ")[0] || "Cliente",
      email: cust.email || "",
      phone: formatPhone(ph, ddd),
      total: order.value_total_formated || "R$ " + (order.value_total || 0).toFixed(2),
      totalRaw: order.value_total || 0,
      status: order.status && order.status.data ? order.status.data.alias : (order.status_alias || ""),
      statusLabel: order.status && order.status.data ? order.status.data.name : (order.status_label || ""),
      createdAt: created,
      daysAgo: daysAgo,
      customerId: cust.id || null
    };
  });
}

// ===================== WHATSAPP SEND =====================

async function sendWA(phone, templateName, params, allTemplates) {
  var searchIn = allTemplates || TEMPLATES;
  var tpl = searchIn.find(function(t) { return t.name === templateName; });
  if (!tpl) throw new Error("Template nao encontrado: " + templateName);
  var r = await fetch("https://graph.facebook.com/" + CFG.waVersion + "/" + CFG.waPhoneId + "/messages", {
    method: "POST",
    headers: { Authorization: "Bearer " + CFG.waToken, "Content-Type": "application/json" },
    body: JSON.stringify({ messaging_product: "whatsapp", to: phone, type: "template", template: { name: tpl.name, language: { code: tpl.lang }, components: [{ type: "body", parameters: params.map(function(p) { return { type: "text", text: String(p) }; }) }] } })
  });
  var data = await r.json();
  if (!r.ok) throw new Error((data.error && data.error.message) || "WA " + r.status);
  return (data.messages && data.messages[0] && data.messages[0].id) || null;
}

function buildParams(tpl, cart) {
  return tpl.vars.map(function(v) {
    if (v === "primeiro_nome") return cart.firstName;
    if (v === "link_carrinho") return cart.simUrl;
    if (v === "cupom") return CFG.coupon;
    return "";
  });
}

function buildPixParams(tpl, cart) {
  return tpl.vars.map(function(v) {
    if (v === "primeiro_nome") return cart.firstName;
    if (v === "link_carrinho") return cart.simUrl;
    return "";
  });
}

function buildRecompraParams(tpl, order, couponCode) {
  return tpl.vars.map(function(v) {
    if (v === "primeiro_nome") return order.firstName;
    if (v === "cupom") return couponCode;
    return "";
  });
}

// ===================== RECORD FUNCTIONS =====================

function record(cart, tpl, status, msgId, auto) {
  var entry = { id: Date.now() + "-" + cart.id, cartId: cart.id, contact: cart.name, phone: cart.phone, template: tpl.display, templateId: tpl.id, status: status, sentAt: new Date().toISOString(), cartValue: cart.total, cartValueRaw: cart.totalRaw, waMessageId: msgId, automated: !!auto, type: "carrinho" };
  DB.history.unshift(entry);
  if (DB.history.length > 500) DB.history.length = 500;
  if (!DB.sentMap[cart.id]) DB.sentMap[cart.id] = [];
  if (DB.sentMap[cart.id].indexOf(tpl.id) === -1) DB.sentMap[cart.id].push(tpl.id);
  if (status !== "failed") { DB.stats.totalSent++; DB.stats.totalCartValue += cart.totalRaw || 0; } else { DB.stats.totalFailed++; }
  return entry;
}

function recordPix(cart, tpl, status, msgId) {
  var entry = { id: Date.now() + "-pix-" + cart.id, cartId: cart.id, contact: cart.name, phone: cart.phone, template: tpl.display, templateId: tpl.id, status: status, sentAt: new Date().toISOString(), cartValue: cart.total, waMessageId: msgId, type: "pix" };
  DB.pixHistory.unshift(entry);
  if (DB.pixHistory.length > 500) DB.pixHistory.length = 500;
  if (!DB.pixSentMap[cart.id]) DB.pixSentMap[cart.id] = [];
  if (DB.pixSentMap[cart.id].indexOf(tpl.id) === -1) DB.pixSentMap[cart.id].push(tpl.id);
  if (status !== "failed") DB.pixStats.totalSent++; else DB.pixStats.totalFailed++;
  return entry;
}

function recordRecompra(order, tpl, status, msgId, intervalDays) {
  var entry = { id: Date.now() + "-recompra-" + order.id, orderId: order.id, contact: order.name, phone: order.phone, template: tpl.display, templateId: tpl.id, status: status, sentAt: new Date().toISOString(), orderValue: order.total, waMessageId: msgId, intervalDays: intervalDays, type: "recompra" };
  DB.recompraHistory.unshift(entry);
  if (DB.recompraHistory.length > 500) DB.recompraHistory.length = 500;
  var key = order.id + "-" + intervalDays;
  if (!DB.recompraSentMap[key]) DB.recompraSentMap[key] = true;
  if (status !== "failed") DB.recompraStats.totalSent++; else DB.recompraStats.totalFailed++;
  return entry;
}

// ===================== CRON: CART RECOVERY (existing) =====================

cron.schedule("*/10 * * * *", async function() {
  console.log("[AUTO-CARRINHO] " + new Date().toISOString() + " Verificando carrinhos...");
  try {
    var carts = await fetchCarts();
    var sent = 0, skipped = 0, failed = 0;
    for (var i = 0; i < carts.length; i++) {
      var cart = carts[i];
      if (!cart.phone || cart.phone.length < 12) { skipped++; continue; }
      var tpl = TEMPLATES.find(function(t) { return t.id === cart.recommended; });
      if (!tpl) { skipped++; continue; }
      if (DB.sentMap[cart.id] && DB.sentMap[cart.id].indexOf(tpl.id) !== -1) { skipped++; continue; }
      try {
        var msgId = await sendWA(cart.phone, tpl.name, buildParams(tpl, cart));
        record(cart, tpl, "sent", msgId, true);
        sent++;
        await new Promise(function(r) { setTimeout(r, 250); });
      } catch (e) { record(cart, tpl, "failed", null, true); failed++; }
    }
    DB.cronLog.unshift({ ts: new Date().toISOString(), cartsFound: carts.length, sent: sent, skipped: skipped, failed: failed });
    if (DB.cronLog.length > 100) DB.cronLog.length = 100;
    console.log("[AUTO-CARRINHO] " + sent + " enviado(s), " + skipped + " pulado(s), " + failed + " falha(s)");
  } catch (e) { console.error("[AUTO-CARRINHO] Erro: " + e.message); DB.cronLog.unshift({ ts: new Date().toISOString(), error: e.message }); }
});

// ===================== CRON: PIX/BOLETO RECOVERY (new) =====================

cron.schedule("*/15 * * * *", async function() {
  console.log("[AUTO-PIX] " + new Date().toISOString() + " Verificando PIX/boleto expirados...");
  try {
    var carts = await fetchCarts();
    var sent = 0, skipped = 0, failed = 0;

    for (var i = 0; i < carts.length; i++) {
      var cart = carts[i];
      if (!cart.phone || cart.phone.length < 12) { skipped++; continue; }

      var txStatus = (cart.lastTxStatus || "").toLowerCase();

      // Check if PIX or boleto expired/refused
      var isPix = txStatus === "pix_expired" || txStatus === "expired" || txStatus === "timeout";
      var isBoleto = txStatus === "boleto_expired" || txStatus === "refused" || txStatus === "waiting_payment";

      // Also check for common Yampi status patterns
      if (!isPix && !isBoleto) {
        if (txStatus.indexOf("pix") !== -1 && (txStatus.indexOf("expir") !== -1 || txStatus.indexOf("cancel") !== -1)) isPix = true;
        if (txStatus.indexOf("boleto") !== -1 && (txStatus.indexOf("expir") !== -1 || txStatus.indexOf("venc") !== -1)) isBoleto = true;
      }

      if (!isPix && !isBoleto) { skipped++; continue; }

      var tpl = isPix ? PIX_TEMPLATES[0] : PIX_TEMPLATES[1];

      // Already sent for this cart?
      if (DB.pixSentMap[cart.id] && DB.pixSentMap[cart.id].indexOf(tpl.id) !== -1) { skipped++; continue; }

      try {
        var allTpls = TEMPLATES.concat(PIX_TEMPLATES).concat(RECOMPRA_TEMPLATES);
        var msgId = await sendWA(cart.phone, tpl.name, buildPixParams(tpl, cart), allTpls);
        recordPix(cart, tpl, "sent", msgId);
        sent++;
        await new Promise(function(r) { setTimeout(r, 250); });
      } catch (e) {
        recordPix(cart, tpl, "failed", null);
        failed++;
      }
    }

    DB.pixCronLog.unshift({ ts: new Date().toISOString(), cartsChecked: carts.length, sent: sent, skipped: skipped, failed: failed });
    if (DB.pixCronLog.length > 100) DB.pixCronLog.length = 100;
    console.log("[AUTO-PIX] " + sent + " enviado(s), " + skipped + " pulado(s), " + failed + " falha(s)");
  } catch (e) {
    console.error("[AUTO-PIX] Erro: " + e.message);
    DB.pixCronLog.unshift({ ts: new Date().toISOString(), error: e.message });
  }
});

// ===================== CRON: REPURCHASE CAMPAIGNS (new) =====================

cron.schedule("0 10 * * *", async function() {
  // Runs once daily at 10:00 AM
  console.log("[AUTO-RECOMPRA] " + new Date().toISOString() + " Verificando campanhas de recompra...");
  if (!DB.recompraConfig.enabled) {
    console.log("[AUTO-RECOMPRA] Desabilitado nas configuracoes.");
    DB.recompraCronLog.unshift({ ts: new Date().toISOString(), disabled: true });
    return;
  }

  try {
    var intervals = DB.recompraConfig.intervals.filter(function(iv) { return iv.enabled; });
    var totalSent = 0, totalSkipped = 0, totalFailed = 0;

    for (var k = 0; k < intervals.length; k++) {
      var iv = intervals[k];
      var tpl = RECOMPRA_TEMPLATES.find(function(t) { return t.id === iv.templateId; });
      if (!tpl) continue;

      // Calculate target date range (tolerance of 1 day)
      var targetDate = new Date();
      targetDate.setDate(targetDate.getDate() - iv.days);
      var fromDate = new Date(targetDate);
      fromDate.setDate(fromDate.getDate() - 1);
      var toDate = new Date(targetDate);
      toDate.setDate(toDate.getDate() + 1);

      var fromStr = fromDate.toISOString().slice(0, 10);
      var toStr = toDate.toISOString().slice(0, 10);

      try {
        var orders = await fetchOrders({ "q[created_at][from]": fromStr, "q[created_at][to]": toStr });

        // Filter only paid/completed orders
        var paidOrders = orders.filter(function(o) {
          var s = (o.status || "").toLowerCase();
          return s === "paid" || s === "invoiced" || s === "shipped" || s === "delivered" || s === "complete" || s === "completed" || s === "pago" || s === "enviado" || s === "entregue";
        });

        for (var j = 0; j < paidOrders.length; j++) {
          var order = paidOrders[j];
          if (!order.phone || order.phone.length < 12) { totalSkipped++; continue; }

          var key = order.id + "-" + iv.days;
          if (DB.recompraSentMap[key]) { totalSkipped++; continue; }

          try {
            var allTpls = TEMPLATES.concat(PIX_TEMPLATES).concat(RECOMPRA_TEMPLATES);
            var coupon = iv.coupon || CFG.coupon;
            var msgId = await sendWA(order.phone, tpl.name, buildRecompraParams(tpl, order, coupon), allTpls);
            recordRecompra(order, tpl, "sent", msgId, iv.days);
            totalSent++;
            await new Promise(function(r) { setTimeout(r, 300); });
          } catch (e) {
            recordRecompra(order, tpl, "failed", null, iv.days);
            totalFailed++;
          }
        }
      } catch (e) {
        console.error("[AUTO-RECOMPRA] Erro ao buscar pedidos " + iv.days + "d: " + e.message);
      }
    }

    DB.recompraCronLog.unshift({ ts: new Date().toISOString(), sent: totalSent, skipped: totalSkipped, failed: totalFailed });
    if (DB.recompraCronLog.length > 100) DB.recompraCronLog.length = 100;
    console.log("[AUTO-RECOMPRA] " + totalSent + " enviado(s), " + totalSkipped + " pulado(s), " + totalFailed + " falha(s)");
  } catch (e) {
    console.error("[AUTO-RECOMPRA] Erro: " + e.message);
    DB.recompraCronLog.unshift({ ts: new Date().toISOString(), error: e.message });
  }
});

// ===================== API ROUTES: EXISTING =====================

app.get("/api/health", async function(req, res) {
  var yOk = false, wOk = false;
  try { await yampiGet("/catalog/products", { limit: "1" }); yOk = true; } catch (e) {}
  try { var r = await fetch("https://graph.facebook.com/" + CFG.waVersion + "/" + CFG.waPhoneId, { headers: { Authorization: "Bearer " + CFG.waToken } }); wOk = r.ok; } catch (e) {}
  res.json({ yampi: { ok: yOk, alias: CFG.yampiAlias }, whatsapp: { ok: wOk, phoneId: CFG.waPhoneId }, uptime: process.uptime(), stats: DB.stats });
});

app.get("/api/carts", async function(req, res) {
  try { var carts = await fetchCarts(); res.json({ ok: true, count: carts.length, totalAbandoned: carts.reduce(function(s, c) { return s + c.totalRaw; }, 0), data: carts }); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get("/api/templates", function(req, res) { res.json({ data: TEMPLATES }); });

app.post("/api/send", async function(req, res) {
  var cartIds = req.body.cartIds, templateId = req.body.templateId;
  if (!cartIds || !cartIds.length) return res.status(400).json({ error: "cartIds obrigatorio" });
  var tpl = TEMPLATES.find(function(t) { return t.id === templateId; });
  if (!tpl) return res.status(400).json({ error: "Template nao encontrado" });
  var carts; try { carts = await fetchCarts(); } catch (e) { return res.status(500).json({ error: e.message }); }
  var results = [];
  for (var i = 0; i < cartIds.length; i++) {
    var cart = carts.find(function(c) { return c.id === cartIds[i]; });
    if (!cart) { results.push({ cartId: cartIds[i], ok: false, error: "Nao encontrado" }); continue; }
    if (!cart.phone) { results.push({ cartId: cartIds[i], ok: false, error: "Sem telefone" }); continue; }
    if (DB.sentMap[cartIds[i]] && DB.sentMap[cartIds[i]].indexOf(templateId) !== -1) { results.push({ cartId: cartIds[i], ok: false, error: "Ja enviado" }); continue; }
    try { var msgId = await sendWA(cart.phone, tpl.name, buildParams(tpl, cart)); record(cart, tpl, "sent", msgId, false); results.push({ cartId: cartIds[i], ok: true, contact: cart.name }); }
    catch (e) { record(cart, tpl, "failed", null, false); results.push({ cartId: cartIds[i], ok: false, error: e.message }); }
  }
  res.json({ ok: true, sent: results.filter(function(r) { return r.ok; }).length, failed: results.filter(function(r) { return !r.ok; }).length, results: results });
});

app.get("/api/history", function(req, res) { res.json({ data: DB.history.slice(0, 50), total: DB.history.length }); });

app.get("/api/stats", function(req, res) {
  var today = new Date().toISOString().slice(0, 10);
  var todayH = DB.history.filter(function(e) { return e.sentAt.startsWith(today); });
  var byTpl = {};
  TEMPLATES.forEach(function(t) { var items = DB.history.filter(function(e) { return e.templateId === t.id; }); byTpl[t.id] = { display: t.display, total: items.length, delivered: items.filter(function(e) { return e.status === "delivered" || e.status === "read"; }).length, read: items.filter(function(e) { return e.status === "read"; }).length, failed: items.filter(function(e) { return e.status === "failed"; }).length }; });
  res.json({ global: DB.stats, today: { sent: todayH.length, delivered: todayH.filter(function(e) { return e.status === "delivered" || e.status === "read"; }).length, read: todayH.filter(function(e) { return e.status === "read"; }).length, failed: todayH.filter(function(e) { return e.status === "failed"; }).length, automated: todayH.filter(function(e) { return e.automated; }).length }, byTemplate: byTpl, cronLog: DB.cronLog.slice(0, 10) });
});

// ===================== API ROUTES: PIX/BOLETO (new) =====================

app.get("/api/pix/carts", async function(req, res) {
  try {
    var carts = await fetchCarts();
    var pixCarts = carts.filter(function(c) {
      var tx = (c.lastTxStatus || "").toLowerCase();
      return tx.indexOf("expir") !== -1 || tx.indexOf("timeout") !== -1 || tx.indexOf("refused") !== -1 || tx === "waiting_payment" || tx.indexOf("pix") !== -1 || tx.indexOf("boleto") !== -1;
    }).map(function(c) {
      var tx = (c.lastTxStatus || "").toLowerCase();
      c.paymentType = tx.indexOf("pix") !== -1 ? "PIX" : tx.indexOf("boleto") !== -1 ? "Boleto" : "PIX/Boleto";
      c.pixAlreadySent = DB.pixSentMap[c.id] || [];
      return c;
    });
    res.json({ ok: true, count: pixCarts.length, data: pixCarts });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get("/api/pix/templates", function(req, res) { res.json({ data: PIX_TEMPLATES }); });

app.post("/api/pix/send", async function(req, res) {
  var cartIds = req.body.cartIds, templateId = req.body.templateId;
  if (!cartIds || !cartIds.length) return res.status(400).json({ error: "cartIds obrigatorio" });
  var allTpls = TEMPLATES.concat(PIX_TEMPLATES).concat(RECOMPRA_TEMPLATES);
  var tpl = allTpls.find(function(t) { return t.id === templateId; });
  if (!tpl) return res.status(400).json({ error: "Template nao encontrado" });
  var carts; try { carts = await fetchCarts(); } catch (e) { return res.status(500).json({ error: e.message }); }
  var results = [];
  for (var i = 0; i < cartIds.length; i++) {
    var cart = carts.find(function(c) { return c.id === cartIds[i]; });
    if (!cart) { results.push({ cartId: cartIds[i], ok: false, error: "Nao encontrado" }); continue; }
    if (!cart.phone) { results.push({ cartId: cartIds[i], ok: false, error: "Sem telefone" }); continue; }
    if (DB.pixSentMap[cartIds[i]] && DB.pixSentMap[cartIds[i]].indexOf(templateId) !== -1) { results.push({ cartId: cartIds[i], ok: false, error: "Ja enviado" }); continue; }
    try {
      var msgId = await sendWA(cart.phone, tpl.name, buildPixParams(tpl, cart), allTpls);
      recordPix(cart, tpl, "sent", msgId);
      results.push({ cartId: cartIds[i], ok: true, contact: cart.name });
    } catch (e) { recordPix(cart, tpl, "failed", null); results.push({ cartId: cartIds[i], ok: false, error: e.message }); }
  }
  res.json({ ok: true, sent: results.filter(function(r) { return r.ok; }).length, failed: results.filter(function(r) { return !r.ok; }).length, results: results });
});

app.get("/api/pix/history", function(req, res) { res.json({ data: DB.pixHistory.slice(0, 50), total: DB.pixHistory.length }); });

app.get("/api/pix/stats", function(req, res) {
  res.json({
    stats: DB.pixStats,
    cronLog: DB.pixCronLog.slice(0, 10),
    byTemplate: PIX_TEMPLATES.map(function(t) {
      var items = DB.pixHistory.filter(function(e) { return e.templateId === t.id; });
      return { display: t.display, total: items.length, sent: items.filter(function(e) { return e.status === "sent"; }).length, failed: items.filter(function(e) { return e.status === "failed"; }).length };
    })
  });
});

// ===================== API ROUTES: REPURCHASE (new) =====================

app.get("/api/recompra/orders", async function(req, res) {
  try {
    var allOrders = [];
    var intervals = DB.recompraConfig.intervals;

    for (var k = 0; k < intervals.length; k++) {
      var iv = intervals[k];
      var targetDate = new Date();
      targetDate.setDate(targetDate.getDate() - iv.days);
      var fromDate = new Date(targetDate); fromDate.setDate(fromDate.getDate() - 2);
      var toDate = new Date(targetDate); toDate.setDate(toDate.getDate() + 2);

      var orders = await fetchOrders({ "q[created_at][from]": fromDate.toISOString().slice(0, 10), "q[created_at][to]": toDate.toISOString().slice(0, 10) });
      var paidOrders = orders.filter(function(o) {
        var s = (o.status || "").toLowerCase();
        return s === "paid" || s === "invoiced" || s === "shipped" || s === "delivered" || s === "complete" || s === "completed" || s === "pago" || s === "enviado" || s === "entregue";
      });

      paidOrders.forEach(function(o) {
        o.intervalDays = iv.days;
        o.intervalTemplate = iv.templateId;
        o.intervalCoupon = iv.coupon;
        var key = o.id + "-" + iv.days;
        o.alreadySent = !!DB.recompraSentMap[key];
        allOrders.push(o);
      });
    }

    res.json({ ok: true, count: allOrders.length, data: allOrders });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get("/api/recompra/templates", function(req, res) { res.json({ data: RECOMPRA_TEMPLATES }); });

app.get("/api/recompra/config", function(req, res) { res.json({ data: DB.recompraConfig }); });

app.post("/api/recompra/config", function(req, res) {
  if (req.body.enabled !== undefined) DB.recompraConfig.enabled = !!req.body.enabled;
  if (req.body.intervals && Array.isArray(req.body.intervals)) {
    req.body.intervals.forEach(function(iv) {
      var existing = DB.recompraConfig.intervals.find(function(e) { return e.days === iv.days; });
      if (existing) {
        if (iv.enabled !== undefined) existing.enabled = !!iv.enabled;
        if (iv.coupon) existing.coupon = iv.coupon;
        if (iv.templateId) existing.templateId = iv.templateId;
      }
    });
  }
  res.json({ ok: true, data: DB.recompraConfig });
});

app.post("/api/recompra/send", async function(req, res) {
  var orderIds = req.body.orderIds, templateId = req.body.templateId, coupon = req.body.coupon, intervalDays = req.body.intervalDays || 30;
  if (!orderIds || !orderIds.length) return res.status(400).json({ error: "orderIds obrigatorio" });
  var allTpls = TEMPLATES.concat(PIX_TEMPLATES).concat(RECOMPRA_TEMPLATES);
  var tpl = allTpls.find(function(t) { return t.id === templateId; });
  if (!tpl) return res.status(400).json({ error: "Template nao encontrado" });

  // Fetch orders to get phone numbers
  var results = [];
  try {
    var targetDate = new Date();
    targetDate.setDate(targetDate.getDate() - intervalDays);
    var fromDate = new Date(targetDate); fromDate.setDate(fromDate.getDate() - 5);
    var toDate = new Date(targetDate); toDate.setDate(toDate.getDate() + 5);
    var orders = await fetchOrders({ "q[created_at][from]": fromDate.toISOString().slice(0, 10), "q[created_at][to]": toDate.toISOString().slice(0, 10) });

    for (var i = 0; i < orderIds.length; i++) {
      var order = orders.find(function(o) { return o.id === orderIds[i]; });
      if (!order) { results.push({ orderId: orderIds[i], ok: false, error: "Nao encontrado" }); continue; }
      if (!order.phone) { results.push({ orderId: orderIds[i], ok: false, error: "Sem telefone" }); continue; }
      var key = order.id + "-" + intervalDays;
      if (DB.recompraSentMap[key]) { results.push({ orderId: orderIds[i], ok: false, error: "Ja enviado" }); continue; }
      try {
        var msgId = await sendWA(order.phone, tpl.name, buildRecompraParams(tpl, order, coupon || CFG.coupon), allTpls);
        recordRecompra(order, tpl, "sent", msgId, intervalDays);
        results.push({ orderId: orderIds[i], ok: true, contact: order.name });
      } catch (e) { recordRecompra(order, tpl, "failed", null, intervalDays); results.push({ orderId: orderIds[i], ok: false, error: e.message }); }
    }
  } catch (e) { return res.status(500).json({ ok: false, error: e.message }); }

  res.json({ ok: true, sent: results.filter(function(r) { return r.ok; }).length, failed: results.filter(function(r) { return !r.ok; }).length, results: results });
});

app.get("/api/recompra/history", function(req, res) { res.json({ data: DB.recompraHistory.slice(0, 50), total: DB.recompraHistory.length }); });

app.get("/api/recompra/stats", function(req, res) {
  res.json({
    stats: DB.recompraStats,
    config: DB.recompraConfig,
    cronLog: DB.recompraCronLog.slice(0, 10),
    byTemplate: RECOMPRA_TEMPLATES.map(function(t) {
      var items = DB.recompraHistory.filter(function(e) { return e.templateId === t.id; });
      return { display: t.display, total: items.length, sent: items.filter(function(e) { return e.status === "sent"; }).length, failed: items.filter(function(e) { return e.status === "failed"; }).length };
    })
  });
});

// ===================== WEBHOOKS & WA TEMPLATES (existing) =====================

app.get("/api/webhook", function(req, res) { if (req.query["hub.mode"] === "subscribe" && req.query["hub.verify_token"] === "ssj_verify_token") return res.send(req.query["hub.challenge"]); res.sendStatus(403); });
app.post("/api/webhook", function(req, res) {
  (req.body.entry || []).forEach(function(entry) { (entry.changes || []).forEach(function(change) { ((change.value && change.value.statuses) || []).forEach(function(st) {
    var h = DB.history.find(function(e) { return e.waMessageId === st.id; });
    if (!h) h = DB.pixHistory.find(function(e) { return e.waMessageId === st.id; });
    if (!h) h = DB.recompraHistory.find(function(e) { return e.waMessageId === st.id; });
    if (h) { var order = ["pending","sent","delivered","read","failed"]; if (order.indexOf(st.status) > order.indexOf(h.status)) { h.status = st.status; if (st.status === "delivered") DB.stats.totalDelivered++; if (st.status === "read") DB.stats.totalRead++; } }
  }); }); });
  res.sendStatus(200);
});

app.get("/api/wa-templates", async function(req, res) {
  try {
    var r = await fetch("https://graph.facebook.com/" + CFG.waVersion + "/" + CFG.wabaId + "/message_templates?limit=20", {
      headers: { Authorization: "Bearer " + CFG.waToken }
    });
    var data = await r.json();
    if (!r.ok) throw new Error((data.error && data.error.message) || "Erro " + r.status);
    res.json({ ok: true, data: data.data || [] });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.post("/api/wa-templates", async function(req, res) {
  try {
    var body = {
      name: req.body.name,
      language: req.body.language || "pt_BR",
      category: req.body.category || "MARKETING",
      components: [
        { type: "BODY", text: req.body.bodyText }
      ]
    };
    if (req.body.footerText) {
      body.components.push({ type: "FOOTER", text: req.body.footerText });
    }
    var r = await fetch("https://graph.facebook.com/" + CFG.waVersion + "/" + CFG.wabaId + "/message_templates", {
      method: "POST",
      headers: { Authorization: "Bearer " + CFG.waToken, "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });
    var data = await r.json();
    if (!r.ok) throw new Error((data.error && data.error.message) || "Erro " + r.status);
    res.json({ ok: true, id: data.id, status: data.status, name: req.body.name });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

app.delete("/api/wa-templates/:name", async function(req, res) {
  try {
    var r = await fetch("https://graph.facebook.com/" + CFG.waVersion + "/" + CFG.wabaId + "/message_templates?name=" + req.params.name, {
      method: "DELETE",
      headers: { Authorization: "Bearer " + CFG.waToken }
    });
    var data = await r.json();
    res.json({ ok: data.success || false });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.listen(CFG.port, function() { console.log("SSJ Recovery rodando na porta " + CFG.port); });
