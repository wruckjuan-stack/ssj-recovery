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
  port: process.env.PORT || 3001,
};

const DB = { history: [], sentMap: {}, cronLog: [], stats: { totalSent: 0, totalDelivered: 0, totalRead: 0, totalFailed: 0, totalCartValue: 0, startedAt: new Date().toISOString() } };

const TEMPLATES = [
  { id: "carrinho_lembrete", name: "carrinho_lembrete", display: "Lembrete Gentil", timing: "30min", minH: 0, maxH: 1.5, lang: "pt_BR", vars: ["primeiro_nome", "link_carrinho"], preview: "Oi {{1}}, tudo bem? Vi que voce estava escolhendo umas pecas lindas aqui na SSJ Moda Fitness e nao finalizou. Seus itens continuam reservados!\n\nVolte quando quiser: {{2}}" },
  { id: "carrinho_apoio", name: "carrinho_apoio", display: "Apoio + Confianca", timing: "2h", minH: 1.5, maxH: 12, lang: "pt_BR", vars: ["primeiro_nome", "link_carrinho"], preview: "Oi {{1}}! Sei que escolher roupa fitness online pode gerar duvidas. Na SSJ, cada peca e pensada para mulheres que valorizam qualidade e conforto de verdade.\n\nSuas pecas continuam esperando: {{2}}" },
  { id: "carrinho_social", name: "carrinho_social", display: "Prova Social", timing: "24h", minH: 12, maxH: 36, lang: "pt_BR", vars: ["primeiro_nome", "link_carrinho"], preview: "{{1}}, as pecas que voce escolheu estao entre as mais amadas pelas nossas clientes!\n\nQue tal garantir as suas? {{2}}" },
  { id: "carrinho_cupom", name: "carrinho_cupom", display: "Urgencia + Cupom", timing: "48h", minH: 36, maxH: 96, lang: "pt_BR", vars: ["primeiro_nome", "cupom", "link_carrinho"], preview: "{{1}}, este e o nosso ultimo chamado!\n\nPreparamos 10% OFF exclusivo para voce: {{2}}\n\nUse o cupom e finalize: {{3}}\n\nValido por 24h!" },
];

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
      alreadySent: DB.sentMap[cart.id] || []
    };
  });
}

async function sendWA(phone, templateName, params) {
  var tpl = TEMPLATES.find(function(t) { return t.name === templateName; });
  if (!tpl) throw new Error("Template nao encontrado");
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

function record(cart, tpl, status, msgId, auto) {
  var entry = { id: Date.now() + "-" + cart.id, cartId: cart.id, contact: cart.name, phone: cart.phone, template: tpl.display, templateId: tpl.id, status: status, sentAt: new Date().toISOString(), cartValue: cart.total, cartValueRaw: cart.totalRaw, waMessageId: msgId, automated: !!auto };
  DB.history.unshift(entry);
  if (DB.history.length > 500) DB.history.length = 500;
  if (!DB.sentMap[cart.id]) DB.sentMap[cart.id] = [];
  if (DB.sentMap[cart.id].indexOf(tpl.id) === -1) DB.sentMap[cart.id].push(tpl.id);
  if (status !== "failed") { DB.stats.totalSent++; DB.stats.totalCartValue += cart.totalRaw || 0; } else { DB.stats.totalFailed++; }
  return entry;
}

cron.schedule("*/10 * * * *", async function() {
  console.log("[AUTO] " + new Date().toISOString() + " Verificando carrinhos...");
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
    console.log("[AUTO] " + sent + " enviado(s), " + skipped + " pulado(s), " + failed + " falha(s)");
  } catch (e) { console.error("[AUTO] Erro: " + e.message); DB.cronLog.unshift({ ts: new Date().toISOString(), error: e.message }); }
});

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

app.get("/api/webhook", function(req, res) { if (req.query["hub.mode"] === "subscribe" && req.query["hub.verify_token"] === "ssj_verify_token") return res.send(req.query["hub.challenge"]); res.sendStatus(403); });
app.post("/api/webhook", function(req, res) {
  (req.body.entry || []).forEach(function(entry) { (entry.changes || []).forEach(function(change) { ((change.value && change.value.statuses) || []).forEach(function(st) {
    var h = DB.history.find(function(e) { return e.waMessageId === st.id; });
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
