require("dotenv").config();
const express = require("express");
const cors = require("cors");
const cron = require("node-cron");
const { Pool } = require("pg");

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(__dirname + "/public"));

// Health check for Railway
app.get("/healthz", function(req, res) { res.send("ok"); });

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

// ===================== POSTGRESQL =====================

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL && process.env.DATABASE_URL.includes("railway")
    ? { rejectUnauthorized: false }
    : false,
  max: 3,              // máximo 3 conexões (economiza RAM no trial)
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

async function initDB() {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS sent_messages (
        id SERIAL PRIMARY KEY,
        cart_id TEXT NOT NULL,
        template_id TEXT NOT NULL,
        phone TEXT,
        contact_name TEXT,
        cart_value NUMERIC DEFAULT 0,
        wa_message_id TEXT,
        status TEXT DEFAULT 'sent',
        automated BOOLEAN DEFAULT false,
        msg_type TEXT DEFAULT 'carrinho',
        sent_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(cart_id, template_id)
      );

      CREATE TABLE IF NOT EXISTS pix_sent (
        id SERIAL PRIMARY KEY,
        cart_id TEXT NOT NULL,
        template_id TEXT NOT NULL,
        phone TEXT,
        contact_name TEXT,
        cart_value TEXT,
        wa_message_id TEXT,
        status TEXT DEFAULT 'sent',
        sent_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(cart_id, template_id)
      );

      CREATE TABLE IF NOT EXISTS recompra_sent (
        id SERIAL PRIMARY KEY,
        order_id TEXT NOT NULL,
        interval_days INT NOT NULL,
        template_id TEXT NOT NULL,
        phone TEXT,
        contact_name TEXT,
        order_value TEXT,
        wa_message_id TEXT,
        status TEXT DEFAULT 'sent',
        sent_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(order_id, interval_days)
      );

      CREATE TABLE IF NOT EXISTS conversations (
        phone TEXT PRIMARY KEY,
        name TEXT,
        unread INT DEFAULT 0,
        last_message_at TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS messages (
        id SERIAL PRIMARY KEY,
        phone TEXT NOT NULL,
        wa_message_id TEXT,
        direction TEXT NOT NULL,
        text TEXT,
        msg_type TEXT DEFAULT 'text',
        template TEXT,
        status TEXT DEFAULT 'sent',
        created_at TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_sent_cart ON sent_messages(cart_id);
      CREATE INDEX IF NOT EXISTS idx_sent_wa ON sent_messages(wa_message_id);
      CREATE INDEX IF NOT EXISTS idx_pix_cart ON pix_sent(cart_id);
      CREATE INDEX IF NOT EXISTS idx_pix_wa ON pix_sent(wa_message_id);
      CREATE INDEX IF NOT EXISTS idx_recompra_order ON recompra_sent(order_id);
      CREATE INDEX IF NOT EXISTS idx_recompra_wa ON recompra_sent(wa_message_id);
      CREATE INDEX IF NOT EXISTS idx_messages_phone ON messages(phone);
      CREATE INDEX IF NOT EXISTS idx_messages_wa ON messages(wa_message_id);
    `);
    console.log("[DB] PostgreSQL inicializado com sucesso");
  } catch (e) {
    console.error("[DB] Erro ao inicializar PostgreSQL:", e.message);
  } finally {
    client.release();
  }
}

// ===================== DB HELPERS =====================

// Batch: buscar todos os envios de uma vez (1 query em vez de 50)
async function getAllSentMap() {
  try {
    var r = await pool.query("SELECT cart_id, template_id FROM sent_messages");
    var map = {};
    r.rows.forEach(function(row) {
      if (!map[row.cart_id]) map[row.cart_id] = [];
      map[row.cart_id].push(row.template_id);
    });
    return map;
  } catch (e) { return {}; }
}

async function getAllPixSentMap() {
  try {
    var r = await pool.query("SELECT cart_id, template_id FROM pix_sent");
    var map = {};
    r.rows.forEach(function(row) {
      if (!map[row.cart_id]) map[row.cart_id] = [];
      map[row.cart_id].push(row.template_id);
    });
    return map;
  } catch (e) { return {}; }
}

async function getAllRecompraSentMap() {
  try {
    var r = await pool.query("SELECT order_id, interval_days FROM recompra_sent");
    var map = {};
    r.rows.forEach(function(row) {
      map[row.order_id + "-" + row.interval_days] = true;
    });
    return map;
  } catch (e) { return {}; }
}

async function wasSent(cartId, templateId) {
  try {
    var r = await pool.query("SELECT 1 FROM sent_messages WHERE cart_id=$1 AND template_id=$2", [String(cartId), templateId]);
    return r.rowCount > 0;
  } catch (e) { return false; }
}

async function getSentTemplates(cartId) {
  try {
    var r = await pool.query("SELECT template_id FROM sent_messages WHERE cart_id=$1", [String(cartId)]);
    return r.rows.map(function(row) { return row.template_id; });
  } catch (e) { return []; }
}

async function wasPixSent(cartId, templateId) {
  try {
    var r = await pool.query("SELECT 1 FROM pix_sent WHERE cart_id=$1 AND template_id=$2", [String(cartId), templateId]);
    return r.rowCount > 0;
  } catch (e) { return false; }
}

async function getPixSentTemplates(cartId) {
  try {
    var r = await pool.query("SELECT template_id FROM pix_sent WHERE cart_id=$1", [String(cartId)]);
    return r.rows.map(function(row) { return row.template_id; });
  } catch (e) { return []; }
}

async function wasRecompraSent(orderId, intervalDays) {
  try {
    var r = await pool.query("SELECT 1 FROM recompra_sent WHERE order_id=$1 AND interval_days=$2", [String(orderId), intervalDays]);
    return r.rowCount > 0;
  } catch (e) { return false; }
}

// ===================== IN-MEMORY STATS & LOGS (non-critical, ok to lose) =====================

var STATS = {
  totalSent: 0, totalDelivered: 0, totalRead: 0, totalFailed: 0, totalCartValue: 0,
  startedAt: new Date().toISOString()
};
var cronLog = [];
var pixStats = { totalSent: 0, totalRecovered: 0, totalFailed: 0 };
var pixCronLog = [];
var recompraStats = { totalSent: 0, totalFailed: 0 };
var recompraCronLog = [];

// Repurchase campaign settings (in-memory, could be persisted later)
var recompraConfig = {
  enabled: true,
  intervals: [
    { days: 30, enabled: true, templateId: "recompra_30dias", coupon: CFG.coupon30 || "VOLTESSJ10" },
    { days: 60, enabled: true, templateId: "recompra_60dias", coupon: CFG.coupon60 || "VOLTESSJ15" },
    { days: 90, enabled: true, templateId: "recompra_90dias", coupon: CFG.coupon90 || "VOLTESSJ20" },
  ]
};

// Template metadata (in-memory)
var templateMeta = {};

// ===================== TEMPLATES =====================

// CARRINHO — todos v2: body={{1}}(nome) + botão URL dinâmica
const TEMPLATES = [
  { id: "lembrete_15min_v2", name: "lembrete_15min_v2", display: "Lembrete 15min", timing: "15min", minH: 0, maxH: 0.5, lang: "pt_BR", vars: ["primeiro_nome"], hasButton: true, buttonText: "Ver minhas pecas", preview: "Oiii, tudo bem {{1}}? ... Notei que voce estava escolhendo algumas pecas..." },
  { id: "confianca_2h_v2", name: "confianca_2h_v2", display: "Confianca 2h", timing: "2h", minH: 0.5, maxH: 12, lang: "pt_BR", vars: ["primeiro_nome"], hasButton: true, buttonText: "Garantir minhas pecas", preview: "Ola, aqui e a equipe SSJ Moda Fitness, {{1}} ..." },
  { id: "social_24h_v2", name: "social_24h_v2", display: "Social 24h", timing: "24h", minH: 12, maxH: 36, lang: "pt_BR", vars: ["primeiro_nome"], hasButton: true, buttonText: "Aproveitar agora", preview: "Ola, tudo bem {{1}}? Aqui e a Jessica da SSJ ..." },
  { id: "cupom_48h_v2", name: "cupom_48h_v2", display: "Cupom 48h", timing: "48h", minH: 36, maxH: 96, lang: "pt_BR", vars: ["primeiro_nome"], hasButton: true, buttonText: "Usar meu cupom", preview: "Ola {{1}}, aqui e a equipe SSJ ... cupom VOLTECOMSSJ ..." },
];

// PIX — CORRIGIDO: nomes corretos do Meta, body={{1}}(nome) + botão URL dinâmica
const PIX_TEMPLATES = [
  { id: "pix_5min",  name: "pix_5min",  display: "PIX 5min",  timing: "5min",  minH: 0, maxH: 0.25, lang: "pt_BR", vars: ["primeiro_nome"], hasButton: true },
  { id: "pix_30min", name: "pix_30min", display: "PIX 30min", timing: "30min", minH: 0.25, maxH: 0.75, lang: "pt_BR", vars: ["primeiro_nome"], hasButton: true },
  { id: "pix_1h",    name: "pix_1h",    display: "PIX 1h",    timing: "1h",    minH: 0.75, maxH: 6, lang: "pt_BR", vars: ["primeiro_nome"], hasButton: true },
  { id: "pix_24h",   name: "pix_24h",   display: "PIX 24h",   timing: "24h",   minH: 6, maxH: 36, lang: "pt_BR", vars: ["primeiro_nome"], hasButton: true },
  { id: "pix_48h",   name: "pix_48h",   display: "PIX 48h",   timing: "48h",   minH: 36, maxH: 96, lang: "pt_BR", vars: ["primeiro_nome"], hasButton: true },
];

// RECOMPRA — body={{1}}(nome) + {{2}}(cupom), SEM botão URL dinâmica
const RECOMPRA_TEMPLATES = [
  { id: "recompra_30dias", name: "recompra_30dias", display: "Recompra 30 dias", timing: "30 dias", lang: "pt_BR", vars: ["primeiro_nome", "cupom"], hasButton: false, preview: "Oi {{1}}! ... Use o cupom {{2}} ..." },
  { id: "recompra_60dias", name: "recompra_60dias", display: "Recompra 60 dias", timing: "60 dias", lang: "pt_BR", vars: ["primeiro_nome", "cupom"], hasButton: false, preview: "{{1}}, sentimos sua falta! ... cupom {{2}} ..." },
  { id: "recompra_90dias", name: "recompra_90dias", display: "Recompra 90 dias", timing: "90 dias", lang: "pt_BR", vars: ["primeiro_nome", "cupom"], hasButton: false, preview: "{{1}}, faz tempo! ... cupom {{2}} ..." },
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

// ===================== CART FUNCTIONS =====================

async function fetchCarts() {
  var data = await yampiGet("/checkout/carts", { include: "customer,items", limit: "50", orderBy: "created_at", sortedBy: "desc" });

  // Batch: buscar todos os envios de uma vez (1 query em vez de 50)
  var sentMap = await getAllSentMap();

  var results = [];
  for (var i = 0; i < (data.data || []).length; i++) {
    var cart = data.data[i];
    var cust = cart.customer && cart.customer.data ? cart.customer.data : {};
    var ph = (cust.phone && cust.phone.full_number) || (cust.spreadsheet && cust.spreadsheet.data && cust.spreadsheet.data.phone_number) || "";
    var ddd = (cust.phone && cust.phone.area_code) || (cust.spreadsheet && cust.spreadsheet.data && cust.spreadsheet.data.phone_code) || "";
    var created = (cart.created_at && cart.created_at.date) || cart.created_at || null;
    var hoursAgo = created ? Math.round((Date.now() - new Date(created).getTime()) / 3600000) : 0;
    var items = Array.isArray(cart.items && cart.items.data) ? cart.items.data.map(function(it) { return (it.sku && it.sku.data && it.sku.data.title) || it.name || "Produto"; }) : [];
    var rec = TEMPLATES.find(function(t) { return hoursAgo >= t.minH && hoursAgo < t.maxH; });
    // Extrair status da transação — Yampi pode retornar string, objeto, ou objeto aninhado
    var rawTxStatus = cart.last_transaction_status || cart.transaction_status || null;
    var lastTxStatus = null;
    if (rawTxStatus) {
      if (typeof rawTxStatus === "string") {
        lastTxStatus = rawTxStatus;
      } else if (typeof rawTxStatus === "object") {
        lastTxStatus = rawTxStatus.alias || rawTxStatus.name || rawTxStatus.status ||
          (rawTxStatus.data && (rawTxStatus.data.alias || rawTxStatus.data.name)) || null;
      }
    }
    // Log primeiro carrinho pra debug (só uma vez)
    if (i === 0) {
      console.log("[DEBUG-CART] Primeiro carrinho raw status:", JSON.stringify(rawTxStatus));
      console.log("[DEBUG-CART] Primeiro carrinho simUrl:", cart.simulate_url || cart.unauth_simulate_url || "VAZIO");
    }

    results.push({
      id: cart.id, name: cust.name || cust.first_name || "Cliente",
      firstName: cust.first_name || (cust.name || "").split(" ")[0] || "Cliente",
      email: cust.email || "", phone: formatPhone(ph, ddd),
      total: (cart.totalizers && cart.totalizers.total_formated) || "R$ " + ((cart.totalizers && cart.totalizers.total) || 0).toFixed(2),
      totalRaw: (cart.totalizers && cart.totalizers.total) || 0,
      items: items.join(", ") || "Itens no carrinho", itemCount: items.length,
      simUrl: cart.simulate_url || cart.unauth_simulate_url || "",
      hoursAgo: hoursAgo, createdAt: created,
      recommended: rec ? rec.id : TEMPLATES[3].id,
      alreadySent: sentMap[String(cart.id)] || [],
      lastTxStatus: lastTxStatus
    });
  }
  return results;
}

// ===================== ORDERS FUNCTIONS =====================

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
    var hoursAgo = created ? Math.round((Date.now() - new Date(created).getTime()) / 3600000) : 0;
    // Extrair status
    var statusAlias = order.status && order.status.data ? order.status.data.alias : (order.status_alias || "");
    var statusLabel = order.status && order.status.data ? order.status.data.name : (order.status_label || "");
    // Checkout URL pra recompra/PIX
    var checkoutUrl = order.checkout_url || order.simulate_url || order.unauth_simulate_url || "";
    return {
      id: order.id,
      number: order.number || order.id,
      name: cust.name || cust.first_name || "Cliente",
      firstName: cust.first_name || (cust.name || "").split(" ")[0] || "Cliente",
      email: cust.email || "",
      phone: formatPhone(ph, ddd),
      total: order.value_total_formated || "R$ " + (order.value_total || 0).toFixed(2),
      totalRaw: order.value_total || 0,
      status: statusAlias,
      statusLabel: statusLabel,
      createdAt: created,
      daysAgo: daysAgo,
      hoursAgo: hoursAgo,
      customerId: cust.id || null,
      simUrl: checkoutUrl
    };
  });
}

// ===================== WHATSAPP SEND =====================

async function sendWA(phone, templateName, params, allTemplates, buttonUrl) {
  var searchIn = allTemplates || TEMPLATES;
  var tpl = searchIn.find(function(t) { return t.name === templateName; });
  if (!tpl) throw new Error("Template nao encontrado: " + templateName);

  var components = [];

  // Body parameters (nome, cupom, etc)
  if (params && params.length > 0) {
    components.push({ type: "body", parameters: params.map(function(p) { return { type: "text", text: String(p) }; }) });
  }

  // Button URL parameter (dynamic URL suffix) — SÓ se template tem botão
  if (tpl.hasButton) {
    // CRITICAL: remover espaços da URL — Yampi gera utm_campaign= &force... com espaço
    var urlParam = (buttonUrl || "cart").replace(/ /g, "");
    components.push({ type: "button", sub_type: "url", index: 0, parameters: [{ type: "text", text: String(urlParam) }] });
  }

  var payload = { messaging_product: "whatsapp", to: phone, type: "template", template: { name: tpl.name, language: { code: tpl.lang }, components: components } };
  console.log("[SEND-WA] " + phone + " tpl=" + tpl.name + " body_params=" + (params ? params.length : 0) + " btn=" + (tpl.hasButton ? "yes" : "no"));

  var r = await fetch("https://graph.facebook.com/" + CFG.waVersion + "/" + CFG.waPhoneId + "/messages", {
    method: "POST",
    headers: { Authorization: "Bearer " + CFG.waToken, "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  var data = await r.json();
  if (!r.ok) {
    console.error("[SEND-WA-ERRO] " + phone + " tpl=" + tpl.name + " status=" + r.status + " erro=" + JSON.stringify(data));
    throw new Error((data.error && data.error.message) || "WA " + r.status);
  }
  return (data.messages && data.messages[0] && data.messages[0].id) || null;
}

function buildParams(tpl, cart) {
  return tpl.vars.map(function(v) {
    if (v === "primeiro_nome") return cart.firstName;
    if (v === "cupom") return CFG.coupon;
    return "";
  });
}

function getCartUrl(cart) {
  var url = cart.simUrl || "";
  if (!url) return "cart";

  // Log pra debug
  console.log("[CART-URL] simUrl original:", url.substring(0, 100));

  // Se a URL contém o domínio SSJ, extrair só o path
  var domain = "https://seguro.ssjmodafitness.com.br/";
  if (url.indexOf(domain) === 0) {
    var path = url.substring(domain.length);
    console.log("[CART-URL] Extraido path:", path.substring(0, 80));
    return path;
  }

  // Tentar extrair path de qualquer URL com domínio SSJ
  var match = url.match(/ssjmodafitness\.com\.br\/(.*)/);
  if (match) return match[1];

  // Se a URL é de outro domínio (Yampi), extrair só o path+query
  try {
    var parsed = new URL(url);
    var suffix = parsed.pathname.substring(1) + parsed.search;
    console.log("[CART-URL] URL externa, usando path:", suffix.substring(0, 80));
    return suffix || "cart";
  } catch (e) {
    // URL inválida, retornar fallback
    return "cart";
  }
}

function buildPixParams(tpl, cart) {
  // PIX templates v2: só {{1}}=nome no body
  return tpl.vars.map(function(v) {
    if (v === "primeiro_nome") return cart.firstName;
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

// ===================== RECORD FUNCTIONS (PostgreSQL) =====================

async function record(cart, tpl, status, msgId, auto) {
  try {
    // SEMPRE salvar no banco — inclusive falhas — pra não reenviar
    await pool.query(
      `INSERT INTO sent_messages (cart_id, template_id, phone, contact_name, cart_value, wa_message_id, status, automated, msg_type)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'carrinho')
       ON CONFLICT (cart_id, template_id) DO UPDATE SET status=$7, wa_message_id=COALESCE($6, sent_messages.wa_message_id)`,
      [String(cart.id), tpl.id, cart.phone, cart.name, cart.totalRaw || 0, msgId, status, !!auto]
    );
    if (status !== "failed") {
      STATS.totalSent++; STATS.totalCartValue += cart.totalRaw || 0;
      if (cart.phone) await addOutgoingMsg(cart.phone, cart.name, "[Template: " + tpl.display + "]", tpl.name, msgId);
    } else {
      STATS.totalFailed++;
    }
  } catch (e) {
    console.error("[RECORD] Erro ao salvar:", e.message);
  }
}

async function recordPix(cart, tpl, status, msgId) {
  try {
    await pool.query(
      `INSERT INTO pix_sent (cart_id, template_id, phone, contact_name, cart_value, wa_message_id, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (cart_id, template_id) DO UPDATE SET status=$7, wa_message_id=COALESCE($6, pix_sent.wa_message_id)`,
      [String(cart.id), tpl.id, cart.phone, cart.name, cart.total, msgId, status]
    );
    if (status !== "failed") {
      pixStats.totalSent++;
      if (cart.phone) await addOutgoingMsg(cart.phone, cart.name, "[Template: " + tpl.display + "]", tpl.name, msgId);
    } else {
      pixStats.totalFailed++;
    }
  } catch (e) {
    console.error("[RECORD-PIX] Erro ao salvar:", e.message);
  }
}

async function recordRecompra(order, tpl, status, msgId, intervalDays) {
  try {
    await pool.query(
      `INSERT INTO recompra_sent (order_id, interval_days, template_id, phone, contact_name, order_value, wa_message_id, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (order_id, interval_days) DO UPDATE SET status=$8, wa_message_id=COALESCE($7, recompra_sent.wa_message_id)`,
      [String(order.id), intervalDays, tpl.id, order.phone, order.name, order.total, msgId, status]
    );
    if (status !== "failed") {
      recompraStats.totalSent++;
      if (order.phone) await addOutgoingMsg(order.phone, order.name, "[Template: " + tpl.display + "]", tpl.name, msgId);
    } else {
      recompraStats.totalFailed++;
    }
  } catch (e) {
    console.error("[RECORD-RECOMPRA] Erro ao salvar:", e.message);
  }
}

// ===================== CONVERSATION TRACKING (PostgreSQL) =====================

async function getOrCreateConvo(phone, name) {
  try {
    var r = await pool.query("SELECT * FROM conversations WHERE phone=$1", [phone]);
    if (r.rowCount === 0) {
      await pool.query(
        "INSERT INTO conversations (phone, name, unread) VALUES ($1, $2, 0) ON CONFLICT (phone) DO UPDATE SET name=$2",
        [phone, name || phone]
      );
    } else if (name && name !== phone) {
      await pool.query("UPDATE conversations SET name=$1 WHERE phone=$2", [name, phone]);
    }
  } catch (e) {
    console.error("[CONVO] Erro:", e.message);
  }
}

async function addOutgoingMsg(phone, name, text, templateName, waMessageId) {
  try {
    await getOrCreateConvo(phone, name);
    await pool.query(
      "INSERT INTO messages (phone, wa_message_id, direction, text, template, status) VALUES ($1, $2, 'out', $3, $4, 'sent')",
      [phone, waMessageId || ("out-" + Date.now()), text, templateName || null]
    );
    await pool.query("UPDATE conversations SET last_message_at=NOW() WHERE phone=$1", [phone]);
  } catch (e) {
    console.error("[MSG-OUT] Erro:", e.message);
  }
}

async function addIncomingMsg(phone, name, text, waMessageId, msgType) {
  try {
    // Avoid duplicates
    if (waMessageId) {
      var dup = await pool.query("SELECT 1 FROM messages WHERE wa_message_id=$1", [waMessageId]);
      if (dup.rowCount > 0) return;
    }
    await getOrCreateConvo(phone, name);
    await pool.query(
      "INSERT INTO messages (phone, wa_message_id, direction, text, msg_type, status) VALUES ($1, $2, 'in', $3, $4, 'received')",
      [phone, waMessageId || ("in-" + Date.now()), text, msgType || "text"]
    );
    await pool.query(
      "UPDATE conversations SET last_message_at=NOW(), unread=unread+1 WHERE phone=$1",
      [phone]
    );
    console.log("[INBOX] Nova mensagem de " + phone + ": " + text);
  } catch (e) {
    console.error("[MSG-IN] Erro:", e.message);
  }
}

// ===================== CRON: CART RECOVERY =====================

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
      // Verifica no PostgreSQL se já foi enviado
      var alreadySent = await wasSent(cart.id, tpl.id);
      if (alreadySent) { skipped++; continue; }
      // Intervalo mínimo de 2h entre templates do mesmo carrinho (evita envio em sequência rápida)
      try {
        var recentCheck = await pool.query("SELECT sent_at FROM sent_messages WHERE cart_id=$1 AND status != 'failed' ORDER BY sent_at DESC LIMIT 1", [String(cart.id)]);
        if (recentCheck.rowCount > 0 && (Date.now() - new Date(recentCheck.rows[0].sent_at).getTime()) < 7200000) { skipped++; continue; }
      } catch (e) {}
      try {
        var msgId = await sendWA(cart.phone, tpl.name, buildParams(tpl, cart), null, getCartUrl(cart));
        await record(cart, tpl, "sent", msgId, true);
        sent++;
        await new Promise(function(r) { setTimeout(r, 250); });
      } catch (e) {
        console.error("[AUTO-CARRINHO] Falha " + cart.name + " (" + cart.phone + "): " + e.message);
        await record(cart, tpl, "failed", null, true); failed++;
      }
    }
    cronLog.unshift({ ts: new Date().toISOString(), cartsFound: carts.length, sent: sent, skipped: skipped, failed: failed });
    if (cronLog.length > 100) cronLog.length = 100;
    console.log("[AUTO-CARRINHO] " + sent + " enviado(s), " + skipped + " pulado(s), " + failed + " falha(s)");
  } catch (e) { console.error("[AUTO-CARRINHO] Erro: " + e.message); cronLog.unshift({ ts: new Date().toISOString(), error: e.message }); }
});

// ===================== CRON: PIX/BOLETO RECOVERY =====================

cron.schedule("*/15 * * * *", async function() {
  console.log("[AUTO-PIX] " + new Date().toISOString() + " Verificando PIX/boleto...");
  try {
    // Buscar pedidos recentes (sem filtro de data — já vem ordenado por created_at desc)
    var orders = await fetchOrders({ limit: "50" });

    var sent = 0, skipped = 0, failed = 0;

    // LOG: mostrar TODOS os status encontrados
    var statusSet = {};
    orders.forEach(function(o) {
      var s = (o.status || "sem_status").toLowerCase();
      statusSet[s] = (statusSet[s] || 0) + 1;
    });
    console.log("[AUTO-PIX] Status encontrados nos pedidos:", JSON.stringify(statusSet));

    // Filtrar pedidos com pagamento cancelado/recusado/expirado
    var pixStatuses = [
      "cancelled", "canceled", "cancelado",
      "refused", "recusado", "expired", "expirado",
      "waiting_payment", "awaiting_payment",
      "not_paid", "payment_error", "payment_failed",
      "pending", "pendente"
    ];

    for (var i = 0; i < orders.length; i++) {
      var order = orders[i];
      if (!order.phone || order.phone.length < 12) { skipped++; continue; }

      var orderStatus = (order.status || "").toLowerCase();

      // Verificar se é status de PIX/boleto não pago
      var isPixBoleto = false;
      for (var s = 0; s < pixStatuses.length; s++) {
        if (orderStatus === pixStatuses[s]) { isPixBoleto = true; break; }
      }
      // Fallback: substrings
      if (!isPixBoleto) {
        if (orderStatus.indexOf("cancel") !== -1 || orderStatus.indexOf("recus") !== -1 ||
            orderStatus.indexOf("expir") !== -1 || orderStatus.indexOf("pending") !== -1 ||
            orderStatus.indexOf("waiting") !== -1) {
          isPixBoleto = true;
        }
      }

      if (!isPixBoleto) { skipped++; continue; }

      // Escolher template PIX baseado na idade do pedido
      var pixTpl = PIX_TEMPLATES.find(function(t) { return order.hoursAgo >= t.minH && order.hoursAgo < t.maxH; });
      if (!pixTpl) { skipped++; continue; } // pedido fora do range de timing (>96h), pular

      // Verifica no PostgreSQL se já foi enviado (usando order.id como cart_id)
      var alreadySent = await wasPixSent(order.id, pixTpl.id);
      if (alreadySent) { skipped++; continue; }

      try {
        var allTpls = TEMPLATES.concat(PIX_TEMPLATES).concat(RECOMPRA_TEMPLATES);
        var urlSuffix = getCartUrl(order); // usa simUrl do pedido
        var msgId = await sendWA(order.phone, pixTpl.name, buildPixParams(pixTpl, order), allTpls, urlSuffix);
        // Registrar como PIX sent (reutiliza cart_id field pro order.id)
        await recordPix({ id: order.id, phone: order.phone, name: order.name, total: order.total }, pixTpl, "sent", msgId);
        sent++;
        await new Promise(function(r) { setTimeout(r, 250); });
      } catch (e) {
        console.error("[AUTO-PIX] Falha " + order.name + " (" + order.phone + "): " + e.message);
        await recordPix({ id: order.id, phone: order.phone, name: order.name, total: order.total }, pixTpl, "failed", null);
        failed++;
      }
    }

    pixCronLog.unshift({ ts: new Date().toISOString(), ordersChecked: orders.length, sent: sent, skipped: skipped, failed: failed });
    if (pixCronLog.length > 100) pixCronLog.length = 100;
    console.log("[AUTO-PIX] " + sent + " enviado(s), " + skipped + " pulado(s), " + failed + " falha(s)");
  } catch (e) {
    console.error("[AUTO-PIX] Erro: " + e.message);
    pixCronLog.unshift({ ts: new Date().toISOString(), error: e.message });
  }
});

// ===================== CRON: REPURCHASE CAMPAIGNS =====================

cron.schedule("0 10 * * *", async function() {
  console.log("[AUTO-RECOMPRA] " + new Date().toISOString() + " Verificando campanhas de recompra...");
  if (!recompraConfig.enabled) {
    console.log("[AUTO-RECOMPRA] Desabilitado nas configuracoes.");
    recompraCronLog.unshift({ ts: new Date().toISOString(), disabled: true });
    return;
  }

  try {
    var intervals = recompraConfig.intervals.filter(function(iv) { return iv.enabled; });
    var totalSent = 0, totalSkipped = 0, totalFailed = 0;

    for (var k = 0; k < intervals.length; k++) {
      var iv = intervals[k];
      var tpl = RECOMPRA_TEMPLATES.find(function(t) { return t.id === iv.templateId; });
      if (!tpl) continue;

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
        var paidOrders = orders.filter(function(o) {
          var s = (o.status || "").toLowerCase();
          return s === "paid" || s === "invoiced" || s === "shipped" || s === "delivered" || s === "complete" || s === "completed" || s === "pago" || s === "enviado" || s === "entregue";
        });

        for (var j = 0; j < paidOrders.length; j++) {
          var order = paidOrders[j];
          if (!order.phone || order.phone.length < 12) { totalSkipped++; continue; }

          var alreadySent = await wasRecompraSent(order.id, iv.days);
          if (alreadySent) { totalSkipped++; continue; }

          try {
            var allTpls = TEMPLATES.concat(PIX_TEMPLATES).concat(RECOMPRA_TEMPLATES);
            var coupon = iv.coupon || CFG.coupon;
            var msgId = await sendWA(order.phone, tpl.name, buildRecompraParams(tpl, order, coupon), allTpls);
            await recordRecompra(order, tpl, "sent", msgId, iv.days);
            totalSent++;
            await new Promise(function(r) { setTimeout(r, 300); });
          } catch (e) {
            await recordRecompra(order, tpl, "failed", null, iv.days);
            totalFailed++;
          }
        }
      } catch (e) {
        console.error("[AUTO-RECOMPRA] Erro ao buscar pedidos " + iv.days + "d: " + e.message);
      }
    }

    recompraCronLog.unshift({ ts: new Date().toISOString(), sent: totalSent, skipped: totalSkipped, failed: totalFailed });
    if (recompraCronLog.length > 100) recompraCronLog.length = 100;
    console.log("[AUTO-RECOMPRA] " + totalSent + " enviado(s), " + totalSkipped + " pulado(s), " + totalFailed + " falha(s)");
  } catch (e) {
    console.error("[AUTO-RECOMPRA] Erro: " + e.message);
    recompraCronLog.unshift({ ts: new Date().toISOString(), error: e.message });
  }
});

// ===================== API ROUTES =====================

app.get("/api/health", async function(req, res) {
  var yOk = false, wOk = false, dbOk = false;
  try { await yampiGet("/catalog/products", { limit: "1" }); yOk = true; } catch (e) {}
  try { var r = await fetch("https://graph.facebook.com/" + CFG.waVersion + "/" + CFG.waPhoneId, { headers: { Authorization: "Bearer " + CFG.waToken } }); wOk = r.ok; } catch (e) {}
  try { await pool.query("SELECT 1"); dbOk = true; } catch (e) {}
  res.json({ yampi: { ok: yOk, alias: CFG.yampiAlias }, whatsapp: { ok: wOk, phoneId: CFG.waPhoneId }, database: { ok: dbOk }, uptime: process.uptime(), stats: STATS });
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
    var alreadySent = await wasSent(cartIds[i], templateId);
    if (alreadySent) { results.push({ cartId: cartIds[i], ok: false, error: "Ja enviado" }); continue; }
    try { var msgId = await sendWA(cart.phone, tpl.name, buildParams(tpl, cart), null, getCartUrl(cart)); await record(cart, tpl, "sent", msgId, false); results.push({ cartId: cartIds[i], ok: true, contact: cart.name }); }
    catch (e) { await record(cart, tpl, "failed", null, false); results.push({ cartId: cartIds[i], ok: false, error: e.message }); }
  }
  res.json({ ok: true, sent: results.filter(function(r) { return r.ok; }).length, failed: results.filter(function(r) { return !r.ok; }).length, results: results });
});

app.get("/api/history", async function(req, res) {
  try {
    var r = await pool.query("SELECT * FROM sent_messages ORDER BY sent_at DESC LIMIT 50");
    var data = r.rows.map(function(row) {
      return {
        id: row.id,
        cartId: row.cart_id,
        contact: row.contact_name || "Cliente",
        phone: row.phone || "",
        template: row.template_id ? row.template_id.replace(/_/g, " ").replace(/\bv2\b/, "").trim() : "",
        templateId: row.template_id,
        status: row.status || "sent",
        sentAt: row.sent_at ? new Date(row.sent_at).toISOString() : "",
        cartValue: row.cart_value ? "R$ " + Number(row.cart_value).toFixed(2) : "—",
        cartValueRaw: Number(row.cart_value) || 0,
        waMessageId: row.wa_message_id,
        automated: row.automated || false,
        type: row.msg_type || "carrinho"
      };
    });
    res.json({ data: data, total: r.rowCount });
  } catch (e) { console.error("[HISTORY] Erro:", e.message); res.json({ data: [], total: 0 }); }
});

app.get("/api/stats", async function(req, res) {
  try {
    var today = new Date().toISOString().slice(0, 10);
    var todayR = await pool.query("SELECT status, automated FROM sent_messages WHERE sent_at::date = $1", [today]);
    var todayRows = todayR.rows;
    var byTpl = {};
    for (var t = 0; t < TEMPLATES.length; t++) {
      var tid = TEMPLATES[t].id;
      var tplR = await pool.query("SELECT status FROM sent_messages WHERE template_id=$1", [tid]);
      byTpl[tid] = {
        display: TEMPLATES[t].display,
        total: tplR.rowCount,
        delivered: tplR.rows.filter(function(r) { return r.status === "delivered" || r.status === "read"; }).length,
        read: tplR.rows.filter(function(r) { return r.status === "read"; }).length,
        failed: tplR.rows.filter(function(r) { return r.status === "failed"; }).length
      };
    }
    res.json({
      global: STATS,
      today: {
        sent: todayRows.length,
        delivered: todayRows.filter(function(r) { return r.status === "delivered" || r.status === "read"; }).length,
        read: todayRows.filter(function(r) { return r.status === "read"; }).length,
        failed: todayRows.filter(function(r) { return r.status === "failed"; }).length,
        automated: todayRows.filter(function(r) { return r.automated; }).length
      },
      byTemplate: byTpl,
      cronLog: cronLog.slice(0, 10)
    });
  } catch (e) { res.json({ global: STATS, today: {}, byTemplate: {}, cronLog: cronLog.slice(0, 10) }); }
});

// ===================== API ROUTES: PIX/BOLETO =====================

app.get("/api/pix/carts", async function(req, res) {
  try {
    // Buscar PEDIDOS com status cancelado/recusado (não carrinhos)
    var orders = await fetchOrders({ limit: "50" });
    var pixStatuses = [
      "cancelled", "canceled", "cancelado",
      "refused", "recusado", "expired", "expirado",
      "waiting_payment", "awaiting_payment",
      "not_paid", "payment_error", "payment_failed",
      "pending", "pendente"
    ];
    var pixOrders = [];
    for (var i = 0; i < orders.length; i++) {
      var o = orders[i];
      var st = (o.status || "").toLowerCase();
      var isPixBoleto = false;
      for (var s = 0; s < pixStatuses.length; s++) {
        if (st === pixStatuses[s]) { isPixBoleto = true; break; }
      }
      if (!isPixBoleto) {
        if (st.indexOf("cancel") !== -1 || st.indexOf("recus") !== -1 ||
            st.indexOf("expir") !== -1 || st.indexOf("pending") !== -1 ||
            st.indexOf("waiting") !== -1) {
          isPixBoleto = true;
        }
      }
      if (isPixBoleto) {
        o.paymentType = "PIX/Boleto";
        o.pixAlreadySent = await getPixSentTemplates(o.id);
        // Compatibilidade com o painel (espera campos de carrinho)
        o.items = "Pedido #" + (o.number || o.id);
        o.itemCount = 1;
        o.totalRaw = o.totalRaw || 0;
        o.recommended = null;
        o.alreadySent = o.pixAlreadySent;
        pixOrders.push(o);
      }
    }
    res.json({ ok: true, count: pixOrders.length, data: pixOrders });
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
    var alreadySent = await wasPixSent(cartIds[i], templateId);
    if (alreadySent) { results.push({ cartId: cartIds[i], ok: false, error: "Ja enviado" }); continue; }
    try {
      var msgId = await sendWA(cart.phone, tpl.name, buildPixParams(tpl, cart), allTpls, getCartUrl(cart));
      await recordPix(cart, tpl, "sent", msgId);
      results.push({ cartId: cartIds[i], ok: true, contact: cart.name });
    } catch (e) { await recordPix(cart, tpl, "failed", null); results.push({ cartId: cartIds[i], ok: false, error: e.message }); }
  }
  res.json({ ok: true, sent: results.filter(function(r) { return r.ok; }).length, failed: results.filter(function(r) { return !r.ok; }).length, results: results });
});

app.get("/api/pix/history", async function(req, res) {
  try {
    var r = await pool.query("SELECT * FROM pix_sent ORDER BY sent_at DESC LIMIT 50");
    var data = r.rows.map(function(row) {
      return {
        id: row.id,
        cartId: row.cart_id,
        contact: row.contact_name || "Cliente",
        phone: row.phone || "",
        template: row.template_id ? row.template_id.replace(/_/g, " ") : "",
        templateId: row.template_id,
        status: row.status || "sent",
        sentAt: row.sent_at ? new Date(row.sent_at).toISOString() : "",
        cartValue: row.cart_value || "—",
        waMessageId: row.wa_message_id,
        type: "pix"
      };
    });
    res.json({ data: data, total: r.rowCount });
  } catch (e) { res.json({ data: [], total: 0 }); }
});

app.get("/api/pix/stats", function(req, res) {
  res.json({
    stats: pixStats,
    cronLog: pixCronLog.slice(0, 10),
    byTemplate: PIX_TEMPLATES.map(function(t) { return { display: t.display, total: 0, sent: 0, failed: 0 }; })
  });
});

// ===================== API ROUTES: REPURCHASE =====================

app.get("/api/recompra/orders", async function(req, res) {
  try {
    var allOrders = [];
    var intervals = recompraConfig.intervals;

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

      for (var j = 0; j < paidOrders.length; j++) {
        var o = paidOrders[j];
        o.intervalDays = iv.days;
        o.intervalTemplate = iv.templateId;
        o.intervalCoupon = iv.coupon;
        o.alreadySent = await wasRecompraSent(o.id, iv.days);
        allOrders.push(o);
      }
    }

    res.json({ ok: true, count: allOrders.length, data: allOrders });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get("/api/recompra/templates", function(req, res) { res.json({ data: RECOMPRA_TEMPLATES }); });

app.get("/api/recompra/config", function(req, res) { res.json({ data: recompraConfig }); });

app.post("/api/recompra/config", function(req, res) {
  if (req.body.enabled !== undefined) recompraConfig.enabled = !!req.body.enabled;
  if (req.body.intervals && Array.isArray(req.body.intervals)) {
    req.body.intervals.forEach(function(iv) {
      var existing = recompraConfig.intervals.find(function(e) { return e.days === iv.days; });
      if (existing) {
        if (iv.enabled !== undefined) existing.enabled = !!iv.enabled;
        if (iv.coupon) existing.coupon = iv.coupon;
        if (iv.templateId) existing.templateId = iv.templateId;
      }
    });
  }
  res.json({ ok: true, data: recompraConfig });
});

app.post("/api/recompra/send", async function(req, res) {
  var orderIds = req.body.orderIds, templateId = req.body.templateId, coupon = req.body.coupon, intervalDays = req.body.intervalDays || 30;
  if (!orderIds || !orderIds.length) return res.status(400).json({ error: "orderIds obrigatorio" });
  var allTpls = TEMPLATES.concat(PIX_TEMPLATES).concat(RECOMPRA_TEMPLATES);
  var tpl = allTpls.find(function(t) { return t.id === templateId; });
  if (!tpl) return res.status(400).json({ error: "Template nao encontrado" });

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
      var alreadySent = await wasRecompraSent(order.id, intervalDays);
      if (alreadySent) { results.push({ orderId: orderIds[i], ok: false, error: "Ja enviado" }); continue; }
      try {
        var msgId = await sendWA(order.phone, tpl.name, buildRecompraParams(tpl, order, coupon || CFG.coupon), allTpls);
        await recordRecompra(order, tpl, "sent", msgId, intervalDays);
        results.push({ orderId: orderIds[i], ok: true, contact: order.name });
      } catch (e) { await recordRecompra(order, tpl, "failed", null, intervalDays); results.push({ orderId: orderIds[i], ok: false, error: e.message }); }
    }
  } catch (e) { return res.status(500).json({ ok: false, error: e.message }); }

  res.json({ ok: true, sent: results.filter(function(r) { return r.ok; }).length, failed: results.filter(function(r) { return !r.ok; }).length, results: results });
});

app.get("/api/recompra/history", async function(req, res) {
  try {
    var r = await pool.query("SELECT * FROM recompra_sent ORDER BY sent_at DESC LIMIT 50");
    var data = r.rows.map(function(row) {
      return {
        id: row.id,
        orderId: row.order_id,
        contact: row.contact_name || "Cliente",
        phone: row.phone || "",
        template: row.template_id ? row.template_id.replace(/_/g, " ") : "",
        templateId: row.template_id,
        status: row.status || "sent",
        sentAt: row.sent_at ? new Date(row.sent_at).toISOString() : "",
        orderValue: row.order_value || "—",
        waMessageId: row.wa_message_id,
        intervalDays: row.interval_days,
        type: "recompra"
      };
    });
    res.json({ data: data, total: r.rowCount });
  } catch (e) { res.json({ data: [], total: 0 }); }
});

app.get("/api/recompra/stats", function(req, res) {
  res.json({
    stats: recompraStats,
    config: recompraConfig,
    cronLog: recompraCronLog.slice(0, 10),
    byTemplate: RECOMPRA_TEMPLATES.map(function(t) { return { display: t.display, total: 0, sent: 0, failed: 0 }; })
  });
});

// ===================== WEBHOOKS =====================

app.get("/api/webhook", function(req, res) { if (req.query["hub.mode"] === "subscribe" && req.query["hub.verify_token"] === "ssj_verify_token") return res.send(req.query["hub.challenge"]); res.sendStatus(403); });

app.post("/api/webhook", async function(req, res) {
  for (var e = 0; e < (req.body.entry || []).length; e++) {
    var entry = req.body.entry[e];
    for (var c = 0; c < (entry.changes || []).length; c++) {
      var value = entry.changes[c].value || {};

      // Status updates
      for (var s = 0; s < (value.statuses || []).length; s++) {
        var st = value.statuses[s];
        try {
          // Update in sent_messages
          await pool.query(
            "UPDATE sent_messages SET status=$1 WHERE wa_message_id=$2 AND status NOT IN ('read')",
            [st.status, st.id]
          );
          // Update in pix_sent
          await pool.query(
            "UPDATE pix_sent SET status=$1 WHERE wa_message_id=$2 AND status NOT IN ('read')",
            [st.status, st.id]
          );
          // Update in recompra_sent
          await pool.query(
            "UPDATE recompra_sent SET status=$1 WHERE wa_message_id=$2 AND status NOT IN ('read')",
            [st.status, st.id]
          );
          // Update in messages
          await pool.query(
            "UPDATE messages SET status=$1 WHERE wa_message_id=$2",
            [st.status, st.id]
          );
          if (st.status === "delivered") STATS.totalDelivered++;
          if (st.status === "read") STATS.totalRead++;
        } catch (err) {
          console.error("[WEBHOOK] Erro ao atualizar status:", err.message);
        }
      }

      // Incoming messages
      for (var m = 0; m < (value.messages || []).length; m++) {
        var msg = value.messages[m];
        var from = msg.from;
        var contactName = from;
        if (value.contacts && value.contacts.length > 0) {
          var contact = value.contacts.find(function(ct) { return ct.wa_id === from; });
          if (contact && contact.profile && contact.profile.name) {
            contactName = contact.profile.name;
          }
        }

        var text = "";
        var msgType = msg.type || "text";

        if (msg.type === "text" && msg.text) {
          text = msg.text.body || "";
        } else if (msg.type === "image") {
          text = "Imagem" + (msg.image && msg.image.caption ? ": " + msg.image.caption : "");
        } else if (msg.type === "audio") {
          text = "Audio";
        } else if (msg.type === "video") {
          text = "Video";
        } else if (msg.type === "document") {
          text = "Documento" + (msg.document && msg.document.filename ? ": " + msg.document.filename : "");
        } else if (msg.type === "sticker") {
          text = "Sticker";
        } else if (msg.type === "location") {
          text = "Localizacao";
        } else if (msg.type === "button") {
          text = (msg.button && msg.button.text) || "Botao";
        } else if (msg.type === "interactive") {
          text = (msg.interactive && msg.interactive.button_reply && msg.interactive.button_reply.title) || "Resposta interativa";
        } else {
          text = "[" + msgType + "]";
        }

        await addIncomingMsg(from, contactName, text, msg.id, msgType);
      }
    }
  }
  res.sendStatus(200);
});

// ===================== INBOX API ROUTES =====================

app.get("/api/inbox/conversations", async function(req, res) {
  try {
    var r = await pool.query(`
      SELECT c.phone, c.name, c.unread, c.last_message_at,
        (SELECT text FROM messages WHERE phone=c.phone ORDER BY created_at DESC LIMIT 1) as last_message,
        (SELECT direction FROM messages WHERE phone=c.phone ORDER BY created_at DESC LIMIT 1) as last_direction,
        (SELECT COUNT(*) FROM messages WHERE phone=c.phone) as message_count
      FROM conversations c
      ORDER BY c.last_message_at DESC
      LIMIT 100
    `);
    var totalUnread = r.rows.reduce(function(s, c) { return s + (c.unread || 0); }, 0);
    res.json({
      ok: true,
      data: r.rows.map(function(c) {
        return {
          phone: c.phone, name: c.name, lastMessage: c.last_message || "",
          lastMessageAt: c.last_message_at, lastDirection: c.last_direction,
          unread: c.unread || 0, messageCount: parseInt(c.message_count) || 0
        };
      }),
      totalUnread: totalUnread
    });
  } catch (e) { res.json({ ok: true, data: [], totalUnread: 0 }); }
});

app.get("/api/inbox/conversation/:phone", async function(req, res) {
  try {
    var convo = await pool.query("SELECT * FROM conversations WHERE phone=$1", [req.params.phone]);
    var msgs = await pool.query("SELECT * FROM messages WHERE phone=$1 ORDER BY created_at ASC LIMIT 200", [req.params.phone]);
    if (convo.rowCount === 0) {
      return res.json({ ok: true, data: { phone: req.params.phone, name: req.params.phone, messages: [] } });
    }
    res.json({
      ok: true,
      data: {
        phone: convo.rows[0].phone,
        name: convo.rows[0].name,
        messages: msgs.rows.map(function(m) {
          return { id: m.wa_message_id || m.id, direction: m.direction, text: m.text, type: m.msg_type, template: m.template, status: m.status, timestamp: m.created_at };
        })
      }
    });
  } catch (e) { res.json({ ok: true, data: { phone: req.params.phone, name: req.params.phone, messages: [] } }); }
});

app.post("/api/inbox/read/:phone", async function(req, res) {
  try { await pool.query("UPDATE conversations SET unread=0 WHERE phone=$1", [req.params.phone]); } catch (e) {}
  res.json({ ok: true });
});

app.post("/api/inbox/send", async function(req, res) {
  var phone = req.body.phone;
  var text = req.body.text;
  if (!phone || !text) return res.status(400).json({ ok: false, error: "phone e text obrigatorios" });

  try {
    var r = await fetch("https://graph.facebook.com/" + CFG.waVersion + "/" + CFG.waPhoneId + "/messages", {
      method: "POST",
      headers: { Authorization: "Bearer " + CFG.waToken, "Content-Type": "application/json" },
      body: JSON.stringify({
        messaging_product: "whatsapp",
        to: phone,
        type: "text",
        text: { body: text }
      })
    });
    var data = await r.json();
    if (!r.ok) throw new Error((data.error && data.error.message) || "WA " + r.status);
    var msgId = (data.messages && data.messages[0] && data.messages[0].id) || null;

    await addOutgoingMsg(phone, null, text, null, msgId);

    res.json({ ok: true, messageId: msgId });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/api/inbox/unread", async function(req, res) {
  try {
    var r = await pool.query("SELECT COALESCE(SUM(unread), 0) as total FROM conversations");
    res.json({ ok: true, unread: parseInt(r.rows[0].total) || 0 });
  } catch (e) { res.json({ ok: true, unread: 0 }); }
});

// ===================== WA TEMPLATE MANAGEMENT =====================

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
    var bodyText = req.body.bodyText || "";
    var varMatches = bodyText.match(/\{\{(\d+)\}\}/g) || [];
    var varCount = 0;
    varMatches.forEach(function(m) {
      var num = parseInt(m.replace(/[{}]/g, ""));
      if (num > varCount) varCount = num;
    });

    var exampleValues = [];
    for (var i = 1; i <= varCount; i++) {
      if (i === 1) exampleValues.push("Maria");
      else if (i === 2) exampleValues.push("https://exemplo.com");
      else if (i === 3) exampleValues.push("https://exemplo.com/checkout");
      else exampleValues.push("valor" + i);
    }

    var bodyComponent = { type: "BODY", text: bodyText };
    if (exampleValues.length > 0) {
      bodyComponent.example = { body_text: [exampleValues] };
    }

    var body = {
      name: req.body.name,
      language: req.body.language || "pt_BR",
      category: req.body.category || "MARKETING",
      components: [bodyComponent]
    };
    if (req.body.footerText) {
      body.components.push({ type: "FOOTER", text: req.body.footerText });
    }
    if (req.body.buttonText && req.body.buttonUrl) {
      body.components.push({
        type: "BUTTONS",
        buttons: [{
          type: "URL",
          text: req.body.buttonText,
          url: req.body.buttonUrl,
          example: [req.body.buttonUrlExample || "https://ssjmodafitness.com.br"]
        }]
      });
    }
    var r = await fetch("https://graph.facebook.com/" + CFG.waVersion + "/" + CFG.wabaId + "/message_templates", {
      method: "POST",
      headers: { Authorization: "Bearer " + CFG.waToken, "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });
    var data = await r.json();
    if (!r.ok) {
      console.error("[TEMPLATE-ERRO] Status:", r.status);
      console.error("[TEMPLATE-ERRO] Resposta Meta:", JSON.stringify(data, null, 2));
      console.error("[TEMPLATE-ERRO] Body enviado:", JSON.stringify(body, null, 2));
      var errMsg = "Erro " + r.status;
      if (data.error) {
        errMsg = data.error.message || errMsg;
        if (data.error.error_user_title) errMsg = data.error.error_user_title + " — " + (data.error.error_user_msg || data.error.message);
        if (data.error.error_data && data.error.error_data.details) errMsg += " | Detalhe: " + data.error.error_data.details;
      }
      throw new Error(errMsg);
    }

    if (req.body.timing || req.body.tplType) {
      templateMeta[req.body.name] = {
        timing: req.body.timing || "",
        tplType: req.body.tplType || "carrinho",
        createdAt: new Date().toISOString()
      };
    }

    if (req.body.timing && (req.body.tplType === "carrinho" || !req.body.tplType)) {
      var timingStr = (req.body.timing || "").toLowerCase().trim();
      var hours = 0;
      if (timingStr.indexOf("min") !== -1) hours = parseFloat(timingStr) / 60;
      else if (timingStr.indexOf("h") !== -1) hours = parseFloat(timingStr);
      else if (timingStr.indexOf("d") !== -1) hours = parseFloat(timingStr) * 24;
      else hours = parseFloat(timingStr) || 0;
      if (hours > 0) {
        var vars = [];
        if (bodyText.indexOf("{{1}}") !== -1) vars.push("primeiro_nome");
        if (bodyText.indexOf("{{2}}") !== -1) {
          if (bodyText.toLowerCase().indexOf("cupom") !== -1 && bodyText.indexOf("{{3}}") !== -1) vars.push("cupom");
          else vars.push("link_carrinho");
        }
        if (bodyText.indexOf("{{3}}") !== -1) vars.push("link_carrinho");
        if (vars.length === 0) vars = ["primeiro_nome"];

        var minH = Math.max(0, hours * 0.5);
        var maxH = hours * 1.5;

        var existing = TEMPLATES.find(function(t) { return t.name === req.body.name; });
        if (existing) {
          existing.minH = minH; existing.maxH = maxH; existing.timing = req.body.timing; existing.vars = vars;
        } else {
          TEMPLATES.push({
            id: req.body.name, name: req.body.name, display: req.body.name.replace(/_/g, " "),
            timing: req.body.timing, minH: minH, maxH: maxH, lang: "pt_BR", vars: vars,
            hasButton: !!(req.body.buttonText && req.body.buttonUrl),
            preview: req.body.bodyText, custom: true
          });
          TEMPLATES.sort(function(a, b) { return a.minH - b.minH; });
        }
        console.log("[TEMPLATE] Registrado '" + req.body.name + "' com timing " + req.body.timing + " (" + hours + "h), range " + minH + "-" + maxH + "h");
      }
    }

    res.json({ ok: true, id: data.id, status: data.status, name: req.body.name, timing: req.body.timing || null });
  } catch (e) { res.status(400).json({ ok: false, error: e.message }); }
});

app.get("/api/template-meta", function(req, res) {
  res.json({ ok: true, data: templateMeta });
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

// ===================== START SERVER =====================
// IMPORTANTE: escutar na porta PRIMEIRO pra healthcheck do Railway passar
// Depois inicializar o banco em background

var server = app.listen(CFG.port, function() {
  console.log("SSJ Recovery rodando na porta " + CFG.port);
});

// Inicializar PostgreSQL em background (não bloqueia o healthcheck)
initDB().then(function() {
  console.log("[STARTUP] Banco pronto, servidor operacional");
}).catch(function(e) {
  console.error("[STARTUP] Erro ao conectar PostgreSQL:", e.message);
  console.error("[STARTUP] Servidor rodando sem persistencia!");
});

// Prevent crashes from unhandled errors
process.on("uncaughtException", function(err) { console.error("[CRASH-PREVENTED] uncaughtException:", err.message); });
process.on("unhandledRejection", function(err) { console.error("[CRASH-PREVENTED] unhandledRejection:", err && err.message ? err.message : err); });

// Graceful shutdown
process.on("SIGTERM", function() {
  console.log("[SHUTDOWN] Recebeu SIGTERM, fechando...");
  server.close(function() {
    pool.end().then(function() {
      console.log("[SHUTDOWN] Encerrado com sucesso");
      process.exit(0);
    });
  });
  // Force close after 5s
  setTimeout(function() { process.exit(0); }, 5000);
});
