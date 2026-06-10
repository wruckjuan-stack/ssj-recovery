/* ============================================================
   SSJ CRM — MÓDULO DE DISPARO EM LISTA (Convite Grupo VIP)
   ------------------------------------------------------------
   COMO USAR:
   1. Abra o seu server.js no GitHub.
   2. Cole TODO este bloco logo ANTES da linha:
        // ===================== START SERVER =====================
   3. Commit. O Railway redeploya sozinho. Pronto.

   Ele se auto-instala (cria as próprias tabelas no banco) e NÃO
   altera nenhuma rota que já existe. Usa as funções que já estão
   no seu server.js: pool, CFG, cron, fetch.
   ============================================================ */

// --- cria as tabelas do módulo (roda uma vez, sozinho) ---
(async function initBroadcast() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS broadcast_campaigns (
        campaign TEXT PRIMARY KEY,
        template_id TEXT,
        lang TEXT DEFAULT 'pt_BR',
        with_name BOOLEAN DEFAULT true,
        active BOOLEAN DEFAULT false,
        per_hour INT DEFAULT 40,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS broadcast_queue (
        id SERIAL PRIMARY KEY,
        campaign TEXT NOT NULL,
        phone TEXT NOT NULL,
        name TEXT,
        status TEXT DEFAULT 'pending',
        wa_message_id TEXT,
        error TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        sent_at TIMESTAMPTZ,
        UNIQUE(campaign, phone)
      );
      CREATE INDEX IF NOT EXISTS idx_bq_status ON broadcast_queue(campaign, status);
    `);
    console.log("[BROADCAST] Tabelas prontas");
  } catch (e) {
    console.error("[BROADCAST] Erro ao criar tabelas:", e.message);
  }
})();

// --- envia UM template de convite pra UM número ---
async function sendBroadcastWA(phone, templateName, firstName, lang, withName) {
  var components = [];
  if (withName) {
    components.push({ type: "body", parameters: [{ type: "text", text: String(firstName || "amiga") }] });
  }
  var payload = {
    messaging_product: "whatsapp",
    to: phone,
    type: "template",
    template: { name: templateName, language: { code: lang || "pt_BR" }, components: components }
  };
  var r = await fetch("https://graph.facebook.com/" + CFG.waVersion + "/" + CFG.waPhoneId + "/messages", {
    method: "POST",
    headers: { Authorization: "Bearer " + CFG.waToken, "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  var data = await r.json();
  if (!r.ok) {
    var err = new Error((data.error && data.error.message) || ("WA " + r.status));
    err.waCode = data.error && data.error.code;
    throw err;
  }
  return (data.messages && data.messages[0] && data.messages[0].id) || null;
}

// --- processa um lote de pendentes (usado pelo botão e pelo cron) ---
async function processBroadcastBatch(campaign, limit) {
  var cfg = await pool.query("SELECT * FROM broadcast_campaigns WHERE campaign=$1", [campaign]);
  if (cfg.rowCount === 0) return { sent: 0, failed: 0, done: true };
  var conf = cfg.rows[0];
  var q = await pool.query(
    "SELECT * FROM broadcast_queue WHERE campaign=$1 AND status='pending' ORDER BY id ASC LIMIT $2",
    [campaign, limit]
  );
  var sent = 0, failed = 0;
  for (var i = 0; i < q.rows.length; i++) {
    var c = q.rows[i];
    try {
      var msgId = await sendBroadcastWA(c.phone, conf.template_id, c.name, conf.lang, conf.with_name);
      await pool.query(
        "UPDATE broadcast_queue SET status='sent', wa_message_id=$1, sent_at=NOW(), error=NULL WHERE id=$2",
        [msgId, c.id]
      );
      sent++;
    } catch (e) {
      var m = (e.message || "").toLowerCase();
      var limitCodes = [131056, 130429, 131048, 80007];
      var isLimit = m.indexOf("limit") !== -1 || m.indexOf("rate") !== -1 ||
                    m.indexOf("too many") !== -1 || limitCodes.indexOf(e.waCode) !== -1;
      if (isLimit) {
        // Atingiu o limite do número: deixa pendente pra tentar no próximo ciclo e PARA o lote
        await pool.query("UPDATE broadcast_queue SET error=$1 WHERE id=$2", [e.message, c.id]);
        console.log("[BROADCAST] Limite atingido, pausando lote: " + e.message);
        break;
      }
      await pool.query("UPDATE broadcast_queue SET status='failed', error=$1 WHERE id=$2", [e.message, c.id]);
      failed++;
    }
    await new Promise(function(r) { setTimeout(r, 400); }); // respiro entre envios
  }
  var pend = await pool.query("SELECT COUNT(*) FROM broadcast_queue WHERE campaign=$1 AND status='pending'", [campaign]);
  var done = parseInt(pend.rows[0].count) === 0;
  if (done) await pool.query("UPDATE broadcast_campaigns SET active=false WHERE campaign=$1", [campaign]);
  return { sent: sent, failed: failed, done: done };
}

// --- CRON: a cada 5 min, goteja os disparos das campanhas ativas ---
cron.schedule("*/5 * * * *", async function() {
  try {
    var r = await pool.query("SELECT campaign, per_hour FROM broadcast_campaigns WHERE active=true");
    for (var i = 0; i < r.rows.length; i++) {
      var perHour = r.rows[i].per_hour || 40;
      var limit = Math.max(1, Math.round(perHour / 12)); // 12 ciclos por hora
      var out = await processBroadcastBatch(r.rows[i].campaign, limit);
      console.log("[BROADCAST-CRON] " + r.rows[i].campaign + " enviou=" + out.sent + " falhas=" + out.failed + (out.done ? " (CONCLUÍDO)" : ""));
    }
  } catch (e) { console.error("[BROADCAST-CRON] Erro:", e.message); }
});

// ===================== ROTAS DO DISPARO =====================

// Importa a lista pra fila
app.post("/api/broadcast/import", async function(req, res) {
  try {
    var campaign = req.body.campaign || "convite_grupo_vip";
    var templateId = req.body.templateId || campaign;
    var lang = req.body.lang || "pt_BR";
    var withName = req.body.withName !== false;
    var perHour = parseInt(req.body.perHour) || 40;
    var contacts = req.body.contacts || [];
    if (!contacts.length) return res.status(400).json({ ok: false, error: "Lista vazia" });

    await pool.query(
      `INSERT INTO broadcast_campaigns (campaign, template_id, lang, with_name, per_hour)
       VALUES ($1,$2,$3,$4,$5)
       ON CONFLICT (campaign) DO UPDATE SET template_id=$2, lang=$3, with_name=$4, per_hour=$5`,
      [campaign, templateId, lang, withName, perHour]
    );

    var imported = 0, dupes = 0, invalid = 0;
    for (var i = 0; i < contacts.length; i++) {
      var ph = String(contacts[i].phone || "").replace(/\D/g, "");
      if (ph.length < 12) { invalid++; continue; }
      var nm = contacts[i].name || "";
      try {
        var ins = await pool.query(
          "INSERT INTO broadcast_queue (campaign, phone, name) VALUES ($1,$2,$3) ON CONFLICT (campaign, phone) DO NOTHING",
          [campaign, ph, nm]
        );
        if (ins.rowCount > 0) imported++; else dupes++;
      } catch (e) { invalid++; }
    }
    res.json({ ok: true, imported: imported, dupes: dupes, invalid: invalid });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// Status / progresso
app.get("/api/broadcast/status", async function(req, res) {
  try {
    var campaign = req.query.campaign || "convite_grupo_vip";
    var r = await pool.query("SELECT status, COUNT(*) c FROM broadcast_queue WHERE campaign=$1 GROUP BY status", [campaign]);
    var counts = { pending: 0, sent: 0, failed: 0 };
    r.rows.forEach(function(row) { counts[row.status] = parseInt(row.c); });
    counts.total = counts.pending + counts.sent + counts.failed;
    var cfg = await pool.query("SELECT active, per_hour, template_id, with_name FROM broadcast_campaigns WHERE campaign=$1", [campaign]);
    var c = cfg.rows[0] || {};
    res.json({ ok: true, campaign: campaign, counts: counts, active: !!c.active, perHour: c.per_hour || 40, templateId: c.template_id, withName: c.with_name !== false });
  } catch (e) { res.json({ ok: false, error: e.message }); }
});

// Liga/desliga o envio automático + ajusta a taxa
app.post("/api/broadcast/auto", async function(req, res) {
  try {
    var campaign = req.body.campaign || "convite_grupo_vip";
    var active = !!req.body.active;
    var perHour = parseInt(req.body.perHour) || 40;
    await pool.query("UPDATE broadcast_campaigns SET active=$1, per_hour=$2 WHERE campaign=$3", [active, perHour, campaign]);
    res.json({ ok: true, active: active, perHour: perHour });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// Dispara 1 lote manualmente (botão)
app.post("/api/broadcast/send-batch", async function(req, res) {
  try {
    var campaign = req.body.campaign || "convite_grupo_vip";
    var limit = Math.min(parseInt(req.body.limit) || 20, 50);
    var out = await processBroadcastBatch(campaign, limit);
    res.json({ ok: true, sent: out.sent, failed: out.failed, done: out.done });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// Reagenda as falhas (volta pra pendente)
app.post("/api/broadcast/retry-failed", async function(req, res) {
  try {
    var campaign = req.body.campaign || "convite_grupo_vip";
    var r = await pool.query("UPDATE broadcast_queue SET status='pending', error=NULL WHERE campaign=$1 AND status='failed'", [campaign]);
    res.json({ ok: true, reset: r.rowCount });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// Inbox FILTRADO: só conversas de quem está na campanha (quem respondeu)
app.get("/api/broadcast/inbox", async function(req, res) {
  try {
    var campaign = req.query.campaign || "convite_grupo_vip";
    var r = await pool.query(`
      SELECT c.phone, c.name, c.unread, c.last_message_at,
        (SELECT text FROM messages WHERE phone=c.phone ORDER BY created_at DESC LIMIT 1) AS last_message
      FROM conversations c
      JOIN broadcast_queue bq ON bq.phone = c.phone AND bq.campaign = $1
      ORDER BY c.last_message_at DESC
      LIMIT 200
    `, [campaign]);
    res.json({ ok: true, data: r.rows.map(function(c) {
      return { phone: c.phone, name: c.name, lastMessage: c.last_message || "", lastMessageAt: c.last_message_at, unread: c.unread || 0 };
    }) });
  } catch (e) { res.json({ ok: true, data: [] }); }
});
