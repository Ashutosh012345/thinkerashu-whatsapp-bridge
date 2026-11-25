const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const pino = require('pino');
const fs = require('fs');
const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion
} = require('@whiskeysockets/baileys');

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws' });

let sock = null;
let connectionStatus = 'Disconnected';
const processedIds = new Set();
const lastReplyTime = new Map();

function broadcast(data) {
  const payload = JSON.stringify(data);
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  });
}

wss.on('connection', (ws) => {
  wss.clients.forEach((client) => {
    if (client !== ws && client.readyState === WebSocket.OPEN) {
      client.close();
    }
  });
  ws.send(JSON.stringify({ type: 'status', status: connectionStatus }));
});

async function startWhatsApp() {
  const { state, saveCreds } = await useMultiFileAuthState('./auth_info');
  const { version } = await fetchLatestBaileysVersion();

  sock = makeWASocket({
    version,
    logger: pino({ level: 'silent' }),
    auth: state,
    printQRInTerminal: false,
    browser: ['Ubuntu', 'Chrome', '20.0.04']
  });

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', (update) => {
    const { connection, lastDisconnect } = update;
    if (connection === 'open') {
      connectionStatus = 'Connected';
      broadcast({ type: 'status', status: 'Connected' });
      console.log('WhatsApp Connected!');
    } else if (connection === 'close') {
      const code = lastDisconnect?.error?.output?.statusCode;
      connectionStatus = 'Disconnected';
      broadcast({ type: 'status', status: 'Disconnected' });
      if (code !== DisconnectReason.loggedOut) {
        setTimeout(startWhatsApp, 3000);
      }
    }
  });

  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    if (type !== 'notify') return;

    for (const msg of messages) {
      if (!msg.message || msg.key.fromMe) continue;

      const msgId = msg.key.id;
      if (processedIds.has(msgId)) continue;
      processedIds.add(msgId);
      setTimeout(() => processedIds.delete(msgId), 60000);

      const text =
        msg.message.conversation ||
        msg.message.extendedTextMessage?.text ||
        '';
      if (!text.trim()) continue;

      if (msg.key.remoteJid === 'status@broadcast') {
        broadcast({
          type: 'status.new',
          sender: msg.pushName || msg.key.participant,
          caption: text,
          timestamp: Date.now()
        });
      } else {
        broadcast({
          type: 'message.new',
          id: msgId,
          jid: msg.key.remoteJid,
          sender: msg.pushName || msg.key.remoteJid,
          text,
          timestamp: Date.now()
        });
      }
    }
  });
}

app.get('/', (req, res) => {
  res.json({ status: connectionStatus, server: 'ThinkerASHU Bridge Running' });
});

app.post('/api/pair', async (req, res) => {
  try {
    const rawPhone = String(req.body.phoneNumber || req.body.phone || '').replace(/\D/g, '');
    if (!rawPhone) return res.status(400).json({ error: 'Valid phone number required' });

    if (!sock) await startWhatsApp();
    if (sock.authState.creds.registered) {
      return res.json({ status: 'Already Connected', code: null });
    }

    connectionStatus = 'Pairing';
    broadcast({ type: 'status', status: 'Pairing' });

    const code = await sock.requestPairingCode(rawPhone);
    const formatted = code?.match(/.{1,4}/g)?.join('-') || code;
    res.json({ code: formatted, pairingCode: formatted });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/send-message', async (req, res) => {
  try {
    const { recipient, messageText, jid, text } = req.body;
    const target = jid || (recipient.includes('@') ? recipient : `${recipient.replace(/\D/g, '')}@s.whatsapp.net`);

    const now = Date.now();
    if (now - (lastReplyTime.get(target) || 0) < 5000) {
      return res.json({ success: true, skipped: 'cooldown' });
    }
    lastReplyTime.set(target, now);

    await sock.sendMessage(target, { text: messageText || text });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/groups', async (req, res) => {
  try {
    const groups = await sock.groupFetchAllParticipating();
    res.json({ groups: Object.values(groups) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/logout', async (req, res) => {
  try {
    if (sock) await sock.logout();
    fs.rmSync('./auth_info', { recursive: true, force: true });
    connectionStatus = 'Disconnected';
    broadcast({ type: 'status', status: 'Disconnected' });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Bridge server running on port ${PORT}`);
  startWhatsApp();
});
