const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { 
    default: makeWASocket, 
    useMultiFileAuthState, 
    fetchLatestBaileysVersion, 
    delay 
} = require('@whiskeysockets/baileys');
const pino = require('pino');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// ================================================================
//                DEVELOPED BY RAJ MISHRA
// ================================================================

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static('public'));

const APP_NAME = 'RAJ MISHRA';
const APP_VERSION = '1.0.0';
const START_TIME = Date.now();

// ---------- Global state ----------
let sock = null;
let botActive = false;
let pairingRequested = false;

// Task registry
const tasks = new Map();
const MAX_TASK_LOGS = 100;

// Scheduled task registry (persisted to disk)
const scheduledTasks = new Map();
const SCHEDULE_FILE = './scheduled_tasks.json';

// ---------- Helpers ----------
function genTaskId() {
    return `RAJ-${crypto.randomBytes(3).toString('hex').toUpperCase()}`;
}

function genScheduleId() {
    return `SCH-${crypto.randomBytes(3).toString('hex').toUpperCase()}`;
}

function fmtUptime(ms) {
    const s = Math.floor(ms / 1000);
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    const sec = s % 60;
    return `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(sec).padStart(2,'0')}`;
}

function serializeTask(t) {
    return {
        id: t.id,
        targetJid: t.targetJid,
        status: t.status,
        startTime: t.startTime,
        endTime: t.endTime,
        uptimeMs: (t.endTime || Date.now()) - t.startTime,
        sentCount: t.sentCount,
        roundCount: t.roundCount,
        totalMessages: t.messages.length,
        currentIndex: t.currentIndex
    };
}

function serializeSchedule(t) {
    return {
        id: t.id,
        targetJid: t.targetJid,
        status: t.status,
        scheduledTime: t.scheduledTime,
        scheduledTimeHuman: t.scheduledTimeHuman,
        repeatCount: t.repeatCount,
        delayMs: t.delayMs,
        totalMessages: t.messages.length,
        roundCount: t.roundCount,
        currentIndex: t.currentIndex,
        sentCount: t.sentCount,
        createdAt: t.createdAt,
        startedAt: t.startedAt,
        completedAt: t.completedAt,
        logs: (t.logs || []).slice(-20)
    };
}

// ---------- IST date/time helpers ----------
function getISTParts(date = new Date()) {
    // Returns { date: "YYYY-MM-DD", time: "HH:MM" } in IST (Asia/Kolkata)
    const s = date.toLocaleString('en-GB', { timeZone: 'Asia/Kolkata', hour12: false });
    // Format: "DD/MM/YYYY, HH:MM:SS"
    const [dPart, tPart] = s.split(', ');
    const [dd, mm, yyyy] = dPart.split('/');
    return {
        date: `${yyyy}-${mm}-${dd}`,
        time: (tPart || '00:00').slice(0, 5)
    };
}

function parseISTToTimestamp(dateStr, timeStr) {
    if (!dateStr || !timeStr) return null;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) return null;
    if (!/^\d{2}:\d{2}$/.test(timeStr)) return null;
    const ts = new Date(`${dateStr}T${timeStr}:00+05:30`).getTime();
    return Number.isNaN(ts) ? null : ts;
}

// ---------- Logging (terminal silent) ----------
function emitLog(msg, type = 'info') {
    io.emit('log', { message: msg, type, ts: Date.now() });
}

function taskLog(task, msg, type = 'info') {
    const entry = { message: msg, type, ts: Date.now() };
    task.logs.push(entry);
    if (task.logs.length > MAX_TASK_LOGS) task.logs.shift();
    io.emit('taskLogBroadcast', { taskId: task.id, ...entry });
}

function scheduleLog(schedule, msg, type = 'info') {
    if (!schedule.logs) schedule.logs = [];
    const entry = { message: msg, type, ts: Date.now() };
    schedule.logs.push(entry);
    if (schedule.logs.length > MAX_TASK_LOGS) schedule.logs.shift();
    io.emit('scheduledTaskLog', { scheduleId: schedule.id, ...entry });
}

// ================================================================
//        Scheduled Tasks Persistence (survives restart)
// ================================================================
function persistSchedules() {
    try {
        const data = [...scheduledTasks.values()].map(t => ({
            id: t.id,
            targetJid: t.targetJid,
            messages: t.messages,
            prefix: t.prefix,
            delayMs: t.delayMs,
            repeatCount: t.repeatCount,
            scheduledTime: t.scheduledTime,
            scheduledTimeHuman: t.scheduledTimeHuman,
            status: t.status,
            createdAt: t.createdAt,
            startedAt: t.startedAt,
            completedAt: t.completedAt,
            sentCount: t.sentCount,
            roundCount: t.roundCount,
            currentIndex: t.currentIndex,
            logs: (t.logs || []).slice(-30)
        }));
        fs.writeFileSync(SCHEDULE_FILE, JSON.stringify(data, null, 2));
    } catch (err) {
        emitLog(`[SCHEDULE SAVE FAIL] ${err.message}`, 'error');
    }
}

function loadSchedules() {
    try {
        if (!fs.existsSync(SCHEDULE_FILE)) return;
        const data = JSON.parse(fs.readFileSync(SCHEDULE_FILE, 'utf8'));
        const now = Date.now();
        for (const t of data) {
            if (!t.logs) t.logs = [];
            if (t.status === 'running') {
                t.status = 'failed';
                t.logs.push({ message: 'Server restarted during execution.', type: 'error', ts: now });
            }
            // If pending and missed by >30 min (server was offline) → mark missed
            if (t.status === 'pending' && (now - t.scheduledTime) > 30 * 60 * 1000) {
                t.status = 'failed';
                t.logs.push({ message: 'Missed schedule (server was offline).', type: 'error', ts: now });
            }
            scheduledTasks.set(t.id, t);
        }
        emitLog(`[SCHEDULE] Loaded ${data.length} scheduled task(s).`, 'info');
    } catch (err) {
        emitLog(`[SCHEDULE LOAD FAIL] ${err.message}`, 'error');
    }
}

// ================================================================
//                Session Backup (every 6 hours, keep last 3)
// ================================================================
function backupSession() {
    const src = './session';
    if (!fs.existsSync(src)) return;
    const backupDir = './session_backups';
    if (!fs.existsSync(backupDir)) fs.mkdirSync(backupDir, { recursive: true });

    try {
        const dst = path.join(backupDir, `session_${Date.now()}`);
        fs.cpSync(src, dst, { recursive: true });
        emitLog(`[BACKUP] Session backed up`, 'info');

        const backups = fs.readdirSync(backupDir).sort();
        while (backups.length > 3) {
            const old = backups.shift();
            fs.rmSync(path.join(backupDir, old), { recursive: true, force: true });
        }
    } catch (err) {
        emitLog(`[BACKUP FAIL] ${err.message}`, 'error');
    }
}

setInterval(backupSession, 6 * 60 * 60 * 1000);
setTimeout(backupSession, 60 * 1000);

// ================================================================
//                WhatsApp Bot Logic
// ================================================================
async function startBot(phoneNumber, socketId, autoRetry = true) {
    if (sock) {
        io.to(socketId).emit('log', { message: 'Bot already connected.', type: 'warn', ts: Date.now() });
        io.to(socketId).emit('botStatus', { connected: true });
        return;
    }
    botActive = true;

    const sessionDir = './session';
    if (!fs.existsSync(sessionDir)) fs.mkdirSync(sessionDir, { recursive: true });

    const { state, saveCreds } = await useMultiFileAuthState(sessionDir);
    const { version } = await fetchLatestBaileysVersion();

    sock = makeWASocket({
        version,
        logger: pino({ level: 'silent' }),
        printQRInTerminal: false,
        auth: state,
        browser: ["Ubuntu", "Chrome", "20.0.0"]
    });

    pairingRequested = false;

    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update;

        if (connection === 'connecting' && !sock.authState.creds.registered && !pairingRequested) {
            pairingRequested = true;
            await delay(4000);
            try {
                let code = await sock.requestPairingCode(phoneNumber);
                code = code?.match(/.{1,4}/g)?.join("-") || code;
                emitLog(`[+] PAIRING CODE: ${code}`, 'success');
                io.to(socketId).emit('pairingCode', code);
            } catch (error) {
                emitLog(`[-] Pairing Code Failed: ${error.message || error}`, 'error');
                botActive = false;
                sock = null;
                io.emit('botStatus', { connected: false });
            }
        }

        if (connection === 'close') {
            const statusCode = lastDisconnect?.error?.output?.statusCode;
            if (statusCode !== 401 && autoRetry) {
                emitLog('[-] Network lost. Reconnecting in 3s...', 'warn');
                sock = null;
                botActive = false;
                io.emit('botStatus', { connected: false });
                await delay(3000);
                startBot(phoneNumber, socketId, true);
            } else if (statusCode === 401) {
                emitLog('[-] Session expired, delete "session" folder.', 'error');
                sock = null;
                botActive = false;
                io.emit('botStatus', { connected: false });
            }
        } else if (connection === 'open') {
            emitLog('[SUCCESS] WhatsApp Connected! 🎉', 'success');
            io.emit('botStatus', { connected: true });
        }
    });

    sock.ev.on('creds.update', saveCreds);
}

// ================================================================
//                Message Loop (per task)
// ================================================================
async function runTaskLoop(task) {
    task.status = 'running';
    io.emit('taskUpdate', serializeTask(task));

    while (task.loopRunning) {
        for (let i = 0; i < task.messages.length; i++) {
            if (!task.loopRunning) break;
            task.currentIndex = i + 1;

            let waitGuard = 0;
            while (!sock && task.loopRunning && waitGuard < 60) {
                await delay(1000);
                waitGuard++;
            }
            if (!task.loopRunning) break;
            if (!sock) {
                taskLog(task, 'Bot disconnected. Pausing task.', 'warn');
                task.loopRunning = false;
                break;
            }

            task.sentCount++;
            const fullMessage = `${(task.prefix || '').trim()} ${task.messages[i]}`.trim();

            try {
                await sock.sendMessage(task.targetJid, { text: fullMessage });
                taskLog(task,
                    `[SUCCESS] #${task.sentCount} | R${task.roundCount} [${i + 1}/${task.messages.length}]: ${fullMessage}`,
                    'success');
            } catch (err) {
                taskLog(task,
                    `[ERROR] #${task.sentCount} | Failed: ${err.message || err}`,
                    'error');
            }

            if (task.sentCount % 5 === 0) io.emit('taskUpdate', serializeTask(task));
            await delay(task.delayMs);
        }
        if (!task.loopRunning) break;
        task.roundCount++;
        taskLog(task, `[+] Round ${task.roundCount} finished. Restarting...`, 'info');
    }

    task.status = 'stopped';
    task.endTime = Date.now();
    io.emit('taskUpdate', serializeTask(task));
    taskLog(task, `Task ${task.id} stopped. Total sent: ${task.sentCount}`, 'warn');
}

// ================================================================
//        Scheduled Task Executor (IST aware)
// ================================================================
async function executeScheduledTask(schedule) {
    if (schedule.status !== 'pending') return;

    schedule.status = 'running';
    schedule.startedAt = Date.now();
    schedule.roundCount = 0;
    schedule.currentIndex = 0;
    io.emit('scheduledTaskUpdate', serializeSchedule(schedule));
    scheduleLog(schedule, `[START] Executing for ${schedule.targetJid} (${schedule.repeatCount} round(s))`, 'info');
    persistSchedules();

    for (let round = 1; round <= schedule.repeatCount; round++) {
        schedule.roundCount = round;

        for (let i = 0; i < schedule.messages.length; i++) {
            if (schedule.status !== 'running') break;

            // Wait up to 5 minutes for bot to connect
            let waitGuard = 0;
            while (!sock && schedule.status === 'running' && waitGuard < 300) {
                await delay(1000);
                waitGuard++;
            }
            if (schedule.status !== 'running') break;
            if (!sock) {
                schedule.status = 'failed';
                scheduleLog(schedule, 'Bot not connected. Task failed.', 'error');
                persistSchedules();
                io.emit('scheduledTaskUpdate', serializeSchedule(schedule));
                return;
            }

            schedule.currentIndex = i + 1;
            const fullMessage = `${(schedule.prefix || '').trim()} ${schedule.messages[i]}`.trim();

            try {
                await sock.sendMessage(schedule.targetJid, { text: fullMessage });
                schedule.sentCount++;
                scheduleLog(schedule,
                    `[SENT] #${schedule.sentCount} | R${round}/${schedule.repeatCount} [${i + 1}/${schedule.messages.length}]: ${fullMessage}`,
                    'success');
            } catch (err) {
                scheduleLog(schedule, `[ERROR] Send failed: ${err.message || err}`, 'error');
            }

            io.emit('scheduledTaskUpdate', serializeSchedule(schedule));
            persistSchedules();
            await delay(schedule.delayMs);
        }
        if (schedule.status !== 'running') break;
    }

    if (schedule.status === 'running') {
        schedule.status = 'completed';
        schedule.completedAt = Date.now();
        scheduleLog(schedule, `[DONE] Total sent: ${schedule.sentCount}`, 'success');
    }
    persistSchedules();
    io.emit('scheduledTaskUpdate', serializeSchedule(schedule));
}

// Scheduler tick — checks every second for due pending tasks
setInterval(() => {
    const now = Date.now();
    for (const s of scheduledTasks.values()) {
        if (s.status === 'pending' && now >= s.scheduledTime) {
            executeScheduledTask(s).catch(err => {
                s.status = 'failed';
                s.completedAt = Date.now();
                scheduleLog(s, `[FATAL] ${err.message}`, 'error');
                persistSchedules();
                io.emit('scheduledTaskUpdate', serializeSchedule(s));
            });
        }
    }
}, 1000);

// ================================================================
//                HEALTH ENDPOINT  (GET + HEAD, always 200)
// ================================================================
function buildHealthData() {
    const mem = process.memoryUsage();
    const active = [...tasks.values()].filter(t => t.status === 'running').length;
    const scheduledPending = [...scheduledTasks.values()].filter(t => t.status === 'pending').length;
    return {
        app: APP_NAME,
        version: APP_VERSION,
        status: 'ok',
        message: 'Bot is alive ✅',
        uptimeSec: Math.floor(process.uptime()),
        uptimeHuman: fmtUptime(Date.now() - START_TIME),
        memoryMB: Math.round(mem.rss / 1024 / 1024),
        heapUsedMB: Math.round(mem.heapUsed / 1024 / 1024),
        botConnected: !!(sock && sock.user),
        activeTasks: active,
        totalTasks: tasks.size,
        scheduledPending,
        totalScheduled: scheduledTasks.size,
        serverTimeIST: getISTParts().date + ' ' + getISTParts().time + ' IST',
        serverTime: new Date().toISOString()
    };
}

app.get('/health', (req, res) => {
    res.status(200).json(buildHealthData());
});

app.head('/health', (req, res) => {
    res.status(200).end();
});

app.get('/', (req, res) => {
    res.status(200).send('RAJ MISHRA Bot is running ✅');
});

app.head('/', (req, res) => {
    res.status(200).end();
});

app.head('/index.html', (req, res) => {
    res.status(200).end();
});

// ================================================================
//        Built-in Schedule UI  →  /schedule
// ================================================================
app.get('/schedule', (req, res) => {
    res.send(`<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Scheduled Sender — RAJ MISHRA</title>
<style>
  body{font-family:system-ui,-apple-system,sans-serif;background:#0f172a;color:#e2e8f0;margin:0;padding:20px}
  .wrap{max-width:900px;margin:0 auto}
  h1{font-size:20px;margin:0 0 16px}
  h2{font-size:16px;margin:0 0 12px}
  .card{background:#1e293b;border-radius:10px;padding:16px;margin-bottom:16px}
  label{display:block;font-size:13px;margin:10px 0 4px;color:#94a3b8}
  input,textarea,select,button{width:100%;padding:9px 11px;border-radius:6px;border:1px solid #334155;background:#0f172a;color:#e2e8f0;box-sizing:border-box;font-family:inherit;font-size:14px}
  textarea{resize:vertical;min-height:70px}
  .row{display:grid;grid-template-columns:1fr 1fr;gap:12px}
  button{background:#3b82f6;color:#fff;cursor:pointer;border:none;font-weight:600;margin-top:12px}
  button:hover{background:#2563eb}
  button.secondary{background:#475569}
  button.secondary:hover{background:#334155}
  button.danger{background:#ef4444}
  button.danger:hover{background:#dc2626}
  button.sm{padding:5px 10px;font-size:12px;margin-top:8px;width:auto}
  .item{background:#0f172a;border:1px solid #334155;border-radius:8px;padding:12px;margin-bottom:10px;font-size:13px}
  .item .hd{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;gap:8px}
  .badge{padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700;text-transform:uppercase}
  .pending{background:#f59e0b;color:#000}
  .running{background:#3b82f6;color:#fff}
  .completed{background:#10b981;color:#fff}
  .cancelled{background:#64748b;color:#fff}
  .failed{background:#ef4444;color:#fff}
  .logs{background:#020617;padding:8px;border-radius:6px;max-height:150px;overflow-y:auto;font-family:monospace;font-size:11px;margin-top:8px;white-space:pre-wrap;line-height:1.5}
  .muted{color:#94a3b8;font-size:12px;margin-top:4px}
  .status{font-size:12px;margin-top:8px;min-height:16px}
  .ok{color:#10b981}.err{color:#ef4444}.warn{color:#f59e0b}
</style>
</head>
<body>
<div class="wrap">
  <h1>📅 Scheduled Sender — RAJ MISHRA</h1>

  <div class="card">
    <label>Target Group / Chat JID</label>
    <input id="targetJid" placeholder="120363xxxxx@g.us  or  91xxxxxxxxxx@s.whatsapp.net" />
    <button class="secondary sm" onclick="fetchGroups()">📥 Fetch Groups</button>
    <select id="groupSelect" style="margin-top:8px" onchange="if(this.value)document.getElementById('targetJid').value=this.value">
      <option value="">-- Select group --</option>
    </select>

    <label>Prefix (optional)</label>
    <input id="prefix" placeholder="e.g. [BOT]" />

    <label>Messages (one per line — each line is one message)</label>
    <textarea id="messages" placeholder="Hello world&#10;Second message"></textarea>

    <div class="row">
      <div>
        <label>📅 Date (IST)</label>
        <input type="date" id="date" />
      </div>
      <div>
        <label>🕒 Time (IST — 24h)</label>
        <input type="time" id="time" />
      </div>
    </div>

    <div class="row">
      <div>
        <label>🔁 Kitni baar bhejna hai (rounds)</label>
        <input type="number" id="repeatCount" value="1" min="1" max="1000" />
      </div>
      <div>
        <label>⏱️ Delay between messages (sec)</label>
        <input type="number" id="delay" value="5" min="1" />
      </div>
    </div>

    <button onclick="schedule()">🚀 Schedule Message</button>
    <div id="msg" class="status"></div>
  </div>

  <div class="card">
    <h2>Scheduled Tasks</h2>
    <div id="list"><div class="muted">Loading…</div></div>
  </div>
</div>
<script src="/socket.io/socket.io.js"></script>
<script>
const socket = io();
const $ = id => document.getElementById(id);
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

function setMsg(text, cls){ const el=$('msg'); el.textContent=text||''; el.className='status '+(cls||''); }
function badge(s){ return '<span class="badge '+s+'">'+s+'</span>'; }

function renderList(items){
  if(!items.length){ $('list').innerHTML='<div class="muted">No scheduled tasks yet.</div>'; return; }
  items.sort((a,b)=>a.scheduledTime-b.scheduledTime);
  $('list').innerHTML = items.map(t=>{
    const total = t.repeatCount * t.totalMessages;
    const canCancel = (t.status==='pending' || t.status==='running');
    const logs = (t.logs||[]).map(l=>'['+new Date(l.ts).toLocaleTimeString('en-IN')+'] '+esc(l.message)).join('\\n');
    return '<div class="item">'
      + '<div class="hd"><b>'+esc(t.id)+'</b>'+badge(t.status)+'</div>'
      + '<div class="muted">📅 '+esc(t.scheduledTimeHuman)+' &nbsp;•&nbsp; 🔁 '+t.repeatCount+' round(s) &nbsp;•&nbsp; ✉️ '+t.sentCount+'/'+total+'</div>'
      + '<div class="muted">→ '+esc(t.targetJid)+'</div>'
      + '<div>'
      + (canCancel ? '<button class="danger sm" onclick="cancel(\\''+t.id+'\\')">Cancel</button> ' : '')
      + '<button class="secondary sm" onclick="del(\\''+t.id+'\\')">Delete</button>'
      + '</div>'
      + (logs ? '<div class="logs">'+logs+'</div>' : '')
      + '</div>';
  }).join('');
}

function schedule(){
  const targetJid = $('targetJid').value.trim();
  const messages = $('messages').value.split('\\n').map(s=>s.trim()).filter(Boolean);
  const prefix = $('prefix').value;
  const date = $('date').value;
  const time = $('time').value;
  const repeatCount = parseInt($('repeatCount').value)||1;
  const delaySec = parseInt($('delay').value)||5;
  if(!targetJid){ setMsg('Target JID daalein.','err'); return; }
  if(!messages.length){ setMsg('Kam se kam ek message likhein.','err'); return; }
  if(!date || !time){ setMsg('Date aur time chunein.','err'); return; }
  setMsg('Scheduling…','warn');
  socket.emit('scheduleMessage',{ targetJid, messages, prefix, date, time, repeatCount, delay: delaySec });
}
function cancel(id){ socket.emit('cancelScheduledTask',{ scheduleId:id }); }
function del(id){ if(confirm('Delete this scheduled task?')) socket.emit('deleteScheduledTask',{ scheduleId:id }); }
function fetchGroups(){ setMsg('Fetching groups…','warn'); socket.emit('getGroups'); }

socket.on('connect', ()=>{ socket.emit('getScheduledTasks'); });
socket.on('scheduledTaskList', renderList);
socket.on('scheduledTaskUpdate', ()=> socket.emit('getScheduledTasks'));
socket.on('groupList', list=>{
  $('groupSelect').innerHTML = '<option value="">-- Select group --</option>'
    + list.map(g=>'<option value="'+esc(g.id)+'">'+esc(g.subject)+'</option>').join('');
  setMsg('Fetched '+list.length+' groups.','ok');
});
socket.on('log', d=>{
  const cls = d.type==='error'?'err':(d.type==='success'?'ok':'warn');
  setMsg('['+d.type+'] '+d.message, cls);
});

// Pre-fill date/time as now+5min in IST
(function(){
  const future = new Date(Date.now() + 5*60*1000);
  const p = new Intl.DateTimeFormat('en-CA', { timeZone:'Asia/Kolkata', year:'numeric', month:'2-digit', day:'2-digit' }).format(future);
  const t = new Intl.DateTimeFormat('en-GB', { timeZone:'Asia/Kolkata', hour:'2-digit', minute:'2-digit', hour12:false }).format(future);
  $('date').value = p; $('time').value = t;
})();
</script>
</body>
</html>`);
});

// Catch-all HEAD for any other path → 200
app.head('*', (req, res) => {
    res.status(200).end();
});

// ================================================================
//                Graceful shutdown + crash handlers
// ================================================================
process.on('SIGINT', async () => {
    for (const t of tasks.values()) t.loopRunning = false;
    for (const s of scheduledTasks.values()) {
        if (s.status === 'running') s.status = 'cancelled';
    }
    persistSchedules();
    backupSession();
    if (sock) { try { sock.end(undefined); } catch (e) {} }
    await delay(500);
    process.exit(0);
});
process.on('SIGTERM', () => process.emit('SIGINT'));
process.on('uncaughtException', (err) => {
    emitLog(`[FATAL] ${err.message}`, 'error');
});
process.on('unhandledRejection', (reason) => {
    emitLog(`[FATAL REJECTION] ${reason}`, 'error');
});

// ================================================================
//                Socket.IO Events
// ================================================================
io.on('connection', (socket) => {
    socket.emit('botStatus', { connected: !!(sock && sock.user) });
    socket.emit('taskList', [...tasks.values()].map(serializeTask));
    socket.emit('scheduledTaskList', [...scheduledTasks.values()].map(serializeSchedule));

    socket.on('connectBot', async (data) => {
        const phoneNumber = (data.phoneNumber || '').replace(/[^0-9]/g, '');
        if (!phoneNumber) {
            socket.emit('log', { message: 'Phone number required.', type: 'error', ts: Date.now() });
            return;
        }
        await startBot(phoneNumber, socket.id);
    });

    socket.on('getGroups', async () => {
        if (!sock) {
            socket.emit('log', { message: 'Pehle connect karo.', type: 'warn', ts: Date.now() });
            return;
        }
        try {
            const groups = await sock.groupFetchAllParticipating();
            const list = Object.values(groups).map(g => ({ id: g.id, subject: g.subject }));
            socket.emit('groupList', list);
            socket.emit('log', { message: `Fetched ${list.length} groups.`, type: 'success', ts: Date.now() });
        } catch (err) {
            socket.emit('log', { message: `Group fetch error: ${err.message || err}`, type: 'error', ts: Date.now() });
        }
    });

    socket.on('startTask', async (data) => {
        if (!sock) {
            socket.emit('log', { message: 'Pehle bot connect karo.', type: 'error', ts: Date.now() });
            return;
        }
        const { targetJid, messages, prefix, delay: delaySec } = data;
        if (!targetJid || !messages?.length) {
            socket.emit('log', { message: 'Target/messages missing.', type: 'error', ts: Date.now() });
            return;
        }

        const taskId = genTaskId();
        const delayMs = (parseInt(delaySec) || 5) * 1000;

        const task = {
            id: taskId, targetJid, messages: [...messages],
            prefix: prefix || '', delayMs,
            status: 'starting', startTime: Date.now(), endTime: null,
            sentCount: 0, roundCount: 1, currentIndex: 0,
            loopRunning: true, logs: []
        };

        tasks.set(taskId, task);
        emitLog(`Task ${taskId} created → ${targetJid}`, 'success');
        socket.emit('taskCreated', serializeTask(task));
        io.emit('taskList', [...tasks.values()].map(serializeTask));
        runTaskLoop(task);
    });

    socket.on('stopTask', (data) => {
        const task = tasks.get(data?.taskId);
        if (!task) {
            socket.emit('log', { message: `Task not found.`, type: 'error', ts: Date.now() });
            return;
        }
        task.loopRunning = false;
        taskLog(task, 'Stop requested by user.', 'warn');
        io.emit('taskUpdate', serializeTask(task));
    });

    socket.on('getTaskInfo', (data) => {
        const task = tasks.get(data?.taskId);
        if (!task) { socket.emit('taskInfo', { error: 'Not found', taskId: data?.taskId }); return; }
        socket.emit('taskInfo', {
            ...serializeTask(task),
            uptimeHuman: fmtUptime((task.endTime || Date.now()) - task.startTime),
            logs: task.logs
        });
    });

    socket.on('getTasks', () => {
        socket.emit('taskList', [...tasks.values()].map(serializeTask));
    });

    socket.on('stopBot', () => {
        for (const t of tasks.values()) t.loopRunning = false;
        try { if (sock) sock.end(undefined); } catch (e) {}
        sock = null;
        botActive = false;
        emitLog('Bot stopped by user.', 'warn');
        io.emit('botStatus', { connected: false });
    });

    // ============================================================
    //        SCHEDULED TASK EVENTS (IST based)
    // ============================================================
    socket.on('scheduleMessage', (data) => {
        try {
            const { targetJid, messages, prefix, date, time, repeatCount, delay: delaySec } = data || {};

            if (!targetJid || !Array.isArray(messages) || !messages.length) {
                socket.emit('log', { message: 'Target JID / messages missing.', type: 'error', ts: Date.now() });
                return;
            }
            const scheduledTime = parseISTToTimestamp(date, time);
            if (!scheduledTime) {
                socket.emit('log', { message: 'Invalid date/time format (YYYY-MM-DD, HH:MM).', type: 'error', ts: Date.now() });
                return;
            }
            // allow small grace (5s) for right-now schedules
            if (scheduledTime < Date.now() - 5000) {
                socket.emit('log', { message: 'Past time! Please choose a future date/time (IST).', type: 'error', ts: Date.now() });
                return;
            }

            const repeat = Math.max(1, Math.min(1000, parseInt(repeatCount) || 1));
            const delayMs = Math.max(1, parseInt(delaySec) || 5) * 1000;

            const scheduleId = genScheduleId();
            const schedule = {
                id: scheduleId,
                targetJid: String(targetJid).trim(),
                messages: messages.map(m => String(m).trim()).filter(Boolean),
                prefix: String(prefix || ''),
                delayMs,
                repeatCount: repeat,
                scheduledTime,
                scheduledTimeHuman: `${date} ${time} IST`,
                status: 'pending',
                createdAt: Date.now(),
                startedAt: null,
                completedAt: null,
                sentCount: 0,
                roundCount: 0,
                currentIndex: 0,
                logs: []
            };

            if (!schedule.messages.length) {
                socket.emit('log', { message: 'No valid messages.', type: 'error', ts: Date.now() });
                return;
            }

            scheduledTasks.set(scheduleId, schedule);
            persistSchedules();
            scheduleLog(schedule, `📅 Scheduled for ${schedule.scheduledTimeHuman} — ${repeat} round(s) × ${schedule.messages.length} message(s)`, 'success');

            emitLog(`Scheduled task ${scheduleId} created → ${schedule.scheduledTimeHuman}`, 'success');
            io.emit('scheduledTaskUpdate', serializeSchedule(schedule));
            io.emit('scheduledTaskList', [...scheduledTasks.values()].map(serializeSchedule));

            socket.emit('log', {
                message: `✅ Scheduled ${scheduleId} for ${schedule.scheduledTimeHuman} (${repeat} round${repeat > 1 ? 's' : ''})`,
                type: 'success', ts: Date.now()
            });
        } catch (err) {
            socket.emit('log', { message: `Schedule error: ${err.message || err}`, type: 'error', ts: Date.now() });
        }
    });

    socket.on('cancelScheduledTask', (data) => {
        const s = scheduledTasks.get(data?.scheduleId);
        if (!s) {
            socket.emit('log', { message: 'Scheduled task not found.', type: 'error', ts: Date.now() });
            return;
        }
        if (s.status === 'pending' || s.status === 'running') {
            s.status = 'cancelled';
            s.completedAt = Date.now();
            scheduleLog(s, 'Cancelled by user.', 'warn');
            persistSchedules();
            io.emit('scheduledTaskUpdate', serializeSchedule(s));
        }
    });

    socket.on('deleteScheduledTask', (data) => {
        const s = scheduledTasks.get(data?.scheduleId);
        if (!s) return;
        if (s.status === 'running') s.status = 'cancelled';
        scheduledTasks.delete(data.scheduleId);
        persistSchedules();
        io.emit('scheduledTaskList', [...scheduledTasks.values()].map(serializeSchedule));
    });

    socket.on('getScheduledTasks', () => {
        socket.emit('scheduledTaskList', [...scheduledTasks.values()].map(serializeSchedule));
    });

    socket.on('disconnect', () => {});
});

// ================================================================
//                Start Server
// ================================================================
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    loadSchedules();
});
