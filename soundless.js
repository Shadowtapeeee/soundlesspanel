#!/usr/bin/env node
'use strict';
/*
 * Soundless — сервер API + Telegram-бот (админ-панель) в одном файле.
 * Требования: Node.js >= 22.13 (встроенный node:sqlite). Опционально: ffmpeg (обложки, громкость).
 * Запуск: node --disable-warning=ExperimentalWarning soundless.js
 */
const http = require('node:http');
const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');
const { spawn } = require('node:child_process');
const { DatabaseSync } = require('node:sqlite');

try { if (fs.existsSync(path.join(process.cwd(), '.env'))) process.loadEnvFile(path.join(process.cwd(), '.env')); } catch (e) { console.warn('[env]', e.message); }

const VERSION = '1.0.0';
const CFG = {
  port: Number(process.env.PORT) || 8080,
  host: process.env.HOST || '0.0.0.0',
  dataDir: path.resolve(process.env.DATA_DIR || './data'),
  botToken: (process.env.BOT_TOKEN || '').trim(),
  admins: (process.env.ADMIN_IDS || '').split(/[\s,;]+/).filter(Boolean).map(Number).filter(Number.isFinite),
  tgApi: (process.env.TELEGRAM_API || 'https://api.telegram.org').replace(/\/+$/, ''),
  publicUrl: (process.env.PUBLIC_URL || '').replace(/\/+$/, ''),
  downloadUrl: process.env.DOWNLOAD_URL || 'https://github.com/Shadowtapeeee/soundless',
  audiusApp: process.env.AUDIUS_APP_NAME || 'SOUNDLESS',
  translate: (process.env.TRANSLATE_PROVIDER || 'mymemory').toLowerCase(),
  libreUrl: (process.env.LIBRETRANSLATE_URL || '').replace(/\/+$/, ''),
  libreKey: process.env.LIBRETRANSLATE_KEY || '',
  deeplKey: process.env.DEEPL_KEY || '',
  ffmpeg: process.env.FFMPEG_PATH || 'ffmpeg',
  premiumEmoji: process.env.PREMIUM_EMOJI !== '0',
  trustProxy: process.env.TRUST_PROXY === '1',
};
const UA = `Soundless/${VERSION} (+${CFG.downloadUrl})`;
const DIRS = ['tracks', 'covers', 'avatars', 'banners', 'tmp'].reduce((o, d) => (o[d] = path.join(CFG.dataDir, d), o), {});
for (const d of Object.values(DIRS)) fs.mkdirSync(d, { recursive: true });

const now = () => Date.now();
const DAY = 86400000;
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

/* ───────────────────────────── База данных ───────────────────────────── */
const db = new DatabaseSync(path.join(CFG.dataDir, 'soundless.db'));
db.exec(`
PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS users(
  id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL UNIQUE COLLATE NOCASE, name TEXT NOT NULL,
  pass TEXT NOT NULL, bio TEXT DEFAULT '', avatar TEXT, banner TEXT, color TEXT, links TEXT DEFAULT '[]',
  privacy TEXT DEFAULT '{}', prefs TEXT DEFAULT '{}', taste TEXT DEFAULT '[]', notify TEXT DEFAULT '{}',
  tg_id INTEGER UNIQUE, tg_username TEXT, verified INTEGER DEFAULT 0, banned INTEGER DEFAULT 0,
  created_at INTEGER NOT NULL, last_seen INTEGER, now_playing TEXT, q TEXT DEFAULT '');
CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  device TEXT, ip TEXT, created_at INTEGER, last_used INTEGER);
CREATE TABLE IF NOT EXISTS follows(follower INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  followee INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, created_at INTEGER, PRIMARY KEY(follower, followee));
CREATE TABLE IF NOT EXISTS plays(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  track_id TEXT, title TEXT, artist TEXT, artist_key TEXT, cover TEXT, source TEXT, listened INTEGER DEFAULT 0, data TEXT, played_at INTEGER);
CREATE INDEX IF NOT EXISTS plays_user ON plays(user_id, played_at);
CREATE INDEX IF NOT EXISTS plays_time ON plays(played_at);
CREATE TABLE IF NOT EXISTS likes(user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, track_id TEXT NOT NULL, data TEXT,
  created_at INTEGER, PRIMARY KEY(user_id, track_id));
CREATE TABLE IF NOT EXISTS playlists(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  name TEXT NOT NULL, description TEXT DEFAULT '', public INTEGER DEFAULT 1, created_at INTEGER, updated_at INTEGER);
CREATE TABLE IF NOT EXISTS playlist_tracks(id INTEGER PRIMARY KEY AUTOINCREMENT, playlist_id INTEGER NOT NULL REFERENCES playlists(id) ON DELETE CASCADE,
  pos REAL, track_id TEXT, data TEXT, added_at INTEGER);
CREATE TABLE IF NOT EXISTS tracks(id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, artist TEXT NOT NULL, album TEXT DEFAULT '',
  genre TEXT DEFAULT '', duration INTEGER DEFAULT 0, file TEXT NOT NULL, mime TEXT, size INTEGER DEFAULT 0, cover TEXT, cover_v INTEGER DEFAULT 0,
  lyrics TEXT, gain REAL, plays INTEGER DEFAULT 0, explicit INTEGER DEFAULT 0, tg_file_id TEXT, uploaded_by INTEGER, created_at INTEGER, q TEXT DEFAULT '');
CREATE TABLE IF NOT EXISTS cache(k TEXT PRIMARY KEY, v TEXT, exp INTEGER);
CREATE TABLE IF NOT EXISTS notifications(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  type TEXT, actor_id INTEGER, data TEXT, created_at INTEGER, read INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS announcements(id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT, created_at INTEGER, active INTEGER DEFAULT 1);
CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS codes(code TEXT PRIMARY KEY, kind TEXT, user_id INTEGER, data TEXT, created_at INTEGER, expires_at INTEGER);
CREATE TABLE IF NOT EXISTS bot_users(chat_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, created_at INTEGER, blocked INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS bot_state(chat_id INTEGER PRIMARY KEY, state TEXT, data TEXT, msg_id INTEGER);
`);
const stmts = new Map();
const S = (sql) => { let s = stmts.get(sql); if (!s) { s = db.prepare(sql); stmts.set(sql, s); } return s; };
const clean = (p) => p.map((v) => (v === undefined ? null : typeof v === 'boolean' ? (v ? 1 : 0) : v));
const one = (sql, ...p) => S(sql).get(...clean(p));
const all = (sql, ...p) => S(sql).all(...clean(p));
const run = (sql, ...p) => S(sql).run(...clean(p));
const tx = (fn) => { db.exec('BEGIN'); try { const r = fn(); db.exec('COMMIT'); return r; } catch (e) { db.exec('ROLLBACK'); throw e; } };
const J = (s, d) => { try { return s ? JSON.parse(s) : d; } catch { return d; } };

const KV_DEFAULTS = { registration: '1', maintenance: '0', audius: '1', notify_reg: '1' };
const kv = (k) => one('SELECT v FROM kv WHERE k=?', k)?.v ?? KV_DEFAULTS[k];
const setKv = (k, v) => run('INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v', k, String(v));
const flag = (k) => kv(k) === '1';

const cacheGet = (k) => { const r = one('SELECT v, exp FROM cache WHERE k=?', k); if (!r) return undefined; if (r.exp < now()) return undefined; return J(r.v, null); };
const cacheSet = (k, v, ttl) => run('INSERT INTO cache(k,v,exp) VALUES(?,?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v, exp=excluded.exp', k, JSON.stringify(v), now() + ttl);
setInterval(() => { try { run('DELETE FROM cache WHERE exp<?', now()); run('DELETE FROM codes WHERE expires_at<?', now()); } catch {} }, 3600e3).unref();

/* ───────────────────────────── Утилиты ───────────────────────────── */
const lower = (s) => String(s || '').toLowerCase().replace(/ё/g, 'е');
const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const token = (n = 32) => crypto.randomBytes(n).toString('base64url');
const sha1 = (s) => crypto.createHash('sha1').update(s).digest('hex');
const clamp = (v, a, b) => Math.min(b, Math.max(a, v));
const fmtNum = (n) => Number(n || 0).toLocaleString('ru-RU');
const fmtDur = (s) => `${Math.floor(s / 60)}:${String(Math.floor(s % 60)).padStart(2, '0')}`;
const fmtSize = (b) => (b > 1 << 30 ? (b / (1 << 30)).toFixed(2) + ' ГБ' : (b / (1 << 20)).toFixed(1) + ' МБ');
const fmtDate = (t) => new Date(t).toLocaleDateString('ru-RU', { day: 'numeric', month: 'long', year: 'numeric' });
const plural = (n, a, b, c) => { n = Math.abs(n) % 100; const m = n % 10; return n > 10 && n < 20 ? c : m > 1 && m < 5 ? b : m === 1 ? a : c; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const primaryArtist = (a) => String(a || '').split(/\s*(?:,|&|\bfeat\.?|\bft\.?|\bx\b|\/|;)\s*/i)[0].trim() || String(a || '').trim();
const levelOf = (plays) => { const level = 1 + Math.floor(Math.sqrt(plays / 2)); const cur = 2 * (level - 1) ** 2; const next = 2 * level ** 2; return { level, plays, cur, next, progress: (plays - cur) / (next - cur) }; };

async function hashPass(p) { const salt = crypto.randomBytes(16); const h = await new Promise((res, rej) => crypto.scrypt(p, salt, 64, { N: 16384, r: 8, p: 1 }, (e, k) => (e ? rej(e) : res(k)))); return `s1$${salt.toString('base64')}$${h.toString('base64')}`; }
async function checkPass(p, stored) {
  if (!stored || !stored.startsWith('s1$')) return false;
  const [, s, h] = stored.split('$'); const salt = Buffer.from(s, 'base64'); const hb = Buffer.from(h, 'base64');
  const k = await new Promise((res, rej) => crypto.scrypt(p, salt, 64, { N: 16384, r: 8, p: 1 }, (e, kk) => (e ? rej(e) : res(kk))));
  return crypto.timingSafeEqual(k, hb);
}

async function fetchJSON(url, opt = {}) {
  const r = await fetch(url, { ...opt, signal: AbortSignal.timeout(opt.timeout || 9000), headers: { 'user-agent': UA, accept: 'application/json', ...(opt.headers || {}) } });
  if (!r.ok) { const e = new Error(`HTTP ${r.status} ${url}`); e.status = r.status; throw e; }
  return r.json();
}
const memo = new Map();
async function memoize(key, ttl, fn) {
  const c = memo.get(key); if (c && c.exp > now()) return c.val;
  const val = await fn(); memo.set(key, { val, exp: now() + ttl });
  if (memo.size > 3000) for (const [k, v] of memo) if (v.exp < now() || memo.size > 2500) memo.delete(k);
  return val;
}

let ffmpegOk = false;
function sh(cmd, args, timeout = 120000) {
  return new Promise((resolve) => {
    let out = '', err = ''; let p;
    try { p = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'] }); } catch (e) { return resolve({ code: -1, out, err: String(e) }); }
    const t = setTimeout(() => p.kill('SIGKILL'), timeout);
    p.stdout.on('data', (d) => (out += d)); p.stderr.on('data', (d) => (err += d).length > 2e6 && (err = err.slice(-1e6)));
    p.on('error', (e) => { clearTimeout(t); resolve({ code: -1, out, err: String(e) }); });
    p.on('close', (code) => { clearTimeout(t); resolve({ code, out, err }); });
  });
}
sh(CFG.ffmpeg, ['-version'], 8000).then((r) => { ffmpegOk = r.code === 0; log(ffmpegOk ? '[ffmpeg] найден' : '[ffmpeg] не найден — обложки/нормализация из файлов отключены'); });
async function probeAudio(file) {
  if (!ffmpegOk) return {};
  const r = await sh(CFG.ffmpeg, ['-hide_banner', '-nostats', '-i', file, '-af', 'ebur128=framelog=quiet', '-f', 'null', '-'], 300000);
  const d = /Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)/.exec(r.err);
  const i = [...r.err.matchAll(/I:\s*(-?\d+(?:\.\d+)?)\s*LUFS/g)].pop();
  const tags = {};
  for (const m of r.err.matchAll(/^\s{4,}(title|artist|album|genre)\s*:\s*(.+)$/gim)) if (!tags[m[1].toLowerCase()]) tags[m[1].toLowerCase()] = m[2].trim();
  return { duration: d ? +d[1] * 3600 + +d[2] * 60 + +d[3] : 0, lufs: i ? +i[1] : null, tags };
}
async function extractCover(file, out) {
  if (!ffmpegOk) return false;
  const r = await sh(CFG.ffmpeg, ['-y', '-hide_banner', '-loglevel', 'error', '-i', file, '-map', '0:v:0', '-frames:v', '1', '-vf', "scale='min(1000,iw)':-2", out], 60000);
  return r.code === 0 && fs.existsSync(out) && fs.statSync(out).size > 500;
}

/* ───────────────────────────── Модели ───────────────────────────── */
const mediaUrl = (kind, file) => (file ? `/api/media/${kind}/${file}` : null);
const isOnline = (u) => !!u.last_seen && now() - u.last_seen < 120000;
const playsCount = (uid) => one('SELECT COUNT(*) c FROM plays WHERE user_id=?', uid).c;
function publicUser(u, mini = false) {
  if (!u) return null;
  const pv = J(u.privacy, {});
  const base = { id: u.id, username: u.username, name: u.name, avatar: mediaUrl('avatars', u.avatar), color: u.color || null, verified: !!u.verified, online: pv.showOnline === false ? null : isOnline(u) };
  if (mini) return base;
  const links = J(u.links, []).filter((l) => l && l.url);
  if (u.tg_username && pv.showTelegram !== false) links.unshift({ type: 'telegram', title: u.tg_username, url: `https://t.me/${u.tg_username}` });
  return { ...base, bio: u.bio || '', banner: mediaUrl('banners', u.banner), links, createdAt: u.created_at, lastSeen: pv.showOnline === false ? null : u.last_seen, level: levelOf(playsCount(u.id)), telegram: pv.showTelegram !== false ? u.tg_username || null : null };
}
function privateUser(u) {
  return { ...publicUser(u), privacy: J(u.privacy, {}), prefs: J(u.prefs, {}), taste: J(u.taste, []), notify: J(u.notify, {}), telegramLinked: !!u.tg_id, telegramUsername: u.tg_username || null, hasPassword: u.pass !== '!', linksRaw: J(u.links, []) };
}
const userQ = (u) => lower(`${u.username} ${u.name}`);

function slTrack(r) {
  if (!r) return null;
  const cover = r.cover ? `/api/cover/${r.id}?v=${r.cover_v || 0}` : null;
  return { id: 'sl:' + r.id, source: 'soundless', title: r.title, artist: r.artist, artistId: 'sl:' + r.artist, album: r.album || '', genre: r.genre || '', duration: r.duration || 0, cover, coverLarge: cover, coverSmall: cover, gain: r.gain ?? null, plays: r.plays || 0, explicit: !!r.explicit, hasLyrics: !!r.lyrics };
}
function auTrack(t) {
  if (!t || t.is_delete || t.is_streamable === false || t.is_stream_gated || t.is_unlisted) return null;
  const a = t.artwork || {};
  return { id: 'au:' + t.id, source: 'audius', title: t.title, artist: t.user?.name || '', artistId: t.user ? 'au:' + t.user.id : null, album: '', genre: t.genre || '', mood: t.mood || '', duration: t.duration || 0, cover: a['480x480'] || null, coverLarge: a['1000x1000'] || a['480x480'] || null, coverSmall: a['150x150'] || a['480x480'] || null, plays: t.play_count || 0, verified: !!t.user?.is_verified };
}
function auArtist(u) {
  if (!u) return null; const p = u.profile_picture || {};
  return { id: 'au:' + u.id, source: 'audius', name: u.name, handle: u.handle, avatar: p['480x480'] || p['150x150'] || null, verified: !!u.is_verified, followers: u.follower_count || 0, tracks: u.track_count || 0, bio: u.bio || '' };
}
function sanitizeTrack(t) {
  if (!t || typeof t !== 'object') return null;
  const id = String(t.id || '').slice(0, 160);
  if (!/^(sl|au|local):/.test(id)) return null;
  const s = (v, n = 300) => (v == null ? '' : String(v).slice(0, n));
  const url = (v) => (typeof v === 'string' && (/^https:\/\//.test(v) || /^\/api\//.test(v)) ? v.slice(0, 1000) : null);
  return { id, source: s(t.source, 20), title: s(t.title) || 'Без названия', artist: s(t.artist) || 'Неизвестен', artistId: t.artistId ? s(t.artistId, 200) : null, album: s(t.album), genre: s(t.genre, 60), duration: Math.max(0, Math.round(+t.duration || 0)), cover: url(t.cover), coverLarge: url(t.coverLarge), coverSmall: url(t.coverSmall), explicit: !!t.explicit };
}

/* ───────────────────────────── Audius ───────────────────────────── */
const AUDIUS = 'https://api.audius.co/v1';
async function audius(p, params = {}, ttl = 300000) {
  if (!flag('audius')) return null;
  const u = new URL(AUDIUS + p);
  for (const [k, v] of Object.entries(params)) if (v != null && v !== '') u.searchParams.set(k, v);
  u.searchParams.set('app_name', CFG.audiusApp);
  return memoize('au:' + u, ttl, async () => (await fetchJSON(u.toString(), { timeout: 8000 })).data);
}
const safe = (p, d = []) => p.then((v) => v ?? d).catch((e) => { log('[warn]', e.message); return d; });
const auSearch = async (q, limit = 20) => (await safe(audius('/tracks/search', { query: q, limit: limit + 10 }))).map(auTrack).filter(Boolean).slice(0, limit);
const auTrending = async (genre, limit = 30, time = 'week') => (await safe(audius('/tracks/trending', { genre, time, limit: limit + 10 }, 900000))).map(auTrack).filter(Boolean).slice(0, limit);

/* ───────────────────────────── Каталог ───────────────────────────── */
function catalogSearch(q, limit = 20) {
  const words = lower(q).split(/\s+/).filter(Boolean).slice(0, 6);
  if (!words.length) return [];
  return all(`SELECT * FROM tracks WHERE ${words.map(() => 'q LIKE ?').join(' AND ')} ORDER BY plays DESC LIMIT ?`, ...words.map((w) => `%${w}%`), limit).map(slTrack);
}
const trackQ = (t) => lower(`${t.title} ${t.artist} ${t.album || ''} ${t.genre || ''}`);
function catalogArtists(q, limit = 8) {
  const lq = lower(q);
  return all(`SELECT artist, COUNT(*) c, MAX(id) mid FROM tracks WHERE q LIKE ? GROUP BY artist ORDER BY c DESC LIMIT 50`, `%${lq}%`).filter((r) => lower(r.artist).includes(lq)).slice(0, limit).map((r) => {
    const t = one('SELECT * FROM tracks WHERE id=?', r.mid);
    return { id: 'sl:' + r.artist, source: 'soundless', name: r.artist, avatar: slTrack(t).cover, tracks: r.c, verified: false };
  });
}

/* ───────────────────────────── Тексты песен ───────────────────────────── */
function lrcTime(s) { const m = /(\d+):(\d+(?:[.:]\d+)?)/.exec(s); if (!m) return null; return Math.round((+m[1] * 60 + parseFloat(m[2].replace(':', '.'))) * 1000); }
function finishLines(lines, durationMs) {
  lines = lines.filter((l) => l.t != null).sort((a, b) => a.t - b.t);
  for (let i = 0; i < lines.length; i++) {
    const l = lines[i]; const next = lines[i + 1];
    if (!l.e || l.e <= l.t) l.e = next ? next.t : Math.max(l.t + 4000, durationMs || 0);
    if (next && l.e > next.t + 50 && !l.words) l.e = next.t;
    if (l.words?.length) {
      l.words.sort((a, b) => a.t - b.t);
      l.words.forEach((w, j) => { if (!w.e) w.e = l.words[j + 1] ? l.words[j + 1].t : Math.max(l.e, w.t + 300); });
      if (l.words[l.words.length - 1].e > l.e) l.e = l.words[l.words.length - 1].e;
    }
  }
  return lines;
}
function parseLRC(text, durationMs) {
  const lines = []; let offset = 0;
  for (const raw of String(text).split(/\r?\n/)) {
    const off = /^\[offset:\s*([+-]?\d+)\]/i.exec(raw); if (off) { offset = +off[1]; continue; }
    const stamps = [...raw.matchAll(/\[(\d+:\d+(?:[.:]\d+)?)\]/g)];
    if (!stamps.length) continue;
    let body = raw.replace(/\[(\d+:\d+(?:[.:]\d+)?)\]/g, '');
    let words = null;
    if (/<\d+:\d+(?:[.:]\d+)?>/.test(body)) {
      words = []; const parts = body.split(/(<\d+:\d+(?:[.:]\d+)?>)/);
      let cur = null;
      for (const p of parts) { if (/^<.*>$/.test(p)) { cur = lrcTime(p) - offset; continue; } if (p && cur != null) { words.push({ t: cur, text: p }); cur = null; } else if (p && words.length) words[words.length - 1].text += p; }
      body = words.map((w) => w.text).join('');
      if (!words.length) words = null;
    }
    for (const s of stamps) lines.push({ t: lrcTime(s[1]) - offset, text: body.trim(), words: words ? words.map((w) => ({ ...w })) : undefined });
  }
  return finishLines(lines, durationMs);
}
function yamlScalar(v) {
  v = v.trim();
  if (v.startsWith("'")) return v.slice(1, v.endsWith("'") ? -1 : undefined).replace(/''/g, "'");
  if (v.startsWith('"')) { try { return JSON.parse(v); } catch { return v.slice(1, -1); } }
  return v;
}
function parseLyricsfile(y, durationMs) {
  // Минимальный парсер формата LRCLIB lyricsfile (YAML): lines[].text/start_ms/end_ms/words[].text/start_ms
  const lines = []; let inLines = false; let cur = null; let word = null; let last = null;
  for (const row of String(y).split(/\r?\n/)) {
    if (!row.trim()) continue;
    if (/^\S/.test(row) && !/^- /.test(row)) { inLines = /^lines:/.test(row); continue; }
    if (!inLines) continue;
    const m = /^(\s*)(- )?([a-z_]+):(?:\s(.*))?$/.exec(row);
    if (!m) { if (last) last.raw += ' ' + row.trim(); continue; } // перенос длинной строки
    const ind = m[1].length; const dash = !!m[2]; const key = m[3]; const val = m[4] || '';
    let target;
    if (dash && ind === 0) { cur = { raw: '', t: null, e: null, words: [] }; lines.push(cur); word = null; target = cur; }
    else if (dash) { if (!cur) continue; word = { raw: '', t: null }; cur.words.push(word); target = word; }
    else target = ind >= 4 ? word : cur;
    if (!target) continue;
    if (key === 'text') { target.raw = val; last = target; }
    else { last = null; if (key === 'start_ms') target.t = +val; else if (key === 'end_ms') target.e = +val; }
  }
  const out = lines.filter((l) => l.t != null).map((l) => ({ t: l.t, e: l.e, text: yamlScalar(l.raw), words: l.words.length ? l.words.filter((w) => w.t != null).map((w) => ({ t: w.t, text: yamlScalar(w.raw) })) : undefined }));
  return finishLines(out, durationMs);
}
async function lrclib(title, artist, album, duration) {
  const base = 'https://lrclib.net/api';
  const qs = (o) => new URLSearchParams(Object.entries(o).filter(([, v]) => v)).toString();
  try { return await fetchJSON(`${base}/get?${qs({ track_name: title, artist_name: artist, album_name: album, duration: duration ? Math.round(duration) : '' })}`, { timeout: 10000 }); }
  catch (e) { if (e.status !== 404 && e.status !== 400) throw e; }
  const list = await fetchJSON(`${base}/search?${qs({ track_name: title, artist_name: primaryArtist(artist) })}`, { timeout: 10000 }).catch(() => []);
  const ok = (x) => !duration || !x.duration || Math.abs(x.duration - duration) < 5;
  const score = (x) => (x.hasWordSync ? 3 : 0) + (x.syncedLyrics ? 2 : 0) + (x.plainLyrics ? 1 : 0);
  return list.filter(ok).sort((a, b) => score(b) - score(a))[0] || list.filter((x) => x.syncedLyrics)[0] || null;
}
function buildLyrics(rec, durationMs, source) {
  if (!rec) return { type: 'none', lines: [], plain: '', source };
  if (rec.instrumental) return { type: 'instrumental', lines: [], plain: '', source };
  let lines = [];
  if (rec.lyricsfile && rec.hasWordSync) { try { lines = parseLyricsfile(rec.lyricsfile, durationMs); } catch { lines = []; } }
  if (!lines.length && rec.syncedLyrics) lines = parseLRC(rec.syncedLyrics, durationMs);
  if (!lines.length && rec.lyricsfile) { try { lines = parseLyricsfile(rec.lyricsfile, durationMs); } catch { lines = []; } }
  const hasWords = lines.some((l) => l.words?.length);
  return { type: lines.length ? (hasWords ? 'word' : 'line') : rec.plainLyrics ? 'plain' : 'none', lines, plain: rec.plainLyrics || lines.map((l) => l.text).join('\n'), source };
}
async function getLyrics({ id, title, artist, album, duration }) {
  const dms = (Number(duration) || 0) * 1000;
  if (id && id.startsWith('sl:')) {
    const t = one('SELECT * FROM tracks WHERE id=?', +id.slice(3));
    if (t?.lyrics) {
      const synced = /\[\d+:\d+/.test(t.lyrics);
      return synced ? buildLyrics({ syncedLyrics: t.lyrics, plainLyrics: '' }, t.duration * 1000, 'catalog') : { type: 'plain', lines: [], plain: t.lyrics, source: 'catalog' };
    }
    if (t) { title = t.title; artist = t.artist; album = t.album; duration = t.duration; }
  }
  if (!title || !artist) return { type: 'none', lines: [], plain: '' };
  const key = 'ly:' + sha1(lower(`${primaryArtist(artist)}|${title}|${Math.round(duration || 0)}`));
  const c = cacheGet(key); if (c !== undefined) return c;
  try {
    const rec = await lrclib(title, artist, album, duration);
    const out = buildLyrics(rec, dms, 'lrclib');
    cacheSet(key, out, out.type === 'none' ? DAY : 30 * DAY);
    return out;
  } catch (e) { log('[lyrics]', e.message); return { type: 'none', lines: [], plain: '', error: 'unavailable' }; }
}

/* ───────────────────────────── Перевод ───────────────────────────── */
const detectLang = (s) => { const cyr = (s.match(/[а-яё]/gi) || []).length; const lat = (s.match(/[a-z]/gi) || []).length; return cyr > lat ? 'ru' : 'en'; };
function chunks(lines, maxBytes) { const out = []; let cur = []; let size = 0; for (const l of lines) { const b = Buffer.byteLength(l + '\n'); if (cur.length && size + b > maxBytes) { out.push(cur); cur = []; size = 0; } cur.push(l); size += b; } if (cur.length) out.push(cur); return out; }
async function translateBatch(lines, from, to) {
  const text = lines.join('\n');
  if (CFG.translate === 'google') {
    const r = await fetch('https://translate.googleapis.com/translate_a/single?client=gtx&dt=t&sl=auto&tl=' + to, { method: 'POST', body: new URLSearchParams({ q: text }), signal: AbortSignal.timeout(12000), headers: { 'user-agent': UA } });
    const j = await r.json(); return j[0].map((x) => x[0]).join('').split('\n');
  }
  if (CFG.translate === 'libre' && CFG.libreUrl) {
    const j = await fetchJSON(CFG.libreUrl + '/translate', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ q: text, source: 'auto', target: to, format: 'text', api_key: CFG.libreKey || undefined }), timeout: 20000 });
    return String(j.translatedText).split('\n');
  }
  if (CFG.translate === 'deepl' && CFG.deeplKey) {
    const host = CFG.deeplKey.endsWith(':fx') ? 'api-free.deepl.com' : 'api.deepl.com';
    const j = await fetchJSON(`https://${host}/v2/translate`, { method: 'POST', headers: { 'content-type': 'application/json', authorization: 'DeepL-Auth-Key ' + CFG.deeplKey }, body: JSON.stringify({ text: lines, target_lang: to.toUpperCase() }), timeout: 20000 });
    return j.translations.map((t) => t.text);
  }
  const j = await fetchJSON(`https://api.mymemory.translated.net/get?${new URLSearchParams({ q: text, langpair: `${from}|${to}` })}`, { timeout: 15000 });
  if (j.responseStatus && +j.responseStatus !== 200) throw new Error(j.responseDetails || 'translate failed');
  return String(j.responseData.translatedText).split('\n');
}
async function translateLines(lines, to) {
  lines = lines.map((l) => String(l || '').slice(0, 400)).slice(0, 300);
  const from = detectLang(lines.join(' '));
  if (!to || to === from) to = from === 'ru' ? 'en' : 'ru';
  const key = 'tr:' + sha1(to + '\n' + lines.join('\n'));
  const c = cacheGet(key); if (c) return c;
  const idx = []; const src = [];
  lines.forEach((l, i) => { if (l.trim()) { idx.push(i); src.push(l); } });
  const out = lines.map(() => '');
  const max = CFG.translate === 'mymemory' ? 450 : 4500;
  let pos = 0;
  for (const batch of chunks(src, max)) {
    let res = await translateBatch(batch, from, to);
    if (res.length !== batch.length) res = await Promise.all(batch.map(async (b) => (await translateBatch([b], from, to)).join(' ')));
    res.forEach((t, k) => (out[idx[pos + k]] = t.trim()));
    pos += batch.length;
  }
  const result = { lines: out, from, to };
  cacheSet(key, result, 90 * DAY);
  return result;
}

/* ───────────────────────────── Статистика ───────────────────────────── */
function topArtists(uid, since = 0, limit = 10) {
  return all(`SELECT artist_key k, MAX(artist) name, COUNT(*) c, SUM(listened) s FROM plays WHERE user_id=? AND played_at>=? AND artist_key<>'' GROUP BY artist_key ORDER BY c DESC LIMIT ?`, uid, since, limit).map((r) => {
    const last = one(`SELECT data FROM plays WHERE user_id=? AND artist_key=? AND cover IS NOT NULL ORDER BY played_at DESC LIMIT 1`, uid, r.k);
    const t = J(last?.data, {});
    return { key: r.k, name: primaryArtist(r.name), plays: r.c, seconds: r.s || 0, image: t.coverLarge || t.cover || null, artistId: t.artistId || null };
  });
}
function topTracks(uid, since = 0, limit = 10) {
  return all(`SELECT track_id, COUNT(*) c, MAX(data) data FROM plays WHERE user_id=? AND played_at>=? GROUP BY track_id ORDER BY c DESC, MAX(played_at) DESC LIMIT ?`, uid, since, limit).map((r) => ({ ...J(r.data, {}), plays: r.c }));
}
function recentTracks(uid, limit = 12) {
  const rows = all(`SELECT track_id, data, MAX(played_at) t FROM plays WHERE user_id=? GROUP BY track_id ORDER BY t DESC LIMIT ?`, uid, limit);
  return rows.map((r) => ({ ...J(r.data, {}), playedAt: r.t }));
}
function compatibility(a, b) {
  const dist = (uid) => { const rows = all(`SELECT artist_key k, MAX(artist) n, COUNT(*) c FROM plays WHERE user_id=? AND artist_key<>'' GROUP BY artist_key ORDER BY c DESC LIMIT 100`, uid); const tot = rows.reduce((s, r) => s + r.c, 0) || 1; return new Map(rows.map((r) => [r.k, { share: r.c / tot, name: primaryArtist(r.n) }])); };
  const da = dist(a); const db2 = dist(b);
  if (!da.size || !db2.size) return { percent: 0, label: 'Пока не с чем сравнить', shared: [] };
  let score = 0; const shared = [];
  for (const [k, v] of da) { const w = db2.get(k); if (w) { const m = Math.min(v.share, w.share); score += m; shared.push({ key: k, name: v.name, w: m }); } }
  const percent = Math.round(Math.sqrt(score) * 100);
  const label = percent < 15 ? 'Немного общего' : percent < 35 ? 'Есть общее' : percent < 60 ? 'Хорошая совместимость' : percent < 85 ? 'Очень похожий вкус' : 'Музыкальные близнецы';
  shared.sort((x, y) => y.w - x.w);
  return { percent, label, shared: shared.slice(0, 5).map((s) => { const last = one(`SELECT data FROM plays WHERE user_id IN (?,?) AND artist_key=? AND cover IS NOT NULL ORDER BY played_at DESC LIMIT 1`, a, b, s.key); const t = J(last?.data, {}); return { name: s.name, image: t.cover || null, artistId: t.artistId || null }; }) };
}
const friendsCount = (uid) => one('SELECT COUNT(*) c FROM follows f JOIN follows g ON g.follower=f.followee AND g.followee=f.follower WHERE f.follower=?', uid).c;
const followersCount = (uid) => one('SELECT COUNT(*) c FROM follows WHERE followee=?', uid).c;
const followingCount = (uid) => one('SELECT COUNT(*) c FROM follows WHERE follower=?', uid).c;
function nowPlayingOf(u) {
  const pv = J(u.privacy, {}); if (pv.showNowPlaying === false) return null;
  const np = J(u.now_playing, null); if (!np || !np.track) return null;
  if (now() - (np.at || 0) > 15 * 60000) return null;
  if (!np.playing && now() - (np.at || 0) > 3 * 60000) return null;
  return np;
}

/* ───────────────────────────── HTTP ───────────────────────────── */
const CORS = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': 'authorization, content-type, x-device, range', 'Access-Control-Allow-Methods': 'GET, POST, PUT, PATCH, DELETE, OPTIONS', 'Access-Control-Expose-Headers': 'content-length, content-range, accept-ranges', 'Access-Control-Max-Age': '86400' };
class HttpError extends Error { constructor(status, code, message) { super(message || code); this.status = status; this.code = code; } }
const fail = (status, code, message) => { throw new HttpError(status, code, message); };
function send(res, status, body, headers = {}) {
  const data = body === undefined ? '' : JSON.stringify(body);
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store', ...CORS, ...headers });
  res.end(data);
}
function readBody(req, limit = 1 << 20) {
  return new Promise((resolve, reject) => {
    const chunks = []; let size = 0;
    req.on('data', (c) => { size += c.length; if (size > limit) { reject(new HttpError(413, 'too_large', 'Слишком большой файл')); req.destroy(); } else chunks.push(c); });
    req.on('end', () => resolve(Buffer.concat(chunks))); req.on('error', reject);
  });
}
async function readJSON(req) { const b = await readBody(req); if (!b.length) return {}; try { return JSON.parse(b.toString('utf8')); } catch { fail(400, 'bad_json', 'Некорректный JSON'); } }
const MIME = { '.mp3': 'audio/mpeg', '.m4a': 'audio/mp4', '.aac': 'audio/aac', '.ogg': 'audio/ogg', '.oga': 'audio/ogg', '.opus': 'audio/ogg', '.flac': 'audio/flac', '.wav': 'audio/wav', '.webm': 'audio/webm', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png', '.webp': 'image/webp', '.gif': 'image/gif' };
async function sendFile(req, res, file, mime, cache = 'public, max-age=31536000, immutable') {
  let st; try { st = await fsp.stat(file); } catch { return send(res, 404, { error: 'not_found' }); }
  const size = st.size; const type = mime || MIME[path.extname(file).toLowerCase()] || 'application/octet-stream';
  const h = { 'Content-Type': type, 'Accept-Ranges': 'bytes', 'Cache-Control': cache, ...CORS };
  const m = /bytes=(\d*)-(\d*)/.exec(req.headers.range || '');
  if (m && (m[1] || m[2])) {
    let start = m[1] === '' ? size - Number(m[2]) : Number(m[1]);
    let end = m[1] === '' || m[2] === '' ? size - 1 : Math.min(Number(m[2]), size - 1);
    if (start < 0) start = 0;
    if (start >= size || start > end) { res.writeHead(416, { 'Content-Range': `bytes */${size}`, ...CORS }); return res.end(); }
    res.writeHead(206, { ...h, 'Content-Range': `bytes ${start}-${end}/${size}`, 'Content-Length': end - start + 1 });
    if (req.method === 'HEAD') return res.end();
    return fs.createReadStream(file, { start, end }).on('error', () => res.destroy()).pipe(res);
  }
  res.writeHead(200, { ...h, 'Content-Length': size });
  if (req.method === 'HEAD') return res.end();
  fs.createReadStream(file).on('error', () => res.destroy()).pipe(res);
}
const ipOf = (req) => (CFG.trustProxy ? String(req.headers['x-forwarded-for'] || '').split(',')[0].trim() : '') || req.socket.remoteAddress || '';
const rl = new Map();
function rateLimit(req, key, max, windowMs) {
  const k = key + ':' + ipOf(req); const t = now(); const e = rl.get(k) || { n: 0, reset: t + windowMs };
  if (e.reset < t) { e.n = 0; e.reset = t + windowMs; }
  e.n++; rl.set(k, e);
  if (e.n > max) fail(429, 'rate_limited', 'Слишком много попыток, подождите немного');
}
setInterval(() => { const t = now(); for (const [k, v] of rl) if (v.reset < t) rl.delete(k); }, 60000).unref();

function auth(req, required = true) {
  const h = req.headers.authorization || ''; const u = new URL(req.url, 'http://x');
  const tok = h.startsWith('Bearer ') ? h.slice(7) : u.searchParams.get('k');
  if (!tok) { if (required) fail(401, 'unauthorized', 'Нужно войти в аккаунт'); return null; }
  const s = one('SELECT * FROM sessions WHERE token=?', tok);
  if (!s) { if (required) fail(401, 'unauthorized', 'Сессия истекла, войдите снова'); return null; }
  const user = one('SELECT * FROM users WHERE id=?', s.user_id);
  if (!user) fail(401, 'unauthorized');
  if (user.banned) fail(403, 'banned', 'Аккаунт заблокирован');
  if (now() - (s.last_used || 0) > 60000) run('UPDATE sessions SET last_used=?, ip=? WHERE token=?', now(), ipOf(req), tok);
  if (now() - (user.last_seen || 0) > 30000) run('UPDATE users SET last_seen=? WHERE id=?', now(), user.id);
  return { user, token: tok };
}
function createSession(req, userId) {
  const t = token(32);
  run('INSERT INTO sessions(token,user_id,device,ip,created_at,last_used) VALUES(?,?,?,?,?,?)', t, userId, String(req.headers['x-device'] || req.headers['user-agent'] || '').slice(0, 120), ipOf(req), now(), now());
  run('UPDATE users SET last_seen=? WHERE id=?', now(), userId);
  return t;
}
const validUsername = (u) => /^[a-z0-9_.]{3,24}$/i.test(u) && !/^[._]|[._]$/.test(u);
const RESERVED = new Set(['admin', 'soundless', 'support', 'root', 'api', 'system', 'me', 'settings']);

function notify(userId, type, actorId, data = {}) {
  run('INSERT INTO notifications(user_id,type,actor_id,data,created_at) VALUES(?,?,?,?,?)', userId, type, actorId, JSON.stringify(data), now());
  const u = one('SELECT tg_id, notify FROM users WHERE id=?', userId); const a = one('SELECT username, name FROM users WHERE id=?', actorId);
  if (u?.tg_id && a && J(u.notify, {}).telegram !== false && bot.ready) {
    const text = type === 'friend' ? `${em('people')} <b>${esc(a.name)}</b> (@${esc(a.username)}) теперь ваш друг` : type === 'follow' ? `${em('userOk')} <b>${esc(a.name)}</b> (@${esc(a.username)}) подписался на вас` : null;
    if (text) bot.send(u.tg_id, text).catch(() => {});
  }
}

const routes = [];
const route = (method, pattern, handler) => routes.push({ method, re: new RegExp('^' + pattern.replace(/:(\w+)/g, '(?<$1>[^/]+)') + '/?$'), handler });

route('GET', '/api/health', () => ({ ok: true, name: 'Soundless', version: VERSION, time: now() }));
route('GET', '/api/config', () => ({
  name: 'Soundless', version: VERSION, registration: flag('registration'), maintenance: flag('maintenance'), audius: flag('audius'),
  telegram: bot.username ? { bot: bot.username, url: `https://t.me/${bot.username}` } : null, download: CFG.downloadUrl,
  announcements: all('SELECT id, text, created_at createdAt FROM announcements WHERE active=1 ORDER BY id DESC LIMIT 5'),
}));

/* — Авторизация — */
route('POST', '/api/auth/register', async (req) => {
  rateLimit(req, 'reg', 8, 3600e3);
  if (!flag('registration')) fail(403, 'registration_closed', 'Регистрация временно закрыта');
  const b = await readJSON(req);
  const username = String(b.username || '').trim().toLowerCase(); const name = String(b.name || '').trim().slice(0, 40) || username; const password = String(b.password || '');
  if (!validUsername(username) || RESERVED.has(username)) fail(400, 'bad_username', 'Логин: 3–24 символа, латиница, цифры, «_» и «.»');
  if (password.length < 6 || password.length > 128) fail(400, 'bad_password', 'Пароль должен быть от 6 символов');
  if (one('SELECT id FROM users WHERE username=?', username)) fail(409, 'username_taken', 'Этот логин уже занят');
  const r = run('INSERT INTO users(username,name,pass,created_at,last_seen,q,privacy) VALUES(?,?,?,?,?,?,?)', username, name, await hashPass(password), now(), now(), lower(`${username} ${name}`), '{}');
  const id = Number(r.lastInsertRowid);
  bot.onRegister(id);
  return { token: createSession(req, id), user: privateUser(one('SELECT * FROM users WHERE id=?', id)) };
});
route('POST', '/api/auth/login', async (req) => {
  rateLimit(req, 'login', 20, 600e3);
  const b = await readJSON(req);
  const u = one('SELECT * FROM users WHERE username=?', String(b.username || '').trim().replace(/^@/, ''));
  if (!u || !(await checkPass(String(b.password || ''), u.pass))) fail(401, 'bad_credentials', 'Неверный логин или пароль');
  if (u.banned) fail(403, 'banned', 'Аккаунт заблокирован');
  return { token: createSession(req, u.id), user: privateUser(u) };
});
route('POST', '/api/auth/logout', (req) => { const a = auth(req); run('DELETE FROM sessions WHERE token=?', a.token); return { ok: true }; });
route('POST', '/api/auth/telegram/start', (req) => {
  rateLimit(req, 'tgstart', 30, 600e3);
  if (!bot.username) fail(503, 'bot_unavailable', 'Вход через Telegram не настроен на сервере');
  const code = token(12);
  run('INSERT INTO codes(code,kind,data,created_at,expires_at) VALUES(?,?,?,?,?)', code, 'login', JSON.stringify({ status: 'pending', device: String(req.headers['x-device'] || '').slice(0, 80) }), now(), now() + 10 * 60000);
  return { code, url: `https://t.me/${bot.username}?start=login_${code}`, expiresIn: 600 };
});
route('GET', '/api/auth/telegram/poll', (req, { query }) => {
  const c = one('SELECT * FROM codes WHERE code=? AND kind=?', String(query.get('code') || ''), 'login');
  if (!c || c.expires_at < now()) return { status: 'expired' };
  const d = J(c.data, {});
  if (d.status === 'ok' && c.user_id) {
    run('DELETE FROM codes WHERE code=?', c.code);
    const u = one('SELECT * FROM users WHERE id=?', c.user_id);
    if (!u || u.banned) return { status: 'denied' };
    return { status: 'ok', token: createSession(req, u.id), user: privateUser(u) };
  }
  return { status: d.status || 'pending' };
});
route('POST', '/api/auth/reset/start', async (req) => {
  rateLimit(req, 'reset', 5, 600e3);
  const b = await readJSON(req); const u = one('SELECT * FROM users WHERE username=?', String(b.username || '').trim().replace(/^@/, ''));
  if (!u || !u.tg_id || !bot.ready) return { sent: false, message: 'Сброс доступен только для аккаунтов с привязанным Telegram' };
  const code = String(crypto.randomInt(100000, 999999));
  run('DELETE FROM codes WHERE kind=? AND user_id=?', 'reset', u.id);
  run('INSERT INTO codes(code,kind,user_id,data,created_at,expires_at) VALUES(?,?,?,?,?,?)', 'reset:' + u.id + ':' + code, 'reset', u.id, '{}', now(), now() + 10 * 60000);
  await bot.send(u.tg_id, `${em('lock')} <b>Сброс пароля Soundless</b>\n\nКод: <code>${code}</code>\n\n${em('clock')} Действует 10 минут. Если это были не вы — просто проигнорируйте сообщение.`).catch(() => {});
  return { sent: true };
});
route('POST', '/api/auth/reset/finish', async (req) => {
  rateLimit(req, 'resetf', 10, 600e3);
  const b = await readJSON(req); const u = one('SELECT * FROM users WHERE username=?', String(b.username || '').trim().replace(/^@/, ''));
  if (!u) fail(400, 'bad_code', 'Неверный код');
  const c = one('SELECT * FROM codes WHERE code=?', 'reset:' + u.id + ':' + String(b.code || '').trim());
  if (!c || c.expires_at < now()) fail(400, 'bad_code', 'Неверный или просроченный код');
  if (String(b.password || '').length < 6) fail(400, 'bad_password', 'Пароль должен быть от 6 символов');
  run('UPDATE users SET pass=? WHERE id=?', await hashPass(String(b.password)), u.id);
  run('DELETE FROM sessions WHERE user_id=?', u.id); run('DELETE FROM codes WHERE code=?', c.code);
  return { token: createSession(req, u.id), user: privateUser(one('SELECT * FROM users WHERE id=?', u.id)) };
});

/* — Профиль — */
route('GET', '/api/me', (req) => privateUser(auth(req).user));
route('PATCH', '/api/me', async (req) => {
  const { user } = auth(req); const b = await readJSON(req); const set = {};
  if (b.name !== undefined) set.name = String(b.name).trim().slice(0, 40) || user.username;
  if (b.bio !== undefined) set.bio = String(b.bio).slice(0, 160);
  if (b.color !== undefined) set.color = /^#[0-9a-f]{6}$/i.test(b.color) ? b.color : null;
  if (b.links !== undefined && Array.isArray(b.links)) set.links = JSON.stringify(b.links.slice(0, 6).map((l) => ({ title: String(l.title || '').slice(0, 40), url: String(l.url || '').slice(0, 300) })).filter((l) => /^https?:\/\//i.test(l.url)));
  if (b.privacy !== undefined && typeof b.privacy === 'object') set.privacy = JSON.stringify({ ...J(user.privacy, {}), ...b.privacy });
  if (b.prefs !== undefined && typeof b.prefs === 'object') { const s = JSON.stringify(b.prefs); if (s.length < 20000) set.prefs = s; }
  if (b.taste !== undefined && Array.isArray(b.taste)) set.taste = JSON.stringify(b.taste.slice(0, 20).map((x) => String(x).slice(0, 40)));
  if (b.notify !== undefined && typeof b.notify === 'object') set.notify = JSON.stringify({ ...J(user.notify, {}), ...b.notify });
  if (b.username !== undefined && String(b.username).toLowerCase() !== user.username.toLowerCase()) {
    const un = String(b.username).trim().toLowerCase();
    if (!validUsername(un) || RESERVED.has(un)) fail(400, 'bad_username', 'Логин: 3–24 символа, латиница, цифры, «_» и «.»');
    if (one('SELECT id FROM users WHERE username=? AND id<>?', un, user.id)) fail(409, 'username_taken', 'Этот логин уже занят');
    set.username = un;
  }
  const keys = Object.keys(set);
  if (keys.length) {
    run(`UPDATE users SET ${keys.map((k) => k + '=?').join(',')} WHERE id=?`, ...keys.map((k) => set[k]), user.id);
    const u2 = one('SELECT * FROM users WHERE id=?', user.id); run('UPDATE users SET q=? WHERE id=?', userQ(u2), user.id);
  }
  return privateUser(one('SELECT * FROM users WHERE id=?', user.id));
});
function imageExt(buf) {
  if (buf[0] === 0xff && buf[1] === 0xd8) return '.jpg';
  if (buf.slice(0, 8).equals(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]))) return '.png';
  if (buf.slice(0, 4).toString() === 'RIFF' && buf.slice(8, 12).toString() === 'WEBP') return '.webp';
  if (buf.slice(0, 3).toString() === 'GIF') return '.gif';
  return null;
}
for (const kind of ['avatar', 'banner']) {
  route('PUT', `/api/me/${kind}`, async (req) => {
    const { user } = auth(req); const buf = await readBody(req, 6 << 20);
    const ext = imageExt(buf); if (!ext) fail(400, 'bad_image', 'Поддерживаются JPG, PNG, WEBP, GIF');
    const name = `${user.id}_${token(6)}${ext}`; const dir = DIRS[kind + 's'];
    await fsp.writeFile(path.join(dir, name), buf);
    if (user[kind]) fsp.unlink(path.join(dir, user[kind])).catch(() => {});
    run(`UPDATE users SET ${kind}=? WHERE id=?`, name, user.id);
    return privateUser(one('SELECT * FROM users WHERE id=?', user.id));
  });
  route('DELETE', `/api/me/${kind}`, (req) => {
    const { user } = auth(req);
    if (user[kind]) fsp.unlink(path.join(DIRS[kind + 's'], user[kind])).catch(() => {});
    run(`UPDATE users SET ${kind}=NULL WHERE id=?`, user.id);
    return privateUser(one('SELECT * FROM users WHERE id=?', user.id));
  });
}
route('POST', '/api/me/password', async (req) => {
  const { user, token: tok } = auth(req); const b = await readJSON(req);
  if (user.pass !== '!' && !(await checkPass(String(b.old || ''), user.pass))) fail(400, 'bad_password', 'Текущий пароль неверный');
  if (String(b.password || '').length < 6) fail(400, 'bad_password', 'Новый пароль должен быть от 6 символов');
  run('UPDATE users SET pass=? WHERE id=?', await hashPass(String(b.password)), user.id);
  run('DELETE FROM sessions WHERE user_id=? AND token<>?', user.id, tok);
  return { ok: true };
});
route('GET', '/api/me/sessions', (req) => { const { user, token: tok } = auth(req); return all('SELECT token, device, ip, created_at createdAt, last_used lastUsed FROM sessions WHERE user_id=? ORDER BY last_used DESC', user.id).map((s) => ({ id: sha1(s.token).slice(0, 16), device: s.device, ip: s.ip, createdAt: s.createdAt, lastUsed: s.lastUsed, current: s.token === tok })); });
route('DELETE', '/api/me/sessions/:id', (req, { params }) => { const { user, token: tok } = auth(req); for (const s of all('SELECT token FROM sessions WHERE user_id=?', user.id)) if (sha1(s.token).slice(0, 16) === params.id && s.token !== tok) run('DELETE FROM sessions WHERE token=?', s.token); return { ok: true }; });
route('DELETE', '/api/me/sessions', (req) => { const { user, token: tok } = auth(req); run('DELETE FROM sessions WHERE user_id=? AND token<>?', user.id, tok); return { ok: true }; });
route('POST', '/api/me/telegram/link', (req) => {
  const { user } = auth(req); if (!bot.username) fail(503, 'bot_unavailable', 'Telegram-бот не настроен на сервере');
  const code = token(12); run('INSERT INTO codes(code,kind,user_id,data,created_at,expires_at) VALUES(?,?,?,?,?,?)', code, 'link', user.id, '{}', now(), now() + 15 * 60000);
  return { url: `https://t.me/${bot.username}?start=link_${code}` };
});
route('DELETE', '/api/me/telegram', (req) => { const { user } = auth(req); run('UPDATE users SET tg_id=NULL, tg_username=NULL WHERE id=?', user.id); return privateUser(one('SELECT * FROM users WHERE id=?', user.id)); });
route('DELETE', '/api/me', async (req) => {
  const { user } = auth(req); const b = await readJSON(req);
  if (user.pass !== '!' && !(await checkPass(String(b.password || ''), user.pass))) fail(400, 'bad_password', 'Неверный пароль');
  for (const k of ['avatar', 'banner']) if (user[k]) fsp.unlink(path.join(DIRS[k + 's'], user[k])).catch(() => {});
  run('DELETE FROM users WHERE id=?', user.id);
  return { ok: true };
});
route('GET', '/api/me/stats', (req, { query }) => {
  const { user } = auth(req); const tz = clamp(Number(query.get('tz')) || 0, -900, 900) * 60000;
  const days = []; const start = Math.floor((now() - tz) / DAY) * DAY + tz - 13 * DAY;
  const rows = all('SELECT played_at, listened FROM plays WHERE user_id=? AND played_at>=?', user.id, start);
  for (let i = 0; i < 14; i++) days.push({ date: start + i * DAY, plays: 0, seconds: 0 });
  for (const r of rows) { const i = Math.floor((r.played_at - start) / DAY); if (days[i]) { days[i].plays++; days[i].seconds += r.listened || 0; } }
  const tot = one('SELECT COUNT(*) c, SUM(listened) s, COUNT(DISTINCT artist_key) a, COUNT(DISTINCT track_id) t FROM plays WHERE user_id=?', user.id);
  return { plays: tot.c, seconds: tot.s || 0, artists: tot.a, tracks: tot.t, level: levelOf(tot.c), days, topArtists: topArtists(user.id, 0, 10), topTracks: topTracks(user.id, 0, 10), monthArtists: topArtists(user.id, now() - 30 * DAY, 5) };
});
route('GET', '/api/me/notifications', (req) => {
  const { user } = auth(req);
  return all('SELECT * FROM notifications WHERE user_id=? ORDER BY id DESC LIMIT 50', user.id).map((n) => ({ id: n.id, type: n.type, data: J(n.data, {}), createdAt: n.created_at, read: !!n.read, actor: publicUser(one('SELECT * FROM users WHERE id=?', n.actor_id), true) }));
});
route('POST', '/api/me/notifications/read', (req) => { const { user } = auth(req); run('UPDATE notifications SET read=1 WHERE user_id=?', user.id); return { ok: true }; });

/* — Пользователи и друзья — */
route('GET', '/api/users/search', (req, { query }) => {
  auth(req); const q = lower(query.get('q') || '').trim().replace(/^@/, ''); if (!q) return [];
  return all('SELECT * FROM users WHERE q LIKE ? AND banned=0 ORDER BY (username=?) DESC, last_seen DESC LIMIT 20', `%${q}%`, q).map((u) => publicUser(u, true));
});
route('GET', '/api/users/:username', (req, { params }) => {
  const me = auth(req).user; const u = one('SELECT * FROM users WHERE username=?', decodeURIComponent(params.username));
  if (!u || u.banned) fail(404, 'not_found', 'Пользователь не найден');
  const pv = J(u.privacy, {}); const self = u.id === me.id;
  const following = !!one('SELECT 1 FROM follows WHERE follower=? AND followee=?', me.id, u.id);
  const followedBy = !!one('SELECT 1 FROM follows WHERE follower=? AND followee=?', u.id, me.id);
  const hidden = !self && pv.publicProfile === false && !(following && followedBy);
  return {
    user: publicUser(u), self, hidden,
    relation: { following, followedBy, friends: following && followedBy },
    counts: { friends: friendsCount(u.id), followers: followersCount(u.id), following: followingCount(u.id), plays: playsCount(u.id) },
    nowPlaying: hidden ? null : nowPlayingOf(u),
    topArtists: hidden || pv.showStats === false ? [] : topArtists(u.id, 0, 8),
    topTracks: hidden || pv.showStats === false ? [] : topTracks(u.id, 0, 8),
    compatibility: self || hidden ? null : compatibility(me.id, u.id),
    me: self ? null : publicUser(me, true),
  };
});
route('GET', '/api/users/:username/friends', (req, { params, query }) => {
  auth(req); const u = one('SELECT * FROM users WHERE username=?', decodeURIComponent(params.username)); if (!u) fail(404, 'not_found');
  const kind = query.get('kind') || 'friends';
  const sql = kind === 'followers' ? 'SELECT u.* FROM follows f JOIN users u ON u.id=f.follower WHERE f.followee=? ORDER BY f.created_at DESC LIMIT 200'
    : kind === 'following' ? 'SELECT u.* FROM follows f JOIN users u ON u.id=f.followee WHERE f.follower=? ORDER BY f.created_at DESC LIMIT 200'
      : 'SELECT u.* FROM follows f JOIN follows g ON g.follower=f.followee AND g.followee=f.follower JOIN users u ON u.id=f.followee WHERE f.follower=? ORDER BY u.last_seen DESC LIMIT 200';
  return all(sql, u.id).map((x) => ({ ...publicUser(x, true), nowPlaying: nowPlayingOf(x) }));
});
route('POST', '/api/users/:username/follow', (req, { params }) => {
  const me = auth(req).user; const u = one('SELECT * FROM users WHERE username=?', decodeURIComponent(params.username));
  if (!u || u.banned) fail(404, 'not_found', 'Пользователь не найден'); if (u.id === me.id) fail(400, 'self', 'Нельзя подписаться на себя');
  const r = run('INSERT OR IGNORE INTO follows(follower,followee,created_at) VALUES(?,?,?)', me.id, u.id, now());
  const mutual = !!one('SELECT 1 FROM follows WHERE follower=? AND followee=?', u.id, me.id);
  if (r.changes) notify(u.id, mutual ? 'friend' : 'follow', me.id);
  return { following: true, friends: mutual };
});
route('DELETE', '/api/users/:username/follow', (req, { params }) => {
  const me = auth(req).user; const u = one('SELECT * FROM users WHERE username=?', decodeURIComponent(params.username)); if (!u) fail(404, 'not_found');
  run('DELETE FROM follows WHERE follower=? AND followee=?', me.id, u.id); return { following: false, friends: false };
});
route('GET', '/api/friends/activity', (req) => {
  const me = auth(req).user;
  return all('SELECT u.* FROM follows f JOIN follows g ON g.follower=f.followee AND g.followee=f.follower JOIN users u ON u.id=f.followee WHERE f.follower=? ORDER BY u.last_seen DESC LIMIT 50', me.id)
    .map((x) => ({ ...publicUser(x, true), nowPlaying: nowPlayingOf(x) }));
});
route('GET', '/api/media/:kind/:file', (req, { params, res }) => {
  if (!['avatars', 'banners'].includes(params.kind) || !/^[\w.-]+$/.test(params.file)) fail(404, 'not_found');
  return sendFile(req, res, path.join(DIRS[params.kind], params.file));
});

/* — Музыка — */
route('GET', '/api/search', async (req, { query }) => {
  auth(req); const q = String(query.get('q') || '').trim().slice(0, 100); const type = query.get('type') || 'all';
  if (!q) return { tracks: [], artists: [], users: [] };
  const [au, auUsers] = await Promise.all([type === 'users' ? [] : auSearch(q, type === 'tracks' ? 40 : 24), type === 'all' || type === 'artists' ? safe(audius('/users/search', { query: q, limit: 10 })) : []]);
  const tracks = type === 'users' ? [] : [...catalogSearch(q, 30), ...au];
  const artists = type === 'all' || type === 'artists' ? [...catalogArtists(q), ...(auUsers || []).filter((u) => u.track_count > 0).map(auArtist)] : [];
  const users = type === 'all' || type === 'users' ? all('SELECT * FROM users WHERE q LIKE ? AND banned=0 ORDER BY last_seen DESC LIMIT 10', `%${lower(q).replace(/^@/, '')}%`).map((u) => publicUser(u, true)) : [];
  return { tracks, artists, users };
});
route('GET', '/api/trending', async (req, { query }) => { auth(req); return auTrending(query.get('genre') || undefined, 40, query.get('time') || 'week'); });
route('GET', '/api/catalog', (req, { query }) => { auth(req); const off = Number(query.get('offset')) || 0; return all('SELECT * FROM tracks ORDER BY id DESC LIMIT 50 OFFSET ?', off).map(slTrack); });
route('GET', '/api/artist', async (req, { query }) => {
  auth(req); const id = String(query.get('id') || '');
  if (id.startsWith('sl:')) {
    const name = id.slice(3); const tracks = all('SELECT * FROM tracks WHERE q LIKE ? ORDER BY plays DESC LIMIT 200', `%${lower(name)}%`).filter((t) => lower(t.artist).includes(lower(name))).map(slTrack);
    const au = await auSearch(name, 12);
    return { artist: { id, source: 'soundless', name, avatar: tracks[0]?.cover || null, tracks: tracks.length }, tracks: [...tracks, ...au.filter((t) => lower(t.artist).includes(lower(name)))] };
  }
  if (id.startsWith('au:')) {
    const uid = id.slice(3);
    const [u, tr] = await Promise.all([safe(audius('/users/' + encodeURIComponent(uid)), null), safe(audius(`/users/${encodeURIComponent(uid)}/tracks`, { limit: 50, sort: 'plays' }))]);
    if (!u) fail(404, 'not_found', 'Исполнитель не найден');
    return { artist: auArtist(u), tracks: tr.map(auTrack).filter(Boolean) };
  }
  fail(400, 'bad_id');
});
route('GET', '/api/tracks/:id', async (req, { params }) => {
  auth(req); const id = decodeURIComponent(params.id);
  if (id.startsWith('sl:')) { const t = slTrack(one('SELECT * FROM tracks WHERE id=?', +id.slice(3))); if (!t) fail(404, 'not_found'); return t; }
  if (id.startsWith('au:')) { const t = auTrack(await safe(audius('/tracks/' + encodeURIComponent(id.slice(3))), null)); if (!t) fail(404, 'not_found'); return t; }
  fail(404, 'not_found');
});
const transcoding = new Map();
async function transcoded(t, kbps) {
  // Кэш перекодированных версий для выбора качества в приложении (192/256/320 kbps MP3)
  const src = path.join(DIRS.tracks, t.file);
  const orig = t.duration ? (t.size * 8) / t.duration / 1000 : 9999;
  if (!ffmpegOk || !kbps || orig <= kbps * 1.15) return { file: src, mime: t.mime };
  const out = path.join(DIRS.tmp, `${t.id}_${kbps}.mp3`);
  if (fs.existsSync(out)) { fs.utimes(out, new Date(), new Date(), () => {}); return { file: out, mime: 'audio/mpeg' }; }
  const key = out;
  if (!transcoding.has(key)) {
    transcoding.set(key, sh(CFG.ffmpeg, ['-y', '-hide_banner', '-loglevel', 'error', '-i', src, '-map', '0:a:0', '-c:a', 'libmp3lame', '-b:a', kbps + 'k', '-map_metadata', '-1', out + '.part.mp3'], 300000)
      .then(async (r) => { if (r.code === 0) await fsp.rename(out + '.part.mp3', out); else await fsp.unlink(out + '.part.mp3').catch(() => {}); })
      .finally(() => transcoding.delete(key)));
  }
  await transcoding.get(key);
  return fs.existsSync(out) ? { file: out, mime: 'audio/mpeg' } : { file: src, mime: t.mime };
}
setInterval(async () => { // чистим кэш перекодирования старше 14 дней
  for (const f of await fsp.readdir(DIRS.tmp).catch(() => [])) { const p = path.join(DIRS.tmp, f); const st = await fsp.stat(p).catch(() => null); if (st && now() - st.mtimeMs > 14 * DAY) fsp.unlink(p).catch(() => {}); }
}, 6 * 3600e3).unref();
route('GET', '/api/stream/:id', async (req, { params, res, query }) => {
  auth(req); const t = one('SELECT * FROM tracks WHERE id=?', Number(params.id)); if (!t) fail(404, 'not_found', 'Трек не найден');
  const q = { low: 192, medium: 256, high: 320 }[query.get('q')] || 0;
  const f = await transcoded(t, q);
  return sendFile(req, res, f.file, f.mime, 'private, max-age=86400');
});
route('GET', '/api/cover/:id', (req, { params, res }) => {
  const t = one('SELECT cover FROM tracks WHERE id=?', Number(params.id)); if (!t?.cover) fail(404, 'not_found');
  return sendFile(req, res, path.join(DIRS.covers, t.cover));
});
route('GET', '/api/lyrics', async (req, { query }) => {
  auth(req);
  return getLyrics({ id: query.get('id') || '', title: query.get('title') || '', artist: query.get('artist') || '', album: query.get('album') || '', duration: Number(query.get('duration')) || 0 });
});
route('POST', '/api/translate', async (req) => {
  auth(req); rateLimit(req, 'tr', 40, 600e3); const b = await readJSON(req);
  if (!Array.isArray(b.lines) || !b.lines.length) fail(400, 'bad_request');
  try { return await translateLines(b.lines, String(b.to || '').slice(0, 5)); } catch (e) { log('[translate]', e.message); fail(502, 'translate_failed', 'Не удалось перевести текст'); }
});
route('POST', '/api/history', async (req) => {
  const { user } = auth(req); const b = await readJSON(req); const t = sanitizeTrack(b.track);
  if (!t) fail(400, 'bad_track');
  const listened = clamp(Math.round(+b.listened || 0), 0, 7200);
  const last = one('SELECT track_id, played_at FROM plays WHERE user_id=? ORDER BY id DESC LIMIT 1', user.id);
  if (last && last.track_id === t.id && now() - last.played_at < Math.max(20000, t.duration * 500)) return { ok: true, dup: true };
  run('INSERT INTO plays(user_id,track_id,title,artist,artist_key,cover,source,listened,data,played_at) VALUES(?,?,?,?,?,?,?,?,?,?)', user.id, t.id, t.title, t.artist, lower(primaryArtist(t.artist)), t.cover, t.source, listened, JSON.stringify(t), now());
  if (t.id.startsWith('sl:')) run('UPDATE tracks SET plays=plays+1 WHERE id=?', +t.id.slice(3));
  return { ok: true, level: levelOf(playsCount(user.id)) };
});
route('GET', '/api/history', (req, { query }) => { const { user } = auth(req); return all('SELECT data, played_at FROM plays WHERE user_id=? ORDER BY id DESC LIMIT ?', user.id, clamp(Number(query.get('limit')) || 100, 1, 500)).map((r) => ({ ...J(r.data, {}), playedAt: r.played_at })); });
route('DELETE', '/api/history', (req) => { const { user } = auth(req); run('DELETE FROM plays WHERE user_id=?', user.id); return { ok: true }; });
route('POST', '/api/presence', async (req) => {
  const { user } = auth(req); const b = await readJSON(req); const t = b.track ? sanitizeTrack(b.track) : null;
  run('UPDATE users SET last_seen=?, now_playing=? WHERE id=?', now(), t ? JSON.stringify({ track: t, playing: !!b.playing, position: Math.round(+b.position || 0), at: now() }) : null, user.id);
  return { ok: true };
});

/* — Библиотека — */
route('GET', '/api/likes', (req) => { const { user } = auth(req); return all('SELECT data, created_at FROM likes WHERE user_id=? ORDER BY created_at DESC', user.id).map((r) => ({ ...J(r.data, {}), likedAt: r.created_at })); });
route('PUT', '/api/likes', async (req) => { const { user } = auth(req); const t = sanitizeTrack((await readJSON(req)).track); if (!t) fail(400, 'bad_track'); run('INSERT OR REPLACE INTO likes(user_id,track_id,data,created_at) VALUES(?,?,?,?)', user.id, t.id, JSON.stringify(t), now()); return { ok: true }; });
route('DELETE', '/api/likes/:id', (req, { params }) => { const { user } = auth(req); run('DELETE FROM likes WHERE user_id=? AND track_id=?', user.id, decodeURIComponent(params.id)); return { ok: true }; });
function playlistOut(p, withTracks) {
  const tracks = all('SELECT id, data FROM playlist_tracks WHERE playlist_id=? ORDER BY pos, id', p.id).map((r) => ({ ...J(r.data, {}), entryId: r.id }));
  return { id: p.id, name: p.name, description: p.description, public: !!p.public, createdAt: p.created_at, updatedAt: p.updated_at, count: tracks.length, covers: tracks.filter((t) => t.cover).slice(0, 4).map((t) => t.cover), duration: tracks.reduce((s, t) => s + (t.duration || 0), 0), tracks: withTracks ? tracks : undefined, owner: publicUser(one('SELECT * FROM users WHERE id=?', p.user_id), true) };
}
const ownPlaylist = (uid, id) => { const p = one('SELECT * FROM playlists WHERE id=?', Number(id)); if (!p || p.user_id !== uid) fail(404, 'not_found', 'Плейлист не найден'); return p; };
route('GET', '/api/playlists', (req) => { const { user } = auth(req); return all('SELECT * FROM playlists WHERE user_id=? ORDER BY updated_at DESC', user.id).map((p) => playlistOut(p, false)); });
route('POST', '/api/playlists', async (req) => {
  const { user } = auth(req); const b = await readJSON(req); const name = String(b.name || '').trim().slice(0, 80) || 'Новый плейлист';
  const r = run('INSERT INTO playlists(user_id,name,description,public,created_at,updated_at) VALUES(?,?,?,?,?,?)', user.id, name, String(b.description || '').slice(0, 300), b.public === false ? 0 : 1, now(), now());
  return playlistOut(one('SELECT * FROM playlists WHERE id=?', Number(r.lastInsertRowid)), true);
});
route('GET', '/api/playlists/:id', (req, { params }) => {
  const { user } = auth(req); const p = one('SELECT * FROM playlists WHERE id=?', Number(params.id));
  if (!p || (p.user_id !== user.id && !p.public)) fail(404, 'not_found', 'Плейлист не найден');
  return playlistOut(p, true);
});
route('PATCH', '/api/playlists/:id', async (req, { params }) => {
  const { user } = auth(req); const p = ownPlaylist(user.id, params.id); const b = await readJSON(req);
  run('UPDATE playlists SET name=?, description=?, public=?, updated_at=? WHERE id=?', b.name !== undefined ? String(b.name).trim().slice(0, 80) || p.name : p.name, b.description !== undefined ? String(b.description).slice(0, 300) : p.description, b.public !== undefined ? (b.public ? 1 : 0) : p.public, now(), p.id);
  if (Array.isArray(b.order)) tx(() => b.order.forEach((eid, i) => run('UPDATE playlist_tracks SET pos=? WHERE id=? AND playlist_id=?', i, Number(eid), p.id)));
  return playlistOut(one('SELECT * FROM playlists WHERE id=?', p.id), true);
});
route('DELETE', '/api/playlists/:id', (req, { params }) => { const { user } = auth(req); const p = ownPlaylist(user.id, params.id); run('DELETE FROM playlists WHERE id=?', p.id); return { ok: true }; });
route('POST', '/api/playlists/:id/tracks', async (req, { params }) => {
  const { user } = auth(req); const p = ownPlaylist(user.id, params.id); const b = await readJSON(req);
  const list = (Array.isArray(b.tracks) ? b.tracks : [b.track]).map(sanitizeTrack).filter(Boolean).slice(0, 500);
  if (!list.length) fail(400, 'bad_track');
  const max = one('SELECT MAX(pos) m FROM playlist_tracks WHERE playlist_id=?', p.id).m ?? -1;
  tx(() => list.forEach((t, i) => run('INSERT INTO playlist_tracks(playlist_id,pos,track_id,data,added_at) VALUES(?,?,?,?,?)', p.id, max + 1 + i, t.id, JSON.stringify(t), now())));
  run('UPDATE playlists SET updated_at=? WHERE id=?', now(), p.id);
  return playlistOut(one('SELECT * FROM playlists WHERE id=?', p.id), true);
});
route('DELETE', '/api/playlists/:id/tracks/:entry', (req, { params }) => {
  const { user } = auth(req); const p = ownPlaylist(user.id, params.id);
  run('DELETE FROM playlist_tracks WHERE id=? AND playlist_id=?', Number(params.entry), p.id); run('UPDATE playlists SET updated_at=? WHERE id=?', now(), p.id);
  return playlistOut(one('SELECT * FROM playlists WHERE id=?', p.id), true);
});
route('GET', '/api/users/:username/playlists', (req, { params }) => {
  const me = auth(req).user; const u = one('SELECT * FROM users WHERE username=?', decodeURIComponent(params.username)); if (!u) fail(404, 'not_found');
  return all(`SELECT * FROM playlists WHERE user_id=? ${u.id === me.id ? '' : 'AND public=1'} ORDER BY updated_at DESC`, u.id).map((p) => playlistOut(p, false));
});

/* — Главная — */
function shuffle(a) { a = a.slice(); for (let i = a.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [a[i], a[j]] = [a[j], a[i]]; } return a; }
function uniq(list) { const seen = new Set(); return list.filter((t) => t && !seen.has(t.id) && seen.add(t.id)); }
route('GET', '/api/home', async (req) => {
  const { user } = auth(req);
  const taste = J(user.taste, []);
  const arts = topArtists(user.id, now() - 60 * DAY, 4);
  const recent = recentTracks(user.id, 16);
  const recentIds = new Set(recent.map((t) => t.id));
  const likes = all('SELECT data FROM likes WHERE user_id=? ORDER BY created_at DESC LIMIT 60', user.id).map((r) => J(r.data, null)).filter(Boolean);
  const genre = taste[0] || recent.find((t) => t.genre)?.genre || undefined;
  const last = recent[0];
  const [byArtists, trending, genreTrend, byTrack] = await Promise.all([
    Promise.all(arts.map(async (a) => [...catalogSearch(a.name, 6), ...(await auSearch(a.name, 6))].filter((t) => lower(t.artist).includes(lower(a.name))))).then((x) => x.flat()),
    auTrending(undefined, 30),
    genre ? auTrending(genre, 20) : Promise.resolve([]),
    last ? Promise.all([Promise.resolve(catalogSearch(primaryArtist(last.artist), 8)), auSearch(primaryArtist(last.artist), 8), last.genre ? auTrending(last.genre, 12, 'month') : []]).then((x) => x.flat()) : Promise.resolve([]),
  ]);
  const catalogNew = all('SELECT * FROM tracks ORDER BY id DESC LIMIT 20').map(slTrack);
  const forYou = uniq(shuffle([...byArtists, ...likes.slice(0, 20), ...topTracks(user.id, now() - 30 * DAY, 10)])).slice(0, 40);
  const discover = uniq(shuffle([...genreTrend, ...trending.slice(0, 15), ...catalogNew.slice(0, 10)]).filter((t) => !recentIds.has(t.id))).slice(0, 40);
  const tasteByTrack = uniq(byTrack.filter((t) => t.id !== last?.id)).slice(0, 30);
  const mix = uniq(shuffle([...forYou.slice(0, 18), ...discover.slice(0, 12)])).slice(0, 30);
  const friends = all('SELECT u.* FROM follows f JOIN follows g ON g.follower=f.followee AND g.followee=f.follower JOIN users u ON u.id=f.followee WHERE f.follower=? ORDER BY u.last_seen DESC LIMIT 20', user.id)
    .map((x) => ({ ...publicUser(x, true), nowPlaying: nowPlayingOf(x) })).filter((x) => x.nowPlaying);
  const artistsOf = (list) => [...new Set(list.map((t) => primaryArtist(t.artist)).filter(Boolean))];
  return {
    mix: mix.length ? mix : trending,
    sections: [
      { id: 'forYou', title: 'Для вас', tracks: forYou.length ? forYou : trending.slice(0, 20), artists: artistsOf(forYou.length ? forYou : trending) },
      { id: 'discover', title: 'Открытия', tracks: discover.length ? discover : trending, artists: artistsOf(discover.length ? discover : trending) },
      { id: 'byTrack', title: 'Вкус по треку', tracks: tasteByTrack.length ? tasteByTrack : shuffle(trending).slice(0, 20), seed: last || null, artists: artistsOf(tasteByTrack) },
    ],
    recent, trending, catalogNew, friends, topArtists: arts, genre: genre || null,
  };
});

/* — Сервер — */
const server = http.createServer(async (req, res) => {
  if (req.method === 'OPTIONS') { res.writeHead(204, CORS); return res.end(); }
  const url = new URL(req.url, 'http://local');
  if (!url.pathname.startsWith('/api/')) {
    if (url.pathname === '/' || url.pathname === '') { res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', ...CORS }); return res.end(`<!doctype html><meta charset="utf-8"><title>Soundless</title><body style="background:#0a0a0b;color:#fff;font:16px 'Segoe UI',system-ui;display:grid;place-items:center;height:100vh;margin:0"><div style="text-align:center"><h1 style="font-weight:800;letter-spacing:-.02em">Soundless</h1><p style="opacity:.6">Сервер работает · v${VERSION}</p><p><a style="color:#fff" href="${esc(CFG.downloadUrl)}">Скачать приложение</a></p></div>`); }
    return send(res, 404, { error: 'not_found' });
  }
  const methodForMatch = req.method === 'HEAD' ? 'GET' : req.method;
  for (const r of routes) {
    if (r.method !== methodForMatch) continue;
    const m = r.re.exec(url.pathname); if (!m) continue;
    try {
      if (flag('maintenance') && !/^\/api\/(health|config)$/.test(url.pathname)) fail(503, 'maintenance', 'Идут технические работы. Скоро вернёмся!');
      const out = await r.handler(req, { params: m.groups || {}, query: url.searchParams, res, url });
      if (!res.headersSent && out !== undefined) send(res, 200, out);
    } catch (e) {
      if (e instanceof HttpError) { if (!res.headersSent) send(res, e.status, { error: e.code, message: e.message }); }
      else { log('[error]', req.method, url.pathname, e.stack || e); if (!res.headersSent) send(res, 500, { error: 'internal', message: 'Ошибка сервера' }); }
    }
    return;
  }
  send(res, 404, { error: 'not_found', message: 'Метод не найден' });
});

/* ───────────────────────────── Telegram-бот ───────────────────────────── */
const EMOJI = {
  settings: ['5870982283724328568', '⚙️'], profile: ['5870994129244131212', '👤'], people: ['5870772616305839506', '👥'], userOk: ['5891207662678317861', '👤'],
  userNo: ['5893192487324880883', '👤'], file: ['5870528606328852614', '📁'], smile: ['5870764288364252592', '🙂'], growth: ['5870930636742595124', '📊'],
  stats: ['5870921681735781843', '📊'], home: ['5873147866364514353', '🏘'], lock: ['6037249452824072506', '🔒'], unlock: ['6037496202990194718', '🔓'],
  megaphone: ['6039422865189638057', '📣'], check: ['5870633910337015697', '✅'], cross: ['5870657884844462243', '❌'], pencil: ['5870676941614354370', '🖋'],
  trash: ['5870875489362513438', '🗑'], down: ['5893057118545646106', '📰'], clip: ['6039451237743595514', '📎'], link: ['5769289093221454192', '🔗'],
  info: ['6028435952299413210', 'ℹ️'], bot: ['6030400221232501136', '🤖'], eye: ['6037397706505195857', '👁'], hidden: ['6037243349675544634', '👁'],
  send: ['5963103826075456248', '⬆️'], download: ['6039802767931871481', '⬇️'], bell: ['6039486778597970865', '🔔'], gift: ['6032644646587338669', '🎁'],
  clock: ['5983150113483134607', '⏰'], party: ['6041731551845159060', '🎉'], font: ['5870801517140775623', '🔗'], write: ['5870753782874246579', '✍️'],
  media: ['6035128606563241721', '🖼'], geo: ['6042011682497106307', '📍'], wallet: ['5769126056262898415', '👛'], box: ['5884479287171485878', '📦'],
  calendar: ['5890937706803894250', '📅'], tag: ['5886285355279193209', '🏷'], timePassed: ['5775896410780079073', '🕓'], apps: ['5778672437122045013', '📦'],
  brush: ['6050679691004612757', '🖌'], addText: ['5771851822897566479', '🔡'], format: ['5778479949572738874', '↔️'], code: ['5940433880585605708', '🔨'],
  loading: ['5345906554510012647', '🔄'],
};
let premium = CFG.premiumEmoji;
const em = (k) => { const e = EMOJI[k]; if (!e) return ''; return premium ? `<tg-emoji emoji-id="${e[0]}">${e[1]}</tg-emoji>` : e[1]; };
const B = (text, action, icon, style) => {
  const b = { text };
  if (/^(https?|tg):\/\//.test(action)) b.url = action; else b.callback_data = action;
  if (icon && EMOJI[icon]) b.icon_custom_emoji_id = EMOJI[icon][0];
  if (style) b.style = style;
  return b;
};
const KB = (...rows) => ({ inline_keyboard: rows.filter(Boolean).map((r) => (Array.isArray(r) ? r.filter(Boolean) : [r])).filter((r) => r.length) });
const BACK = (to) => B('◁ Назад', to);
const cbq = (prefix, q) => { let d = prefix + (q ? ':' + q : ''); while (Buffer.byteLength(d) > 64) d = d.slice(0, -1); return d; };
const isAdmin = (id) => CFG.admins.includes(Number(id));
function stripPremium(p) {
  const o = JSON.parse(JSON.stringify(p));
  for (const k of ['text', 'caption']) if (typeof o[k] === 'string') o[k] = o[k].replace(/<tg-emoji[^>]*>(.*?)<\/tg-emoji>/g, '$1');
  const walk = (x) => { if (Array.isArray(x)) x.forEach(walk); else if (x && typeof x === 'object') { delete x.icon_custom_emoji_id; Object.values(x).forEach(walk); } };
  walk(o.reply_markup); return o;
}

const bot = {
  ready: false, username: null, offset: 0, panels: new Map(),
  async api(method, params = {}, retry = true) {
    const r = await fetch(`${CFG.tgApi}/bot${CFG.botToken}/${method}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(params), signal: AbortSignal.timeout(method === 'getUpdates' ? 60000 : 30000) });
    const j = await r.json().catch(() => ({ ok: false, description: 'bad response' }));
    if (j.ok) return j.result;
    const d = String(j.description || '');
    if (/message is not modified/i.test(d)) return null;
    if (j.error_code === 429 && retry) { await sleep(((j.parameters?.retry_after) || 1) * 1000 + 200); return this.api(method, params, false); }
    if (retry && /emoji|icon|ENTITY|DOCUMENT_INVALID|custom/i.test(d) && JSON.stringify(params).match(/tg-emoji|icon_custom_emoji_id/)) {
      if (premium) { premium = false; log('[bot] премиум-эмодзи недоступны (нужен Telegram Premium у владельца бота) — переключаюсь на обычные'); }
      return this.api(method, stripPremium(params), false);
    }
    const e = new Error(`${method}: ${d}`); e.code = j.error_code; throw e;
  },
  send(chat_id, text, reply_markup, extra = {}) { return this.api('sendMessage', { chat_id, text, parse_mode: 'HTML', link_preview_options: { is_disabled: true }, reply_markup, ...extra }); },
  async show(chatId, view, msgId) {
    const { text, kb } = view;
    if (msgId) {
      try { await this.api('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'HTML', link_preview_options: { is_disabled: true }, reply_markup: kb }); this.panels.set(chatId, msgId); return msgId; }
      catch (e) { if (!/not found|can't be edited|no text in the message/i.test(e.message)) log('[bot]', e.message); }
    }
    const m = await this.send(chatId, text, kb); this.panels.set(chatId, m.message_id); return m.message_id;
  },
  getState(chatId) { const r = one('SELECT * FROM bot_state WHERE chat_id=?', chatId); return r ? { state: r.state, data: J(r.data, {}), msgId: r.msg_id } : null; },
  setState(chatId, state, data = {}, msgId = null) { run('INSERT OR REPLACE INTO bot_state(chat_id,state,data,msg_id) VALUES(?,?,?,?)', chatId, state, JSON.stringify(data), msgId); },
  clearState(chatId) { run('DELETE FROM bot_state WHERE chat_id=?', chatId); },
  onRegister(userId) {
    if (!this.ready || !flag('notify_reg')) return;
    const u = one('SELECT * FROM users WHERE id=?', userId); if (!u) return;
    const total = one('SELECT COUNT(*) c FROM users').c;
    for (const a of CFG.admins) this.send(a, `${em('userOk')} <b>Новый пользователь</b>\n\n${esc(u.name)} · @${esc(u.username)}\n${em('people')} Всего: <b>${fmtNum(total)}</b>`, KB([B('Открыть', 'a:u:' + u.id, 'profile')])).catch(() => {});
  },
};

/* — Экраны админ-панели — */
function vMain() {
  const users = one('SELECT COUNT(*) c FROM users').c; const online = one('SELECT COUNT(*) c FROM users WHERE last_seen>?', now() - 120000).c;
  const tracks = one('SELECT COUNT(*) c FROM tracks').c; const today = one('SELECT COUNT(*) c FROM plays WHERE played_at>?', now() - DAY).c;
  return {
    text: `<b>${em('bot')} Soundless · Панель управления</b>\n\n${em('people')} Пользователи: <b>${fmtNum(users)}</b> · онлайн <b>${fmtNum(online)}</b>\n${em('box')} Каталог: <b>${fmtNum(tracks)}</b> ${plural(tracks, 'трек', 'трека', 'треков')}\n${em('growth')} За сутки: <b>${fmtNum(today)}</b> ${plural(today, 'прослушивание', 'прослушивания', 'прослушиваний')}\n\n<i>Отправьте аудиофайл в этот чат, чтобы добавить его в каталог.</i>`,
    kb: KB([B('Статистика', 'a:stats', 'stats'), B('Пользователи', 'a:users:0', 'people')], [B('Каталог', 'a:cat', 'box'), B('Рассылка', 'a:bc', 'megaphone')], [B('Настройки', 'a:set', 'settings'), B('Приложение', CFG.downloadUrl, 'apps')]),
  };
}
function vStats() {
  const c = (sql, ...p) => one(sql, ...p).c || 0;
  const users = c('SELECT COUNT(*) c FROM users'); const d1 = c('SELECT COUNT(*) c FROM users WHERE created_at>?', now() - DAY); const d7 = c('SELECT COUNT(*) c FROM users WHERE created_at>?', now() - 7 * DAY);
  const online = c('SELECT COUNT(*) c FROM users WHERE last_seen>?', now() - 120000); const active = c('SELECT COUNT(DISTINCT user_id) c FROM plays WHERE played_at>?', now() - DAY);
  const p1 = c('SELECT COUNT(*) c FROM plays WHERE played_at>?', now() - DAY); const p7 = c('SELECT COUNT(*) c FROM plays WHERE played_at>?', now() - 7 * DAY);
  const pt = one('SELECT COUNT(*) c, SUM(listened) s FROM plays'); const cat = one('SELECT COUNT(*) c, SUM(size) s FROM tracks');
  const top = all('SELECT title, artist, COUNT(*) c FROM plays WHERE played_at>? GROUP BY track_id ORDER BY c DESC LIMIT 5', now() - DAY);
  const topA = all("SELECT MAX(artist) a, COUNT(*) c FROM plays WHERE played_at>? AND artist_key<>'' GROUP BY artist_key ORDER BY c DESC LIMIT 3", now() - 7 * DAY);
  const bots = c('SELECT COUNT(*) c FROM bot_users WHERE blocked=0');
  return {
    text: `<b>${em('stats')} Статистика</b>\n\n${em('people')} <b>Пользователи</b>\n├ Всего: <b>${fmtNum(users)}</b>\n├ Новых: <b>${fmtNum(d1)}</b> за сутки · <b>${fmtNum(d7)}</b> за неделю\n├ Онлайн: <b>${fmtNum(online)}</b> · активных за сутки <b>${fmtNum(active)}</b>\n└ В боте: <b>${fmtNum(bots)}</b>\n\n${em('growth')} <b>Прослушивания</b>\n├ Сутки: <b>${fmtNum(p1)}</b> · неделя <b>${fmtNum(p7)}</b>\n└ Всего: <b>${fmtNum(pt.c)}</b> · ${fmtNum(Math.round((pt.s || 0) / 3600))} ч\n\n${em('box')} <b>Каталог</b>\n└ ${fmtNum(cat.c)} ${plural(cat.c, 'трек', 'трека', 'треков')} · ${fmtSize(cat.s || 0)}` +
      (top.length ? `\n\n${em('party')} <b>Топ за сутки</b>\n` + top.map((t, i) => `${i + 1}. ${esc(t.artist)} — ${esc(t.title)} <i>(${t.c})</i>`).join('\n') : '') +
      (topA.length ? `\n\n${em('profile')} <b>Артисты недели</b>\n` + topA.map((t) => `· ${esc(primaryArtist(t.a))} <i>(${t.c})</i>`).join('\n') : ''),
    kb: KB([B('Обновить', 'a:stats', 'loading'), BACK('a:main')]),
  };
}
function vUsers(page = 0, q = '') {
  const per = 8; const where = q ? 'WHERE q LIKE ?' : ''; const args = q ? [`%${lower(q)}%`] : [];
  const total = one(`SELECT COUNT(*) c FROM users ${where}`, ...args).c; const pages = Math.max(1, Math.ceil(total / per)); page = clamp(page, 0, pages - 1);
  const list = all(`SELECT * FROM users ${where} ORDER BY id DESC LIMIT ? OFFSET ?`, ...args, per, page * per);
  return {
    text: `<b>${em('people')} Пользователи</b>${q ? ` · поиск «${esc(q)}»` : ''}\n\nНайдено: <b>${fmtNum(total)}</b> · стр. ${page + 1}/${pages}`,
    kb: KB(...list.map((u) => [B(`${u.name} · @${u.username}`, 'a:u:' + u.id, u.banned ? 'lock' : isOnline(u) ? 'userOk' : 'profile')]),
      pages > 1 ? [B('‹', cbq(`a:users:${Math.max(0, page - 1)}`, q)), B(`${page + 1} / ${pages}`, 'a:noop'), B('›', cbq(`a:users:${Math.min(pages - 1, page + 1)}`, q))] : null,
      [B('Найти', 'a:usearch', 'eye'), q ? B('Сбросить', 'a:users:0', 'cross') : null], [BACK('a:main')]),
  };
}
function vUser(id) {
  const u = one('SELECT * FROM users WHERE id=?', id); if (!u) return { text: `${em('cross')} Пользователь не найден`, kb: KB([BACK('a:users:0')]) };
  const plays = playsCount(u.id); const lv = levelOf(plays); const sess = one('SELECT COUNT(*) c FROM sessions WHERE user_id=?', u.id).c;
  const np = nowPlayingOf(u);
  return {
    text: `<b>${em('profile')} ${esc(u.name)}</b>${u.verified ? ' ' + em('check') : ''}\n@${esc(u.username)} · ID <code>${u.id}</code>\n\n${em('calendar')} Регистрация: ${fmtDate(u.created_at)}\n${em('clock')} ${isOnline(u) ? '<b>В сети</b>' : 'Был(а): ' + (u.last_seen ? new Date(u.last_seen).toLocaleString('ru-RU') : '—')}\n${em('growth')} Уровень <b>${lv.level}</b> · ${fmtNum(plays)} ${plural(plays, 'прослушивание', 'прослушивания', 'прослушиваний')}\n${em('people')} Друзей: ${friendsCount(u.id)} · подписчиков: ${followersCount(u.id)}\n${em('link')} Telegram: ${u.tg_id ? (u.tg_username ? '@' + esc(u.tg_username) : '<code>' + u.tg_id + '</code>') : 'не привязан'}\n${em('lock')} Сессий: ${sess}` +
      (np ? `\n\n${em('eye')} Слушает: <b>${esc(np.track.artist)} — ${esc(np.track.title)}</b>` : '') + (u.banned ? `\n\n${em('lock')} <b>Аккаунт заблокирован</b>` : ''),
    kb: KB(
      [u.banned ? B('Разблокировать', 'a:ub:' + u.id, 'unlock', 'success') : B('Заблокировать', 'a:ub:' + u.id, 'lock', 'danger'), B(u.verified ? 'Снять галочку' : 'Верифицировать', 'a:uv:' + u.id, u.verified ? 'cross' : 'check')],
      [B('Сбросить пароль', 'a:ur:' + u.id, 'pencil'), B('Завершить сессии', 'a:us:' + u.id, 'timePassed')],
      [B('Удалить аккаунт', 'a:ud:' + u.id, 'trash', 'danger')], [BACK('a:users:0')]),
  };
}
function vCatalog() {
  const c = one('SELECT COUNT(*) c, SUM(size) s, SUM(plays) p FROM tracks'); const noLy = one("SELECT COUNT(*) c FROM tracks WHERE lyrics IS NULL OR lyrics=''").c;
  return {
    text: `<b>${em('box')} Каталог</b>\n\n${em('file')} Треков: <b>${fmtNum(c.c)}</b> · ${fmtSize(c.s || 0)}\n${em('growth')} Прослушиваний: <b>${fmtNum(c.p || 0)}</b>\n${em('addText')} Без своего текста: ${fmtNum(noLy)} <i>(тексты подтянутся из LRCLIB)</i>\n\n${em('send')} Чтобы добавить трек — просто отправьте аудиофайл (MP3, M4A, FLAC, OGG) в этот чат. Название, исполнитель и обложка возьмутся из файла.${ffmpegOk ? '' : `\n\n${em('info')} <i>ffmpeg не найден: обложки из файлов и нормализация громкости недоступны.</i>`}`,
    kb: KB([B('Все треки', 'a:tl:0', 'file'), B('Найти', 'a:tsearch', 'eye')], [BACK('a:main')]),
  };
}
function vTracks(page = 0, q = '') {
  const per = 8; const where = q ? 'WHERE q LIKE ?' : ''; const args = q ? [`%${lower(q)}%`] : [];
  const total = one(`SELECT COUNT(*) c FROM tracks ${where}`, ...args).c; const pages = Math.max(1, Math.ceil(total / per)); page = clamp(page, 0, pages - 1);
  const list = all(`SELECT * FROM tracks ${where} ORDER BY id DESC LIMIT ? OFFSET ?`, ...args, per, page * per);
  return {
    text: `<b>${em('file')} Треки</b>${q ? ` · «${esc(q)}»` : ''}\n\nВсего: <b>${fmtNum(total)}</b> · стр. ${page + 1}/${pages}`,
    kb: KB(...list.map((t) => [B(`${t.artist} — ${t.title}`.slice(0, 60), 'a:t:' + t.id, 'file')]),
      pages > 1 ? [B('‹', cbq(`a:tl:${Math.max(0, page - 1)}`, q)), B(`${page + 1} / ${pages}`, 'a:noop'), B('›', cbq(`a:tl:${Math.min(pages - 1, page + 1)}`, q))] : null,
      [B('Найти', 'a:tsearch', 'eye'), BACK('a:cat')]),
  };
}
function vTrack(id) {
  const t = one('SELECT * FROM tracks WHERE id=?', id); if (!t) return { text: `${em('cross')} Трек не найден`, kb: KB([BACK('a:tl:0')]) };
  const gain = t.gain != null ? `${t.gain > 0 ? '+' : ''}${t.gain.toFixed(1)} дБ` : '—';
  return {
    text: `<b>${em('file')} ${esc(t.title)}</b>\n${esc(t.artist)}${t.album ? ' · ' + esc(t.album) : ''}\n\n${em('clock')} ${fmtDur(t.duration)} · ${fmtSize(t.size)} · ${esc((t.mime || '').replace('audio/', ''))}\n${em('growth')} Прослушиваний: <b>${fmtNum(t.plays)}</b>\n${em('media')} Обложка: ${t.cover ? 'есть' : 'нет'} · ${em('addText')} Текст: ${t.lyrics ? (/\[\d+:\d+/.test(t.lyrics) ? 'синхронизирован' : 'обычный') : 'из LRCLIB'}\n${em('format')} Нормализация: ${gain}${t.genre ? `\n${em('tag')} Жанр: ${esc(t.genre)}` : ''}\n\nID в приложении: <code>sl:${t.id}</code>`,
    kb: KB([B('Название', `a:te:${t.id}:title`, 'pencil'), B('Исполнитель', `a:te:${t.id}:artist`, 'write')], [B('Альбом', `a:te:${t.id}:album`, 'font'), B('Жанр', `a:te:${t.id}:genre`, 'tag')],
      [B('Обложка', `a:te:${t.id}:cover`, 'media'), B('Текст песни', `a:te:${t.id}:lyrics`, 'addText')], [B('Удалить', `a:td:${t.id}`, 'trash', 'danger'), BACK('a:tl:0')]),
  };
}
function vBroadcast() {
  const bots = one('SELECT COUNT(*) c FROM bot_users WHERE blocked=0').c; const ann = one('SELECT COUNT(*) c FROM announcements WHERE active=1').c;
  return {
    text: `<b>${em('megaphone')} Рассылка</b>\n\n${em('send')} <b>В Telegram</b> — сообщение всем, кто запускал бота (${fmtNum(bots)}).\n${em('bell')} <b>В приложении</b> — объявление-баннер на главной у всех пользователей. Активных: ${ann}.`,
    kb: KB([B('В Telegram', 'a:bct', 'send'), B('В приложении', 'a:bca', 'bell')], [B('Объявления', 'a:ann', 'file')], [BACK('a:main')]),
  };
}
function vAnnouncements() {
  const list = all('SELECT * FROM announcements WHERE active=1 ORDER BY id DESC LIMIT 10');
  return {
    text: `<b>${em('bell')} Объявления в приложении</b>\n\n` + (list.length ? list.map((a) => `<b>#${a.id}</b> · ${new Date(a.created_at).toLocaleDateString('ru-RU')}\n${esc(a.text).slice(0, 300)}`).join('\n\n') : '<i>Активных объявлений нет</i>'),
    kb: KB(...list.map((a) => [B(`Снять #${a.id}`, 'a:annx:' + a.id, 'trash')]), [B('Новое', 'a:bca', 'write'), BACK('a:bc')]),
  };
}
function vSettings() {
  const t = (k, on, off) => (flag(k) ? on : off);
  return {
    text: `<b>${em('settings')} Настройки сервера</b>\n\n${em('info')} Версия сервера: <code>${VERSION}</code>\n${em('code')} ffmpeg: ${ffmpegOk ? 'доступен' : 'не найден'}\n${em('link')} Публичный адрес: ${CFG.publicUrl ? esc(CFG.publicUrl) : '<i>не задан (PUBLIC_URL)</i>'}`,
    kb: KB(
      [B(`Регистрация: ${t('registration', 'открыта', 'закрыта')}`, 'a:tog:registration', flag('registration') ? 'unlock' : 'lock')],
      [B(`Тех. работы: ${t('maintenance', 'включены', 'выключены')}`, 'a:tog:maintenance', flag('maintenance') ? 'check' : 'cross', flag('maintenance') ? 'danger' : undefined)],
      [B(`Каталог Audius: ${t('audius', 'включён', 'выключен')}`, 'a:tog:audius', flag('audius') ? 'check' : 'cross')],
      [B(`Уведомления о регистрациях: ${t('notify_reg', 'вкл', 'выкл')}`, 'a:tog:notify_reg', flag('notify_reg') ? 'bell' : 'hidden')],
      [BACK('a:main')]),
  };
}
const vPrompt = (title, hint, back) => ({ text: `<b>${title}</b>\n\n${hint}`, kb: KB([B('Отмена', back, 'cross')]) });

/* — Экраны пользователя — */
function linkedUser(chatId) { return one('SELECT * FROM users WHERE tg_id=?', chatId); }
function vStart(chatId) {
  const u = linkedUser(chatId);
  return {
    text: `<b>${em('smile')} Soundless</b> — музыка без лишнего шума.\n\n${em('download')} Скачайте приложение для Windows или Android, войдите и слушайте с караоке-текстами, кроссфейдом и друзьями.` + (u ? `\n\n${em('userOk')} Аккаунт: <b>${esc(u.name)}</b> · @${esc(u.username)}` : `\n\n${em('link')} Привяжите Telegram в приложении: <i>Настройки → Интеграции</i> — так можно входить в один тап и восстанавливать пароль.`),
    kb: KB([B('Скачать приложение', CFG.downloadUrl, 'download')], u ? [B('Мой аккаунт', 'u:acc', 'profile')] : null, isAdmin(chatId) ? [B('Панель управления', 'a:main', 'settings')] : null),
  };
}
function vAccount(chatId) {
  const u = linkedUser(chatId); if (!u) return vStart(chatId);
  const plays = playsCount(u.id); const lv = levelOf(plays); const np = nowPlayingOf(u); const top = topArtists(u.id, 0, 3);
  return {
    text: `<b>${em('profile')} ${esc(u.name)}</b>${u.verified ? ' ' + em('check') : ''}\n@${esc(u.username)}\n\n${em('growth')} Уровень <b>${lv.level}</b> · ${fmtNum(plays)} ${plural(plays, 'трек', 'трека', 'треков')}\n${em('people')} Друзей: <b>${friendsCount(u.id)}</b> · подписчиков: <b>${followersCount(u.id)}</b>\n${em('calendar')} С нами с ${fmtDate(u.created_at)}` +
      (top.length ? `\n\n${em('party')} Любимые артисты: ${top.map((a) => esc(a.name)).join(', ')}` : '') + (np ? `\n\n${em('eye')} Сейчас: <b>${esc(np.track.artist)} — ${esc(np.track.title)}</b>` : ''),
    kb: KB([B('Уведомления', 'u:notif', J(u.notify, {}).telegram === false ? 'hidden' : 'bell'), B('Отвязать', 'u:unlink', 'cross', 'danger')], [BACK('u:start')]),
  };
}

async function importAudio(msg, statusId) {
  const chat = msg.chat.id; const a = msg.audio || msg.document;
  if (a.file_size && a.file_size > 20 * 1024 * 1024 && /api\.telegram\.org/.test(CFG.tgApi)) {
    return bot.show(chat, { text: `${em('cross')} <b>Файл больше 20 МБ</b>\n\nОблачный Bot API не позволяет ботам скачивать такие файлы. Сожмите трек или поднимите локальный Bot API сервер и укажите его в <code>TELEGRAM_API</code>.`, kb: KB([BACK('a:cat')]) }, statusId);
  }
  const f = await bot.api('getFile', { file_id: a.file_id });
  const ext = (path.extname(a.file_name || f.file_path || '') || (a.mime_type === 'audio/mp4' ? '.m4a' : '.mp3')).toLowerCase().slice(0, 6);
  const name = `${Date.now().toString(36)}_${token(5)}${ext}`; const dest = path.join(DIRS.tracks, name);
  await downloadTg(f.file_path, dest);
  const size = (await fsp.stat(dest)).size; const info = await probeAudio(dest).catch(() => ({}));
  let base = (a.file_name || '').replace(/\.[^.]+$/, '').replace(/_/g, ' ').trim(); let fnArtist = '';
  const dash = /^(.+?)\s+[-–—]\s+(.+)$/.exec(base); if (dash) { fnArtist = dash[1]; base = dash[2]; }
  const title = (a.title || info.tags?.title || base || 'Без названия').slice(0, 200); const artist = (a.performer || info.tags?.artist || fnArtist || 'Неизвестный исполнитель').slice(0, 200);
  const album = (info.tags?.album || '').slice(0, 200); const genre = (info.tags?.genre || '').slice(0, 60);
  const duration = Math.round(a.duration || info.duration || 0);
  const gain = info.lufs != null && isFinite(info.lufs) ? clamp(-14 - info.lufs, -12, 8) : null;
  let cover = null; const coverName = name.replace(/\.[^.]+$/, '') + '.jpg';
  if (await extractCover(dest, path.join(DIRS.covers, coverName))) cover = coverName;
  else if (a.thumbnail) { try { const tf = await bot.api('getFile', { file_id: a.thumbnail.file_id }); await downloadTg(tf.file_path, path.join(DIRS.covers, coverName)); cover = coverName; } catch {} }
  const mime = a.mime_type && a.mime_type.startsWith('audio/') ? a.mime_type : MIME[ext] || 'audio/mpeg';
  const r = run('INSERT INTO tracks(title,artist,album,genre,duration,file,mime,size,cover,gain,tg_file_id,uploaded_by,created_at,q) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)', title, artist, album, genre, duration, name, mime, size, cover, gain, a.file_id, chat, now(), trackQ({ title, artist, album, genre }));
  const id = Number(r.lastInsertRowid); log('[catalog] +', artist, '—', title);
  const v = vTrack(id); v.text = `${em('party')} <b>Добавлено в каталог</b>\n\n` + v.text;
  return bot.show(chat, v, statusId);
}
async function downloadTg(filePath, dest) {
  if (/^\//.test(filePath) && fs.existsSync(filePath)) return fsp.copyFile(filePath, dest); // локальный Bot API (--local)
  const r = await fetch(`${CFG.tgApi}/file/bot${CFG.botToken}/${filePath}`, { signal: AbortSignal.timeout(300000) });
  if (!r.ok) throw new Error('download failed ' + r.status);
  await fsp.writeFile(dest, Buffer.from(await r.arrayBuffer()));
}

async function broadcast(adminChat, fromChat, messageId) {
  const list = all('SELECT chat_id FROM bot_users WHERE blocked=0').map((r) => r.chat_id);
  let ok = 0, fail2 = 0; const status = await bot.send(adminChat, `${em('loading')} Рассылка: 0 / ${list.length}`);
  for (let i = 0; i < list.length; i++) {
    try { await bot.api('copyMessage', { chat_id: list[i], from_chat_id: fromChat, message_id: messageId }); ok++; }
    catch (e) { fail2++; if (e.code === 403) run('UPDATE bot_users SET blocked=1 WHERE chat_id=?', list[i]); }
    if (i % 25 === 24) bot.api('editMessageText', { chat_id: adminChat, message_id: status.message_id, text: `${em('loading')} Рассылка: ${i + 1} / ${list.length}`, parse_mode: 'HTML' }).catch(() => {});
    await sleep(45);
  }
  await bot.api('editMessageText', { chat_id: adminChat, message_id: status.message_id, text: `${em('check')} <b>Рассылка завершена</b>\n\nДоставлено: <b>${ok}</b> · ошибок: ${fail2}`, parse_mode: 'HTML', reply_markup: KB([B('В меню', 'a:main', 'home')]) }).catch(() => {});
}

async function onCallback(cq) {
  const chat = cq.message?.chat?.id; const mid = cq.message?.message_id; const d = cq.data || ''; const from = cq.from.id;
  let toast = null; let alert = false;
  try {
    if (d.startsWith('a:')) {
      if (!isAdmin(from)) { await bot.api('answerCallbackQuery', { callback_query_id: cq.id, text: 'Нет доступа', show_alert: true }); return; }
      const [, act, x, ...rest] = d.split(':'); const y = rest.join(':');
      bot.clearState(chat);
      switch (act) {
        case 'noop': break;
        case 'main': await bot.show(chat, vMain(), mid); break;
        case 'stats': await bot.show(chat, vStats(), mid); toast = 'Обновлено'; break;
        case 'users': await bot.show(chat, vUsers(+x || 0, y), mid); break;
        case 'usearch': bot.setState(chat, 'usearch', {}, mid); await bot.show(chat, vPrompt(`${em('eye')} Поиск пользователей`, 'Отправьте логин или имя.', 'a:users:0'), mid); break;
        case 'u': await bot.show(chat, vUser(+x), mid); break;
        case 'ub': { const u = one('SELECT banned FROM users WHERE id=?', +x); if (u) { run('UPDATE users SET banned=? WHERE id=?', u.banned ? 0 : 1, +x); if (!u.banned) run('DELETE FROM sessions WHERE user_id=?', +x); toast = u.banned ? 'Разблокирован' : 'Заблокирован'; } await bot.show(chat, vUser(+x), mid); break; }
        case 'uv': run('UPDATE users SET verified=1-verified WHERE id=?', +x); await bot.show(chat, vUser(+x), mid); toast = 'Готово'; break;
        case 'us': run('DELETE FROM sessions WHERE user_id=?', +x); toast = 'Все сессии завершены'; await bot.show(chat, vUser(+x), mid); break;
        case 'ur': {
          const u = one('SELECT * FROM users WHERE id=?', +x); if (!u) break;
          const pass = token(6).replace(/[-_]/g, 'x'); run('UPDATE users SET pass=? WHERE id=?', await hashPass(pass), u.id); run('DELETE FROM sessions WHERE user_id=?', u.id);
          if (u.tg_id) bot.send(u.tg_id, `${em('lock')} <b>Пароль сброшен администратором</b>\n\nВременный пароль: <code>${pass}</code>\nСмените его в приложении: <i>Настройки → Безопасность</i>.`).catch(() => {});
          await bot.show(chat, { text: `${em('check')} <b>Пароль сброшен</b>\n\n@${esc(u.username)} · временный пароль: <code>${pass}</code>${u.tg_id ? '\n\nПользователь получил его в Telegram.' : ''}`, kb: KB([BACK('a:u:' + u.id)]) }, mid); break;
        }
        case 'ud': { const u = one('SELECT * FROM users WHERE id=?', +x); await bot.show(chat, { text: `${em('trash')} <b>Удалить аккаунт @${esc(u?.username)}?</b>\n\nИстория, лайки и плейлисты будут удалены навсегда.`, kb: KB([B('Удалить', 'a:udy:' + x, 'trash', 'danger'), B('Отмена', 'a:u:' + x, 'cross')]) }, mid); break; }
        case 'udy': run('DELETE FROM users WHERE id=?', +x); toast = 'Аккаунт удалён'; await bot.show(chat, vUsers(0), mid); break;
        case 'cat': await bot.show(chat, vCatalog(), mid); break;
        case 'tl': await bot.show(chat, vTracks(+x || 0, y), mid); break;
        case 'tsearch': bot.setState(chat, 'tsearch', {}, mid); await bot.show(chat, vPrompt(`${em('eye')} Поиск по каталогу`, 'Отправьте название или исполнителя.', 'a:cat'), mid); break;
        case 't': await bot.show(chat, vTrack(+x), mid); break;
        case 'te': {
          const hints = { title: 'Отправьте новое название трека.', artist: 'Отправьте исполнителя (несколько — через запятую).', album: 'Отправьте название альбома или «-», чтобы очистить.', genre: 'Отправьте жанр или «-», чтобы очистить.', cover: 'Отправьте изображение (как фото) для обложки.', lyrics: 'Отправьте текст песни сообщением или файлом <code>.lrc</code>/<code>.txt</code>. Формат LRC <code>[mm:ss.xx]</code> включит синхронизацию. «-» — удалить свой текст.' };
          bot.setState(chat, 'tedit', { id: +x, field: y }, mid); await bot.show(chat, vPrompt(`${em('pencil')} Редактирование`, hints[y] || '', 'a:t:' + x), mid); break;
        }
        case 'td': await bot.show(chat, { text: `${em('trash')} <b>Удалить трек из каталога?</b>\n\nФайл будет удалён с сервера.`, kb: KB([B('Удалить', 'a:tdy:' + x, 'trash', 'danger'), B('Отмена', 'a:t:' + x, 'cross')]) }, mid); break;
        case 'tdy': {
          const t = one('SELECT * FROM tracks WHERE id=?', +x);
          if (t) { fsp.unlink(path.join(DIRS.tracks, t.file)).catch(() => {}); if (t.cover) fsp.unlink(path.join(DIRS.covers, t.cover)).catch(() => {}); run('DELETE FROM tracks WHERE id=?', t.id); }
          toast = 'Трек удалён'; await bot.show(chat, vTracks(0), mid); break;
        }
        case 'bc': await bot.show(chat, vBroadcast(), mid); break;
        case 'bct': bot.setState(chat, 'bct', {}, mid); await bot.show(chat, vPrompt(`${em('send')} Рассылка в Telegram`, 'Отправьте сообщение для рассылки — текст, фото, видео или файл. Форматирование сохранится.', 'a:bc'), mid); break;
        case 'bca': bot.setState(chat, 'bca', {}, mid); await bot.show(chat, vPrompt(`${em('bell')} Объявление в приложении`, 'Отправьте текст объявления (до 500 символов). Он появится баннером на главной.', 'a:bc'), mid); break;
        case 'ann': await bot.show(chat, vAnnouncements(), mid); break;
        case 'annx': run('UPDATE announcements SET active=0 WHERE id=?', +x); toast = 'Объявление снято'; await bot.show(chat, vAnnouncements(), mid); break;
        case 'set': await bot.show(chat, vSettings(), mid); break;
        case 'tog': if (KV_DEFAULTS[x] !== undefined) setKv(x, flag(x) ? '0' : '1'); await bot.show(chat, vSettings(), mid); toast = 'Сохранено'; break;
        case 'send': {
          const [fc, fm] = [Number(x), Number(y)];
          await bot.show(chat, { text: `${em('loading')} <b>Рассылка запущена</b>`, kb: KB() }, mid);
          broadcast(chat, fc, fm).catch((e) => log('[bc]', e.message)); break;
        }
      }
    } else if (d.startsWith('u:')) {
      const act = d.slice(2);
      if (act === 'start') await bot.show(chat, vStart(chat), mid);
      else if (act === 'acc') await bot.show(chat, vAccount(chat), mid);
      else if (act === 'unlink') await bot.show(chat, { text: `${em('cross')} <b>Отвязать Telegram?</b>\n\nВход в один тап и восстановление пароля через бота станут недоступны.`, kb: KB([B('Отвязать', 'u:unlinky', 'cross', 'danger'), B('Отмена', 'u:acc', 'check')]) }, mid);
      else if (act === 'unlinky') { run('UPDATE users SET tg_id=NULL, tg_username=NULL WHERE tg_id=?', chat); toast = 'Telegram отвязан'; await bot.show(chat, vStart(chat), mid); }
      else if (act === 'notif') { const u = linkedUser(chat); if (u) { const n = J(u.notify, {}); n.telegram = n.telegram === false; run('UPDATE users SET notify=? WHERE id=?', JSON.stringify(n), u.id); toast = n.telegram ? 'Уведомления включены' : 'Уведомления выключены'; } await bot.show(chat, vAccount(chat), mid); }
    } else if (d.startsWith('lk:')) {
      const [, code, ans] = d.split(':'); const c = one('SELECT * FROM codes WHERE code=? AND kind=?', code, 'link');
      if (!c || c.expires_at < now()) { await bot.show(chat, { text: `${em('clock')} Ссылка устарела. Сгенерируйте новую в приложении.`, kb: KB([BACK('u:start')]) }, mid); }
      else if (ans === 'y') {
        run('UPDATE users SET tg_id=NULL, tg_username=NULL WHERE tg_id=?', from);
        run('UPDATE users SET tg_id=?, tg_username=? WHERE id=?', from, cq.from.username || null, c.user_id); run('DELETE FROM codes WHERE code=?', code);
        const u = one('SELECT * FROM users WHERE id=?', c.user_id);
        await bot.show(chat, { text: `${em('party')} <b>Telegram привязан</b>\n\nАккаунт: <b>${esc(u.name)}</b> · @${esc(u.username)}\nТеперь можно входить в один тап и получать уведомления.`, kb: KB([B('Мой аккаунт', 'u:acc', 'profile')]) }, mid);
      } else { run('DELETE FROM codes WHERE code=?', code); await bot.show(chat, vStart(chat), mid); }
    } else if (d.startsWith('lg:')) {
      const [, code, ans] = d.split(':'); const c = one('SELECT * FROM codes WHERE code=? AND kind=?', code, 'login');
      if (!c || c.expires_at < now() || J(c.data, {}).status !== 'pending') { await bot.show(chat, { text: `${em('clock')} Запрос на вход устарел. Попробуйте ещё раз в приложении.`, kb: KB([BACK('u:start')]) }, mid); }
      else if (ans === 'y' || ans === 'new') {
        let u = linkedUser(from);
        if (!u && ans === 'new') {
          if (!flag('registration')) { await bot.show(chat, { text: `${em('lock')} Регистрация временно закрыта.`, kb: KB([BACK('u:start')]) }, mid); return; }
          let un = lower(cq.from.username || '').replace(/[^a-z0-9_.]/g, '').slice(0, 24);
          if (!validUsername(un) || RESERVED.has(un) || one('SELECT id FROM users WHERE username=?', un)) un = 'user' + String(from).slice(-8);
          while (one('SELECT id FROM users WHERE username=?', un)) un = 'user' + crypto.randomInt(1e6, 1e8);
          const name = [cq.from.first_name, cq.from.last_name].filter(Boolean).join(' ').slice(0, 40) || un;
          const r = run('INSERT INTO users(username,name,pass,tg_id,tg_username,created_at,last_seen,q) VALUES(?,?,?,?,?,?,?,?)', un, name, '!', from, cq.from.username || null, now(), now(), lower(`${un} ${name}`));
          u = one('SELECT * FROM users WHERE id=?', Number(r.lastInsertRowid)); bot.onRegister(u.id);
        }
        if (!u) { await bot.show(chat, vStart(chat), mid); return; }
        if (u.banned) { await bot.show(chat, { text: `${em('lock')} Аккаунт заблокирован.`, kb: KB() }, mid); return; }
        run('UPDATE codes SET user_id=?, data=? WHERE code=?', u.id, JSON.stringify({ status: 'ok' }), code);
        await bot.show(chat, { text: `${em('check')} <b>Вход подтверждён</b>\n\nВозвращайтесь в приложение — вы вошли как <b>${esc(u.name)}</b> (@${esc(u.username)}).`, kb: KB([B('Мой аккаунт', 'u:acc', 'profile')]) }, mid);
      } else { run('UPDATE codes SET data=? WHERE code=?', JSON.stringify({ status: 'denied' }), code); await bot.show(chat, { text: `${em('cross')} Вход отклонён.`, kb: KB([BACK('u:start')]) }, mid); }
    }
  } catch (e) { log('[bot cb]', e.message); toast = 'Ошибка: ' + e.message.slice(0, 150); alert = true; }
  await bot.api('answerCallbackQuery', { callback_query_id: cq.id, text: toast || undefined, show_alert: alert }).catch(() => {});
}

async function onMessage(msg) {
  const chat = msg.chat.id; if (msg.chat.type !== 'private') return;
  run('INSERT INTO bot_users(chat_id,username,first_name,created_at) VALUES(?,?,?,?) ON CONFLICT(chat_id) DO UPDATE SET username=excluded.username, first_name=excluded.first_name, blocked=0', chat, msg.from?.username || null, msg.from?.first_name || null, now());
  const linked = linkedUser(chat); if (linked && (msg.from?.username || null) !== linked.tg_username) run('UPDATE users SET tg_username=? WHERE id=?', msg.from?.username || null, linked.id);
  const text = msg.text || ''; const admin = isAdmin(chat);
  if (text.startsWith('/start')) {
    bot.clearState(chat); const payload = text.split(/\s+/)[1] || '';
    if (payload.startsWith('link_')) {
      const code = payload.slice(5); const c = one('SELECT * FROM codes WHERE code=? AND kind=?', code, 'link'); const u = c && one('SELECT * FROM users WHERE id=?', c.user_id);
      if (!c || !u || c.expires_at < now()) return bot.show(chat, { text: `${em('clock')} Ссылка для привязки устарела. Сгенерируйте новую в приложении.`, kb: KB([B('Скачать приложение', CFG.downloadUrl, 'download')]) });
      return bot.show(chat, { text: `${em('link')} <b>Привязать Telegram к аккаунту?</b>\n\n<b>${esc(u.name)}</b> · @${esc(u.username)}${linked && linked.id !== u.id ? `\n\n${em('info')} Сейчас Telegram привязан к @${esc(linked.username)} — привязка перейдёт на новый аккаунт.` : ''}`, kb: KB([B('Привязать', `lk:${code}:y`, 'check', 'success'), B('Отмена', `lk:${code}:n`, 'cross')]) });
    }
    if (payload.startsWith('login_')) {
      const code = payload.slice(6); const c = one('SELECT * FROM codes WHERE code=? AND kind=?', code, 'login');
      if (!c || c.expires_at < now()) return bot.show(chat, { text: `${em('clock')} Запрос на вход устарел. Нажмите «Войти через Telegram» в приложении ещё раз.`, kb: KB() });
      const dev = J(c.data, {}).device;
      if (linked) return bot.show(chat, { text: `${em('lock')} <b>Вход в Soundless</b>\n\nАккаунт: <b>${esc(linked.name)}</b> · @${esc(linked.username)}${dev ? `\nУстройство: ${esc(dev)}` : ''}\n\nПодтвердите, если это вы.`, kb: KB([B('Войти', `lg:${code}:y`, 'check', 'success'), B('Отклонить', `lg:${code}:n`, 'cross', 'danger')]) });
      return bot.show(chat, { text: `${em('userOk')} <b>Добро пожаловать в Soundless</b>\n\nК этому Telegram ещё не привязан аккаунт. Создадим его за секунду — логин и имя возьмём из профиля, пароль можно задать позже в настройках.`, kb: KB([B('Создать аккаунт', `lg:${code}:new`, 'userOk', 'success')], [B('Отмена', `lg:${code}:n`, 'cross')]) });
    }
    if (admin) await bot.send(chat, `${em('settings')} Панель администратора включена`, { keyboard: [[{ text: 'Панель', icon_custom_emoji_id: EMOJI.settings[0] }, { text: 'Статистика', icon_custom_emoji_id: EMOJI.stats[0] }, { text: 'Каталог', icon_custom_emoji_id: EMOJI.box[0] }]], resize_keyboard: true, is_persistent: true }).catch(() => {});
    return bot.show(chat, admin ? vMain() : vStart(chat));
  }
  if (admin && (text === '/admin' || text === 'Панель')) { bot.clearState(chat); return bot.show(chat, vMain()); }
  if (admin && text === 'Статистика') { bot.clearState(chat); return bot.show(chat, vStats()); }
  if (admin && text === 'Каталог') { bot.clearState(chat); return bot.show(chat, vCatalog()); }

  const st = bot.getState(chat);
  const isAudioDoc = msg.document && (/^audio\//.test(msg.document.mime_type || '') || /\.(mp3|m4a|flac|ogg|opus|wav|aac)$/i.test(msg.document.file_name || ''));
  if (admin && (msg.audio || isAudioDoc) && !(st?.state === 'bct')) {
    bot.clearState(chat);
    const s = await bot.send(chat, `${em('loading')} Загружаю «${esc((msg.audio?.title || msg.audio?.file_name || msg.document?.file_name || 'трек'))}»…`);
    return importAudio(msg, s.message_id).catch((e) => bot.show(chat, { text: `${em('cross')} Не удалось добавить трек: ${esc(e.message)}`, kb: KB([BACK('a:cat')]) }, s.message_id));
  }
  if (admin && st) {
    const del = () => bot.api('deleteMessage', { chat_id: chat, message_id: msg.message_id }).catch(() => {});
    switch (st.state) {
      case 'usearch': if (!text) break; bot.clearState(chat); del(); return bot.show(chat, vUsers(0, text.slice(0, 40)), st.msgId);
      case 'tsearch': if (!text) break; bot.clearState(chat); del(); return bot.show(chat, vTracks(0, text.slice(0, 40)), st.msgId);
      case 'bca': {
        if (!text) break; bot.clearState(chat); del();
        run('INSERT INTO announcements(text,created_at,active) VALUES(?,?,1)', text.slice(0, 500), now());
        return bot.show(chat, { text: `${em('check')} <b>Объявление опубликовано</b>\n\n${esc(text.slice(0, 500))}`, kb: KB([B('Объявления', 'a:ann', 'bell'), B('В меню', 'a:main', 'home')]) }, st.msgId);
      }
      case 'bct': {
        bot.clearState(chat); const n = one('SELECT COUNT(*) c FROM bot_users WHERE blocked=0').c;
        return bot.show(chat, { text: `${em('megaphone')} <b>Отправить это сообщение ${fmtNum(n)} ${plural(n, 'пользователю', 'пользователям', 'пользователям')}?</b>\n\nСообщение выше будет скопировано как есть.`, kb: KB([B('Отправить', `a:send:${chat}:${msg.message_id}`, 'send', 'success'), B('Отмена', 'a:bc', 'cross')]) });
      }
      case 'tedit': {
        const { id, field } = st.data; const t = one('SELECT * FROM tracks WHERE id=?', id); if (!t) { bot.clearState(chat); break; }
        if (field === 'cover') {
          const ph = msg.photo?.[msg.photo.length - 1] || (msg.document && /^image\//.test(msg.document.mime_type || '') ? msg.document : null);
          if (!ph) return bot.send(chat, `${em('info')} Нужна картинка — отправьте фото.`);
          const f = await bot.api('getFile', { file_id: ph.file_id }); const name = t.cover || t.file.replace(/\.[^.]+$/, '') + '.jpg';
          await downloadTg(f.file_path, path.join(DIRS.covers, name)); run('UPDATE tracks SET cover=?, cover_v=cover_v+1 WHERE id=?', name, id);
        } else if (field === 'lyrics') {
          let ly = text;
          if (msg.document && /\.(lrc|txt)$/i.test(msg.document.file_name || '') && msg.document.file_size < 512000) {
            const f = await bot.api('getFile', { file_id: msg.document.file_id }); const r = await fetch(`${CFG.tgApi}/file/bot${CFG.botToken}/${f.file_path}`); ly = await r.text();
          }
          if (!ly) return bot.send(chat, `${em('info')} Отправьте текст или файл .lrc`);
          run('UPDATE tracks SET lyrics=? WHERE id=?', ly.trim() === '-' ? null : ly.slice(0, 60000), id);
        } else if (['title', 'artist', 'album', 'genre'].includes(field)) {
          if (!text) return;
          const val = text.trim() === '-' && (field === 'album' || field === 'genre') ? '' : text.trim().slice(0, 200);
          run(`UPDATE tracks SET ${field}=? WHERE id=?`, val, id);
          const t2 = one('SELECT * FROM tracks WHERE id=?', id); run('UPDATE tracks SET q=? WHERE id=?', trackQ(t2), id);
        }
        bot.clearState(chat); del();
        return bot.show(chat, vTrack(id), st.msgId);
      }
    }
  }
  if (admin) return bot.show(chat, vMain());
  return bot.show(chat, vStart(chat));
}

async function botLoop() {
  if (!CFG.botToken) { log('[bot] BOT_TOKEN не задан — бот отключён'); return; }
  try {
    const me = await bot.api('getMe'); bot.username = me.username; bot.ready = true; log(`[bot] @${me.username} запущен`);
    await bot.api('setMyCommands', { commands: [{ command: 'start', description: 'Главное меню' }] }).catch(() => {});
    for (const a of CFG.admins) await bot.api('setMyCommands', { commands: [{ command: 'start', description: 'Главное меню' }, { command: 'admin', description: 'Панель управления' }], scope: { type: 'chat', chat_id: a } }).catch(() => {});
  } catch (e) { log('[bot] ошибка запуска:', e.message, '— повтор через 30 с'); setTimeout(botLoop, 30000); return; }
  for (;;) {
    try {
      const updates = await bot.api('getUpdates', { offset: bot.offset, timeout: 50, allowed_updates: ['message', 'callback_query'] });
      for (const u of updates || []) {
        bot.offset = u.update_id + 1;
        if (u.message) onMessage(u.message).catch((e) => log('[bot msg]', e.message));
        else if (u.callback_query) onCallback(u.callback_query).catch((e) => log('[bot cb]', e.message));
      }
    } catch (e) { if (!/aborted|timeout/i.test(e.message)) log('[bot poll]', e.message); await sleep(3000); }
  }
}

server.listen(CFG.port, CFG.host, () => {
  log(`Soundless server v${VERSION} · http://${CFG.host}:${CFG.port} · data: ${CFG.dataDir}`);
  if (!CFG.admins.length) log('[bot] ADMIN_IDS не задан — админ-панель никому не доступна');
  botLoop();
});
for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, () => { log('Остановка…'); server.close(); try { db.close(); } catch {} process.exit(0); });
module.exports = { parseLRC, parseLyricsfile };
