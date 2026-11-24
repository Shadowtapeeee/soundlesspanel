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
const { Readable } = require('node:stream');
const { pipeline } = require('node:stream/promises');
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
  ytdlp: process.env.YTDLP_PATH || 'yt-dlp',
  translate: (process.env.TRANSLATE_PROVIDER || 'mymemory').toLowerCase(),
  libreUrl: (process.env.LIBRETRANSLATE_URL || '').replace(/\/+$/, ''),
  libreKey: process.env.LIBRETRANSLATE_KEY || '',
  deeplKey: process.env.DEEPL_KEY || '',
  ffmpeg: process.env.FFMPEG_PATH || 'ffmpeg',
  premiumEmoji: process.env.PREMIUM_EMOJI !== '0',
  trustProxy: process.env.TRUST_PROXY === '1',
  googleId: (process.env.GOOGLE_CLIENT_ID || '').trim(),
  googleSecret: (process.env.GOOGLE_CLIENT_SECRET || '').trim(),
  scId: (process.env.SOUNDCLOUD_CLIENT_ID || '').trim(),
  scSecret: (process.env.SOUNDCLOUD_CLIENT_SECRET || '').trim(),
  spId: (process.env.SPOTIFY_CLIENT_ID || '').trim(),
  spSecret: (process.env.SPOTIFY_CLIENT_SECRET || '').trim(),
  cdnUrl: (process.env.CDN_URL || '').replace(/\/+$/, ''),
  captchaDifficulty: Number(process.env.CAPTCHA_DIFFICULTY) || 40000,
};
const UA = `Soundless/${VERSION} (+${CFG.downloadUrl})`;
const DIRS = ['tracks', 'covers', 'avatars', 'banners', 'plcovers', 'tmp'].reduce((o, d) => (o[d] = path.join(CFG.dataDir, d), o), {});
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
// Миграции без потери данных
for (const sql of [
  'ALTER TABLE users ADD COLUMN google_sub TEXT', 'ALTER TABLE users ADD COLUMN google_email TEXT',
  "ALTER TABLE users ADD COLUMN taste_artists TEXT DEFAULT '[]'", "ALTER TABLE users ADD COLUMN profile_ext TEXT DEFAULT '{}'",
  'ALTER TABLE users ADD COLUMN sync_state TEXT', 'ALTER TABLE playlists ADD COLUMN cover TEXT', 'ALTER TABLE tracks ADD COLUMN waveform TEXT',
]) { try { db.exec(sql); } catch {} }
db.exec(`
CREATE UNIQUE INDEX IF NOT EXISTS users_google ON users(google_sub) WHERE google_sub IS NOT NULL;
CREATE TABLE IF NOT EXISTS subscriptions(user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE, plan TEXT NOT NULL DEFAULT 'aura',
  started_at INTEGER, expires_at INTEGER, granted_by INTEGER, note TEXT);
CREATE TABLE IF NOT EXISTS offline_items(user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, device TEXT NOT NULL, track_id TEXT NOT NULL,
  bytes INTEGER DEFAULT 0, created_at INTEGER, PRIMARY KEY(user_id, device, track_id));
CREATE TABLE IF NOT EXISTS generations(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, kind TEXT, created_at INTEGER);
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, kind TEXT, created_at INTEGER);
CREATE INDEX IF NOT EXISTS events_user ON events(user_id, kind);
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
let ytdlpOk = false;
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
const toolsReady = Promise.all([sh(CFG.ffmpeg, ['-version'], 8000), sh(CFG.ytdlp, ['--version'], 8000)]).then(([ffmpeg, ytdlp]) => {
  ffmpegOk = ffmpeg.code === 0;
  ytdlpOk = ytdlp.code === 0;
  log(ffmpegOk ? '[ffmpeg] найден' : '[ffmpeg] не найден — обложки/нормализация/волны из файлов отключены');
  log(ytdlpOk ? '[yt-dlp] найден' : '[yt-dlp] не найден — офлайн-загрузка использует поток SoundCloud');
  if (ffmpegOk) (async () => {
    for (const t of all('SELECT id, file FROM tracks WHERE waveform IS NULL LIMIT 500')) { // досчитываем волны для старых треков
      const w = await computeWaveform(path.join(DIRS.tracks, t.file)).catch(() => null);
      if (w) run('UPDATE tracks SET waveform=? WHERE id=?', JSON.stringify(w), t.id);
    }
  })().catch((e) => log('[waveform backfill]', e.message));
});
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
const mediaUrl = (kind, file) => (file ? `${CFG.cdnUrl}/api/media/${kind}/${file}` : null);
const isOnline = (u) => !!u.last_seen && now() - u.last_seen < 120000;
const playsCount = (uid) => one('SELECT COUNT(*) c FROM plays WHERE user_id=?', uid).c;
function publicUser(u, mini = false) {
  if (!u) return null;
  const pv = J(u.privacy, {});
  const ents = entitlementsOf(u.id);
  const base = { id: u.id, username: u.username, name: u.name, avatar: mediaUrl('avatars', u.avatar), color: u.color || null, verified: !!u.verified, online: pv.showOnline === false ? null : isOnline(u), aura: ents.has('aura.badge') };
  if (mini) return base;
  const links = J(u.links, []).filter((l) => l && l.url);
  if (u.tg_username && pv.showTelegram !== false) links.unshift({ type: 'telegram', title: u.tg_username, url: `https://t.me/${u.tg_username}` });
  return { ...base, bio: u.bio || '', banner: mediaUrl('banners', u.banner), links, createdAt: u.created_at, lastSeen: pv.showOnline === false ? null : u.last_seen, level: levelOf(playsCount(u.id)), ext: ents.has('profile.extended') ? J(u.profile_ext, {}) : {} };
}
function privateUser(u) {
  return {
    ...publicUser(u), privacy: J(u.privacy, {}), prefs: J(u.prefs, {}), taste: J(u.taste, []), tasteArtists: J(u.taste_artists, []), notify: J(u.notify, {}),
    hasPassword: u.pass !== '!', linksRaw: J(u.links, []), google: u.google_sub ? { linked: true, email: u.google_email || null } : { linked: false },
    entitlements: [...entitlementsOf(u.id)], limits: limitsOf(u.id), subscription: auraOf(u.id), extRaw: J(u.profile_ext, {}),
  };
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
function auAlbum(a) {
  if (!a || a.is_delete || a.is_private) return null;
  const artwork = a.artwork || {};
  return { id: 'au:' + a.id, source: 'audius', title: a.playlist_name || a.title || 'Без названия', type: String(a.release_type || a.set_type || 'album').toLowerCase(), trackCount: a.total_track_count || a.track_count || a.tracks?.length || 0, cover: artwork['1000x1000'] || artwork['480x480'] || artwork['150x150'] || null, releaseDate: a.release_date || a.created_at || '' };
}
function sanitizeTrack(t) {
  if (!t || typeof t !== 'object') return null;
  const id = String(t.id || '').slice(0, 160);
  if (!/^(sl|au|local|sc|dz|sp):/.test(id)) return null;
  const s = (v, n = 300) => (v == null ? '' : String(v).slice(0, n));
  const url = (v) => (typeof v === 'string' && (/^https:\/\//.test(v) || /^\/api\//.test(v)) ? v.slice(0, 1000) : null);
  return { id, source: s(t.source, 20), title: s(t.title) || 'Без названия', artist: s(t.artist) || 'Неизвестен', artistId: t.artistId ? s(t.artistId, 200) : null, album: s(t.album), genre: s(t.genre, 60), duration: Math.max(0, Math.round(+t.duration || 0)), cover: url(t.cover), coverLarge: url(t.coverLarge), coverSmall: url(t.coverSmall), explicit: !!t.explicit, preview: t.preview ? url(t.preview) : undefined };
}

/* ───────────────────────────── Aura: подписка и разрешения ───────────────────────────── */
const GB = 1024 ** 3;
const AURA_ENT = ['aura.badge', 'profile.extended', 'media.animated', 'mixes.advanced', 'playlists.text.unlimited', 'karaoke.plus', 'offline.extended', 'sync', 'stats.detailed', 'rooms.10', 'achievements.extended', 'early_access'];
function auraOf(uid) {
  const s = one('SELECT * FROM subscriptions WHERE user_id=?', uid);
  if (!s) return { active: false };
  const active = !s.expires_at || s.expires_at > now();
  return { active, plan: s.plan, startedAt: s.started_at, expiresAt: s.expires_at || null };
}
function entitlementsOf(uid) {
  const ents = new Set(['base']);
  if (auraOf(uid).active) for (const e of AURA_ENT) ents.add(e);
  return ents;
}
const can = (uid, e) => entitlementsOf(uid).has(e);
function limitsOf(uid) {
  const e = entitlementsOf(uid);
  return { roomSize: e.has('rooms.10') ? 10 : 5, offlineBytes: e.has('offline.extended') ? 10 * GB : 1 * GB, textPlaylistsPerDay: e.has('playlists.text.unlimited') ? null : 3, statsDays: e.has('stats.detailed') ? 365 : 30 };
}
const requireEnt = (uid, e, msg) => { if (!can(uid, e)) fail(402, 'aura_required', msg || 'Доступно с подпиской Soundless Aura'); };
const logEvent = (uid, kind) => run('INSERT INTO events(user_id,kind,created_at) VALUES(?,?,?)', uid, kind, now());

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
const CORS = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': 'authorization, content-type, x-device, range, if-none-match, if-modified-since', 'Access-Control-Allow-Methods': 'GET, POST, PUT, PATCH, DELETE, OPTIONS', 'Access-Control-Expose-Headers': 'content-length, content-range, accept-ranges, etag, last-modified', 'Access-Control-Max-Age': '86400' };
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
  const etag = `W/"${size.toString(36)}-${Math.floor(st.mtimeMs).toString(36)}"`;
  const lastMod = new Date(st.mtimeMs).toUTCString();
  const h = { 'Content-Type': type, 'Accept-Ranges': 'bytes', 'Cache-Control': cache, ETag: etag, 'Last-Modified': lastMod, ...CORS };
  const inm = req.headers['if-none-match']; const ims = req.headers['if-modified-since'];
  if (!req.headers.range && ((inm && inm === etag) || (!inm && ims && Date.parse(ims) >= Math.floor(st.mtimeMs / 1000) * 1000))) { res.writeHead(304, { ETag: etag, 'Last-Modified': lastMod, 'Cache-Control': cache, ...CORS }); return res.end(); }
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
}

const routes = [];
const route = (method, pattern, handler) => routes.push({ method, re: new RegExp('^' + pattern.replace(/:(\w+)/g, '(?<$1>[^/]+)') + '/?$'), handler });

route('GET', '/api/health', () => ({ ok: true, name: 'Soundless', version: VERSION, time: now() }));
route('GET', '/api/config', () => ({
  name: 'Soundless', version: VERSION, registration: flag('registration'), maintenance: flag('maintenance'),
  google: !!(CFG.googleId && CFG.googleSecret && CFG.publicUrl),
  sources: { catalog: true, deezer: true, soundcloud: !!(CFG.scId && CFG.scSecret), spotify: !!(CFG.spId && CFG.spSecret), audius: flag('audius') },
  announcements: all('SELECT id, text, created_at createdAt FROM announcements WHERE active=1 ORDER BY id DESC LIMIT 5'),
  aura: { contact: /^https:\/\//.test(process.env.AURA_CONTACT || '') ? process.env.AURA_CONTACT : null },
}));

/* — Авторизация — */
route('POST', '/api/auth/register', async (req) => {
  rateLimit(req, 'reg', 8, 3600e3);
  if (!flag('registration')) fail(403, 'registration_closed', 'Регистрация временно закрыта');
  const b = await readJSON(req);
  verifyCaptcha(b.captcha);
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
  verifyCaptcha(b.captcha);
  const u = one('SELECT * FROM users WHERE username=?', String(b.username || '').trim().replace(/^@/, ''));
  if (!u || !(await checkPass(String(b.password || ''), u.pass))) fail(401, 'bad_credentials', 'Неверный логин или пароль');
  if (u.banned) fail(403, 'banned', 'Аккаунт заблокирован');
  return { token: createSession(req, u.id), user: privateUser(u) };
});
route('POST', '/api/auth/logout', (req) => { const a = auth(req); run('DELETE FROM sessions WHERE token=?', a.token); return { ok: true }; });
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
  if (b.ext !== undefined) {
    requireEnt(user.id, 'profile.extended', 'Расширенная персонализация профиля доступна с Aura');
    const e = b.ext || {}; const THEMES = ['none', 'aurora', 'ember', 'frost', 'noir', 'bloom'];
    set.profile_ext = JSON.stringify({ theme: THEMES.includes(e.theme) ? e.theme : 'none', glow: /^#[0-9a-f]{6}$/i.test(e.glow || '') ? e.glow : null, pinned: e.pinned ? sanitizeTrack(e.pinned) : null, status: String(e.status || '').slice(0, 60) });
  }
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
function isAnimated(buf, ext) {
  if (ext === '.gif') { let frames = 0; for (let i = 0; i < buf.length - 1 && frames < 2; i++) if (buf[i] === 0x21 && buf[i + 1] === 0xf9) frames++; return frames > 1; }
  if (ext === '.webp') return buf.includes(Buffer.from('ANIM'));
  if (ext === '.png') return buf.includes(Buffer.from('acTL'));
  return false;
}
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
    if (isAnimated(buf, ext)) requireEnt(user.id, 'media.animated', 'Анимированные аватары и баннеры доступны с Aura');
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
route('DELETE', '/api/me', async (req) => {
  const { user } = auth(req); const b = await readJSON(req);
  if (user.pass !== '!' && !(await checkPass(String(b.password || ''), user.pass))) fail(400, 'bad_password', 'Неверный пароль');
  for (const k of ['avatar', 'banner']) if (user[k]) fsp.unlink(path.join(DIRS[k + 's'], user[k])).catch(() => {});
  run('DELETE FROM users WHERE id=?', user.id);
  return { ok: true };
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
    achievements: hidden ? [] : achievementsOf(u.id).filter((a) => a.unlocked).map(({ id, title, desc, ext }) => ({ id, title, desc, ext })),
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
  if (!['avatars', 'banners', 'plcovers'].includes(params.kind) || !/^[\w.-]+$/.test(params.file)) fail(404, 'not_found');
  return sendFile(req, res, path.join(DIRS[params.kind], params.file));
});

/* — Музыка — */
route('GET', '/api/trending', async (req, { query }) => {
  auth(req); const genre = String(query.get('genre') || '').slice(0, 40) || undefined;
  if (flag('audius')) { const list = await safe(auTrending(genre, 40, query.get('time') || 'week'), []); if (list.length) return list; }
  // Без Audius жанровые подборки собираем из открытого каталога Deezer
  const pls = await safe(dz('/search/playlist', { q: genre || 'top hits', limit: 6 }, 6 * 3600e3), {});
  const pl = (pls.data || []).find((p) => p.nb_tracks >= 20) || pls.data?.[0];
  if (!pl) return [];
  const tr = await safe(dz(`/playlist/${pl.id}/tracks`, { limit: 60 }, 6 * 3600e3), {});
  return (tr.data || []).map(dzTrack).filter(Boolean);
});
route('GET', '/api/catalog', (req, { query }) => {
  auth(req); const limit = clamp(Number(query.get('limit')) || 30, 1, 100); const offset = Math.max(0, Number(query.get('offset')) || 0);
  const items = all('SELECT * FROM tracks ORDER BY id DESC LIMIT ? OFFSET ?', limit + 1, offset).map(slTrack);
  return { items: items.slice(0, limit), offset, hasMore: items.length > limit, total: one('SELECT COUNT(*) c FROM tracks').c };
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
  for (const f of await fsp.readdir(DIRS.tmp).catch(() => [])) { const p = path.join(DIRS.tmp, f); const st = await fsp.stat(p).catch(() => null); if (st && now() - st.mtimeMs > 14 * DAY) fsp.rm(p, { recursive: true, force: true }).catch(() => {}); }
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
  const { user } = auth(req); rateLimit(req, 'tr', can(user.id, 'karaoke.plus') ? 400 : 40, 600e3); const b = await readJSON(req);
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
route('GET', '/api/history', (req, { query }) => { const { user } = auth(req); return all('SELECT data, played_at FROM plays WHERE user_id=? ORDER BY id DESC LIMIT ? OFFSET ?', user.id, clamp(Number(query.get('limit')) || 100, 1, 500), Math.max(0, Number(query.get('offset')) || 0)).map((r) => ({ ...J(r.data, {}), playedAt: r.played_at })); });
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
  return { id: p.id, name: p.name, description: p.description, public: !!p.public, createdAt: p.created_at, updatedAt: p.updated_at, count: tracks.length, cover: mediaUrl('plcovers', p.cover), covers: tracks.filter((t) => t.cover).slice(0, 4).map((t) => t.cover), duration: tracks.reduce((s, t) => s + (t.duration || 0), 0), tracks: withTracks ? tracks : undefined, owner: publicUser(one('SELECT * FROM users WHERE id=?', p.user_id), true) };
}
route('PUT', '/api/playlists/:id/cover', async (req, { params }) => {
  const { user } = auth(req); const p = ownPlaylist(user.id, params.id); const buf = await readBody(req, 6 << 20);
  const ext = imageExt(buf); if (!ext) fail(400, 'bad_image', 'Поддерживаются JPG, PNG, WEBP, GIF');
  if (isAnimated(buf, ext)) requireEnt(user.id, 'media.animated', 'Анимированные обложки доступны с Aura');
  const name = `pl${p.id}_${token(6)}${ext}`; await fsp.writeFile(path.join(DIRS.plcovers, name), buf);
  if (p.cover) fsp.unlink(path.join(DIRS.plcovers, p.cover)).catch(() => {});
  run('UPDATE playlists SET cover=?, updated_at=? WHERE id=?', name, now(), p.id);
  return playlistOut(one('SELECT * FROM playlists WHERE id=?', p.id), true);
});
route('DELETE', '/api/playlists/:id/cover', (req, { params }) => {
  const { user } = auth(req); const p = ownPlaylist(user.id, params.id);
  if (p.cover) fsp.unlink(path.join(DIRS.plcovers, p.cover)).catch(() => {});
  run('UPDATE playlists SET cover=NULL, updated_at=? WHERE id=?', now(), p.id);
  return playlistOut(one('SELECT * FROM playlists WHERE id=?', p.id), true);
});
const ownPlaylist = (uid, id) => { const p = one('SELECT * FROM playlists WHERE id=?', Number(id)); if (!p || p.user_id !== uid) fail(404, 'not_found', 'Плейлист не найден'); return p; };
route('GET', '/api/playlists', (req) => { const { user } = auth(req); return all('SELECT * FROM playlists WHERE user_id=? ORDER BY updated_at DESC', user.id).map((p) => playlistOut(p, false)); });
route('POST', '/api/playlists', async (req) => {
  const { user } = auth(req); const b = await readJSON(req); const name = String(b.name || '').trim().slice(0, 80) || 'Новый плейлист';
  const r = run('INSERT INTO playlists(user_id,name,description,public,created_at,updated_at) VALUES(?,?,?,?,?,?)', user.id, name, String(b.description || '').slice(0, 300), b.public === false ? 0 : 1, now(), now());
  logEvent(user.id, 'playlist');
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
/* ───────────────────────────── CAPTCHA (proof-of-work, проверка на сервере) ───────────────────────────── */
const CAPTCHA_SECRET = process.env.CAPTCHA_SECRET || kv('captcha_secret') || (() => { const v = token(32); setKv('captcha_secret', v); return v; })();
const sha256hex = (v) => crypto.createHash('sha256').update(v).digest('hex');
const hmacHex = (v) => crypto.createHmac('sha256', CAPTCHA_SECRET).update(v).digest('hex');
const safeEq = (a, b) => { const x = Buffer.from(String(a)); const y = Buffer.from(String(b)); return x.length === y.length && crypto.timingSafeEqual(x, y); };
route('GET', '/api/captcha', (req) => {
  rateLimit(req, 'captcha', 60, 600e3);
  const max = CFG.captchaDifficulty;
  const number = crypto.randomInt(0, max);
  const expires = now() + 5 * 60e3;
  const salt = `${token(9)}.${expires}`;
  const challenge = sha256hex(salt + number);
  return { algorithm: 'SHA-256', challenge, salt, maxnumber: max, signature: hmacHex(challenge) };
});
function verifyCaptcha(p) {
  if (!p || typeof p !== 'object') fail(400, 'captcha_required', 'Подтвердите, что вы не робот');
  const salt = String(p.salt || ''); const challenge = String(p.challenge || ''); const number = Number(p.number);
  const expires = Number(salt.split('.').pop());
  if (!expires || expires < now()) fail(400, 'captcha_expired', 'Проверка устарела — пройдите её ещё раз');
  if (!Number.isInteger(number) || !safeEq(hmacHex(challenge), p.signature || '') || sha256hex(salt + number) !== challenge) fail(400, 'captcha_invalid', 'Проверка не пройдена');
  const key = 'cap:' + challenge;
  if (one('SELECT 1 FROM codes WHERE code=?', key)) fail(400, 'captcha_used', 'Проверка уже использована — пройдите её ещё раз');
  run('INSERT INTO codes(code,kind,data,created_at,expires_at) VALUES(?,?,?,?,?)', key, 'captcha', '{}', now(), expires);
}

/* ───────────────────────────── Google: вход и привязка ───────────────────────────── */
const googleReady = () => !!(CFG.googleId && CFG.googleSecret && CFG.publicUrl);
function htmlPage(res, ok, title, text) {
  res.writeHead(ok ? 200 : 400, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(`<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Soundless</title><body style="margin:0;min-height:100vh;display:grid;place-items:center;background:#000;color:#ece6da;font:16px 'Segoe UI',system-ui,sans-serif"><div style="text-align:center;padding:32px;max-width:420px"><div style="font:italic 600 44px Georgia,serif;margin-bottom:10px">Soundless</div><div style="width:56px;height:56px;margin:18px auto;border-radius:18px;display:grid;place-items:center;font-size:28px;background:${ok ? '#e8e0cc;color:#000' : '#2a1414;color:#ff6b5e'}">${ok ? '✓' : '!'}</div><h2 style="margin:8px 0;font-weight:700">${esc(title)}</h2><p style="opacity:.6;line-height:1.5">${esc(text)}</p></div></body>`);
}
route('POST', '/api/auth/google/start', async (req) => {
  rateLimit(req, 'gstart', 30, 600e3);
  if (!googleReady()) fail(503, 'google_unavailable', 'Вход через Google временно недоступен');
  const b = await readJSON(req);
  const mode = b.mode === 'link' ? 'link' : 'login';
  let userId = null;
  if (mode === 'link') userId = auth(req).user.id; else verifyCaptcha(b.captcha);
  const state = token(18); const verifier = token(48);
  const challenge = crypto.createHash('sha256').update(verifier).digest('base64url');
  run('INSERT INTO codes(code,kind,user_id,data,created_at,expires_at) VALUES(?,?,?,?,?,?)', 'g:' + state, 'google', userId, JSON.stringify({ status: 'pending', mode, verifier }), now(), now() + 10 * 60e3);
  const u = new URL('https://accounts.google.com/o/oauth2/v2/auth');
  const params = { client_id: CFG.googleId, redirect_uri: `${CFG.publicUrl}/api/auth/google/callback`, response_type: 'code', scope: 'openid email profile', state, code_challenge: challenge, code_challenge_method: 'S256', prompt: 'select_account' };
  // Быстрый вход: подсказываем Google последний использованный адрес, чтобы не выбирать аккаунт заново
  const hint = String(b.hint || '').trim();
  if (/^[^\s@]{1,64}@[^\s@]{1,190}$/.test(hint)) { params.login_hint = hint; delete params.prompt; }
  u.search = new URLSearchParams(params).toString();
  return { state, url: u.toString() };
});
route('GET', '/api/auth/google/callback', async (req, { query, res }) => {
  const state = String(query.get('state') || ''); const code = query.get('code');
  const c = one('SELECT * FROM codes WHERE code=?', 'g:' + state);
  if (!c || c.expires_at < now()) return htmlPage(res, false, 'Ссылка устарела', 'Вернитесь в приложение и попробуйте ещё раз.');
  const d = J(c.data, {});
  const finish = (patch) => run('UPDATE codes SET data=?, user_id=? WHERE code=?', JSON.stringify({ ...d, ...patch }), patch.userId ?? c.user_id, c.code);
  if (!code || query.get('error')) { finish({ status: 'error', message: 'Вход через Google отменён' }); return htmlPage(res, false, 'Вход отменён', 'Вы можете закрыть это окно.'); }
  try {
    const tok = await fetchJSON('https://oauth2.googleapis.com/token', { method: 'POST', timeout: 15000, headers: { 'content-type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams({ code, client_id: CFG.googleId, client_secret: CFG.googleSecret, redirect_uri: `${CFG.publicUrl}/api/auth/google/callback`, grant_type: 'authorization_code', code_verifier: d.verifier }).toString() });
    const info = await fetchJSON('https://openidconnect.googleapis.com/v1/userinfo', { timeout: 15000, headers: { authorization: 'Bearer ' + tok.access_token } });
    if (!info.sub) throw new Error('no sub');
    if (d.mode === 'link') {
      const other = one('SELECT id FROM users WHERE google_sub=? AND id<>?', info.sub, c.user_id);
      if (other) { finish({ status: 'error', message: 'Этот Google-аккаунт уже привязан к другому профилю' }); return htmlPage(res, false, 'Аккаунт уже занят', 'Этот Google-аккаунт привязан к другому профилю Soundless.'); }
      run('UPDATE users SET google_sub=?, google_email=? WHERE id=?', info.sub, info.email || null, c.user_id);
      finish({ status: 'ok' });
      return htmlPage(res, true, 'Google привязан', 'Вернитесь в приложение — всё готово.');
    }
    let u = one('SELECT * FROM users WHERE google_sub=?', info.sub);
    if (!u) {
      if (!flag('registration')) { finish({ status: 'error', message: 'Регистрация временно закрыта' }); return htmlPage(res, false, 'Регистрация закрыта', 'Попробуйте позже.'); }
      let un = lower(String(info.email || '').split('@')[0]).replace(/[^a-z0-9_.]/g, '').replace(/^[._]+|[._]+$/g, '').slice(0, 20);
      if (!validUsername(un) || RESERVED.has(un)) un = 'user' + crypto.randomInt(1e5, 1e7);
      while (one('SELECT id FROM users WHERE username=?', un)) un = (un.slice(0, 16) + crypto.randomInt(10, 9999)).slice(0, 24);
      const name = String(info.name || un).slice(0, 40);
      const r = run('INSERT INTO users(username,name,pass,google_sub,google_email,created_at,last_seen,q) VALUES(?,?,?,?,?,?,?,?)', un, name, '!', info.sub, info.email || null, now(), now(), lower(`${un} ${name}`));
      u = one('SELECT * FROM users WHERE id=?', Number(r.lastInsertRowid));
      bot.onRegister(u.id);
    }
    if (u.banned) { finish({ status: 'error', message: 'Аккаунт заблокирован' }); return htmlPage(res, false, 'Аккаунт заблокирован', ''); }
    finish({ status: 'ok', userId: u.id });
    return htmlPage(res, true, 'Вход выполнен', 'Вернитесь в приложение Soundless.');
  } catch (e) {
    log('[google]', e.message); finish({ status: 'error', message: 'Не удалось войти через Google' });
    return htmlPage(res, false, 'Не получилось', 'Попробуйте ещё раз.');
  }
});
route('GET', '/api/auth/google/poll', (req, { query }) => {
  const c = one('SELECT * FROM codes WHERE code=?', 'g:' + String(query.get('state') || ''));
  if (!c || c.expires_at < now()) return { status: 'expired' };
  const d = J(c.data, {});
  if (d.status === 'ok') {
    run('DELETE FROM codes WHERE code=?', c.code);
    const u = one('SELECT * FROM users WHERE id=?', c.user_id);
    if (!u || u.banned) return { status: 'error', message: 'Аккаунт недоступен' };
    return d.mode === 'login' ? { status: 'ok', token: createSession(req, u.id), user: privateUser(u) } : { status: 'ok', user: privateUser(u) };
  }
  if (d.status === 'error') { run('DELETE FROM codes WHERE code=?', c.code); return { status: 'error', message: d.message }; }
  return { status: 'pending' };
});
route('DELETE', '/api/me/google', (req) => {
  const { user } = auth(req);
  if (user.pass === '!') fail(400, 'no_password', 'Сначала задайте пароль, иначе вы не сможете войти');
  run('UPDATE users SET google_sub=NULL, google_email=NULL WHERE id=?', user.id);
  return privateUser(one('SELECT * FROM users WHERE id=?', user.id));
});

/* ───────────────────────────── Источники: Deezer (открытый API), SoundCloud и Spotify (официальные API) ───────────────────────────── */
const normText = (v) => lower(v).replace(/\([^)]*\)|\[[^\]]*\]/g, ' ').replace(/\b(feat|ft)\.?\b.*$/i, ' ').replace(/[^\p{L}\p{N}]+/gu, ' ').trim();
const trackKey = (t) => normText(primaryArtist(t.artist)) + '|' + normText(t.title);
async function dz(p, params = {}, ttl = 600000) {
  const u = new URL('https://api.deezer.com' + p);
  for (const [k, v] of Object.entries(params)) if (v != null && v !== '') u.searchParams.set(k, v);
  return memoize('dz:' + u, ttl, async () => {
    const j = await withRetry(() => fetchJSON(u.toString(), { timeout: 9000 }));
    if (j && j.error) { const e = new Error('deezer: ' + (j.error.message || j.error.type)); e.status = 502; throw e; }
    return j;
  });
}
function dzTrack(t) {
  if (!t || t.readable === false || !t.id) return null;
  const al = t.album || {};
  return { id: 'dz:' + t.id, source: 'deezer', title: t.title, artist: t.artist?.name || '', artistId: t.artist?.id ? 'dz:' + t.artist.id : null, album: al.title || '', duration: t.duration || 0, cover: al.cover_big || al.cover_medium || null, coverLarge: al.cover_xl || al.cover_big || null, coverSmall: al.cover_medium || al.cover_small || null, explicit: !!t.explicit_lyrics, preview: t.preview || null, rank: t.rank || 0 };
}
const dzArtist = (a) => a && { id: 'dz:' + a.id, source: 'deezer', name: a.name, avatar: a.picture_xl || a.picture_big || a.picture_medium || null, followers: a.nb_fan || 0 };
const dzAlbum = (a) => a && ({ id: 'dz:' + a.id, source: 'deezer', title: a.title || 'Без названия', type: String(a.record_type || 'album').toLowerCase(), trackCount: a.nb_tracks || 0, cover: a.cover_xl || a.cover_big || a.cover_medium || null, releaseDate: a.release_date || '' });
async function withRetry(fn, tries = 3) {
  let last;
  for (let i = 0; i < tries; i++) {
    try { return await fn(); } catch (e) { last = e; if (e.status && e.status < 500 && e.status !== 429) throw e; await sleep(300 * 2 ** i + Math.random() * 200); }
  }
  throw last;
}
const scState = { token: null, exp: 0 };
const scReady = () => !!(CFG.scId && CFG.scSecret);
async function scToken() {
  if (!scReady()) return null;
  if (scState.token && scState.exp > now() + 60000) return scState.token;
  const r = await fetchJSON('https://secure.soundcloud.com/oauth/token', { method: 'POST', timeout: 10000, headers: { 'content-type': 'application/x-www-form-urlencoded', authorization: 'Basic ' + Buffer.from(`${CFG.scId}:${CFG.scSecret}`).toString('base64') }, body: 'grant_type=client_credentials' });
  scState.token = r.access_token; scState.exp = now() + (r.expires_in || 3600) * 1000;
  return scState.token;
}
async function scApi(p, params = {}, ttl = 300000) {
  const t = await scToken(); if (!t) return null;
  const u = new URL('https://api.soundcloud.com' + p);
  for (const [k, v] of Object.entries(params)) if (v != null && v !== '') u.searchParams.set(k, v);
  return memoize('sc:' + u, ttl, () => withRetry(() => fetchJSON(u.toString(), { timeout: 10000, headers: { authorization: 'OAuth ' + t, accept: 'application/json; charset=utf-8' } })));
}
function scTrack(t) {
  if (!t || !(t.urn || t.id) || t.access === 'blocked' || t.streamable === false) return null;
  const art = (t.artwork_url || t.user?.avatar_url || '').replace('-large.', '-t500x500.') || null;
  return { id: 'sc:' + (t.urn || `soundcloud:tracks:${t.id}`), source: 'soundcloud', title: t.title, artist: t.metadata_artist || t.user?.username || '', artistId: t.user?.urn ? 'sc:' + t.user.urn : null, album: '', genre: t.genre || '', duration: Math.round((t.duration || 0) / 1000), cover: art, coverLarge: art, coverSmall: art ? art.replace('-t500x500.', '-large.') : null, plays: t.playback_count || 0, waveform: t.waveform_url || null, preview: t.access === 'preview' ? 'snippet' : undefined };
}
async function scStream(urn) {
  const t = await scToken(); if (!t) fail(404, 'unavailable', 'Источник SoundCloud не подключён');
  const s = await scApi(`/tracks/${encodeURIComponent(urn)}/streams`, {}, 60000);
  const pick = s?.http_mp3_128_url ? ['progressive', s.http_mp3_128_url, 'mp3'] : s?.hls_aac_160_url ? ['hls', s.hls_aac_160_url, 'm4a'] : s?.hls_mp3_128_url ? ['hls', s.hls_mp3_128_url, 'mp3'] : null;
  if (!pick) fail(404, 'unavailable', 'Трек недоступен для воспроизведения');
  const r = await fetch(pick[1], { redirect: 'manual', headers: { authorization: 'OAuth ' + t }, signal: AbortSignal.timeout(10000) });
  const loc = r.headers.get('location');
  await r.body?.cancel().catch(() => {});
  return { url: loc || pick[1], protocol: pick[0], extension: pick[2] };
}
const spState = { token: null, exp: 0 };
const spReady = () => !!(CFG.spId && CFG.spSecret);
async function spApi(p, params = {}) {
  if (!spReady()) return null;
  if (!spState.token || spState.exp < now() + 60000) {
    const r = await fetchJSON('https://accounts.spotify.com/api/token', { method: 'POST', timeout: 10000, headers: { 'content-type': 'application/x-www-form-urlencoded', authorization: 'Basic ' + Buffer.from(`${CFG.spId}:${CFG.spSecret}`).toString('base64') }, body: 'grant_type=client_credentials' });
    spState.token = r.access_token; spState.exp = now() + (r.expires_in || 3600) * 1000;
  }
  const u = new URL('https://api.spotify.com/v1' + p);
  for (const [k, v] of Object.entries(params)) if (v != null && v !== '') u.searchParams.set(k, v);
  return memoize('sp:' + u, 600000, () => withRetry(() => fetchJSON(u.toString(), { timeout: 10000, headers: { authorization: 'Bearer ' + spState.token } })));
}
function spTrack(t) {
  if (!t?.id) return null;
  const img = t.album?.images || [];
  return { id: 'sp:' + t.id, source: 'spotify', title: t.name, artist: (t.artists || []).map((a) => a.name).join(', '), artistId: null, album: t.album?.name || '', duration: Math.round((t.duration_ms || 0) / 1000), cover: img[1]?.url || img[0]?.url || null, coverLarge: img[0]?.url || null, coverSmall: img[2]?.url || img[1]?.url || null, explicit: !!t.explicit, isrc: t.external_ids?.isrc };
}

/* Сопоставление метаданных (Deezer/Spotify) с источником полного воспроизведения */
function similarity(a, b) {
  const A = new Set(normText(a).split(' ').filter(Boolean)); const B = new Set(normText(b).split(' ').filter(Boolean));
  if (!A.size || !B.size) return 0;
  let inter = 0; for (const x of A) if (B.has(x)) inter++;
  return inter / Math.max(A.size, B.size);
}
function matchScore(meta, cand) {
  const ts = similarity(meta.title, cand.title);
  const ma = normText(primaryArtist(meta.artist)); const ca = normText(cand.artist) + ' ' + normText(cand.title);
  const as = ma && ca.includes(ma) ? 1 : similarity(meta.artist, cand.artist);
  const dd = meta.duration && cand.duration ? Math.abs(meta.duration - cand.duration) : 0;
  const dp = dd <= 3 ? 0 : dd <= 8 ? 0.15 : dd <= 20 ? 0.4 : 0.8;
  return ts * 0.6 + as * 0.4 - dp - (cand.preview ? 0.5 : 0);
}
async function findPlayable(meta) {
  const key = 'match:' + sha1(trackKey(meta) + '|' + Math.round(meta.duration || 0));
  const cached = cacheGet(key); if (cached !== undefined) return cached;
  const q = `${primaryArtist(meta.artist)} ${normText(meta.title)}`.trim();
  const [cat, scl, au] = await Promise.all([
    Promise.resolve(catalogSearch(q, 8)),
    scReady() ? safe(scApi('/tracks', { q, access: 'playable', limit: 10 }).then((r) => (r?.collection || r || []).map(scTrack).filter(Boolean))) : [],
    flag('audius') ? safe(auSearch(q, 8)) : [],
  ]);
  let best = null; let bestScore = 0.72;
  for (const c of [...cat, ...scl, ...au]) { const sc = matchScore(meta, c); if (sc > bestScore) { best = c; bestScore = sc; } }
  cacheSet(key, best, best ? 7 * DAY : 6 * 3600e3);
  return best;
}
async function playInfo(req, tok, track, quality) {
  const id = track.id;
  if (id.startsWith('sl:')) return { url: `${CFG.cdnUrl}/api/stream/${Number(id.slice(3))}?k=${encodeURIComponent(tok)}${quality ? '&q=' + quality : ''}`, protocol: 'progressive', track: id };
  if (id.startsWith('au:')) return { url: `https://api.audius.co/v1/tracks/${encodeURIComponent(id.slice(3))}/stream?app_name=${CFG.audiusApp}`, protocol: 'progressive', track: id };
  if (id.startsWith('sc:')) return { ...(await scStream(id.slice(3))), track: id };
  if (id.startsWith('dz:') || id.startsWith('sp:')) {
    let meta = track;
    if (id.startsWith('dz:') && (!meta.title || !meta.artist)) meta = dzTrack(await dz('/track/' + Number(id.slice(3)))) || meta;
    const m = meta.title ? await findPlayable(meta) : null;
    if (m) return { ...(await playInfo(req, tok, m, quality)), matched: m };
    const preview = meta.preview || (id.startsWith('dz:') ? dzTrack(await safe(dz('/track/' + Number(id.slice(3))), null))?.preview : null);
    if (preview) return { url: preview, protocol: 'progressive', preview: true, track: id };
  }
  fail(404, 'unavailable', 'Трек недоступен для воспроизведения');
}
route('POST', '/api/play', async (req) => {
  const { token: tok } = auth(req);
  const b = await readJSON(req);
  const t = sanitizeTrack(b.track);
  if (!t) fail(400, 'bad_track');
  if (t.id.startsWith('local:')) fail(400, 'local');
  if (!t.preview && b.track?.preview && /^https:\/\/cdn[a-z-]*\.dzcdn\.net\//.test(b.track.preview)) t.preview = b.track.preview;
  return playInfo(req, tok, t, { low: 'low', medium: 'medium', high: 'high' }[b.quality] || '');
});

const scDownloadJobs = new Map();
async function downloadSoundCloudFile(trackId) {
  await toolsReady;
  if (!scReady()) fail(404, 'unavailable', 'Источник SoundCloud не подключён');
  const id = String(trackId || '').slice(3, 300);
  if (!id) fail(400, 'bad_track');
  const raw = await scApi(`/tracks/${encodeURIComponent(id)}`, {}, 3600e3);
  if (!raw || raw.access === 'preview' || raw.streamable === false) fail(404, 'unavailable', 'Трек недоступен для загрузки');
  const key = sha1(String(raw.urn || raw.id || id));
  const cached = path.join(DIRS.tmp, `sc-offline-${key}.mp3`);
  const cachedStat = await fsp.stat(cached).catch(() => null);
  if (cachedStat?.size > 16 * 1024 && now() - cachedStat.mtimeMs < 14 * DAY) return cached;
  if (cachedStat) await fsp.unlink(cached).catch(() => {});
  if (scDownloadJobs.has(key)) return scDownloadJobs.get(key);

  const job = (async () => {
    const dir = await fsp.mkdtemp(path.join(DIRS.tmp, 'sc-offline-'));
    const out = path.join(dir, 'audio.mp3');
    try {
      if (ytdlpOk) {
        const target = path.join(dir, 'yt.%(ext)s');
        const url = raw.permalink_url || `https://api.soundcloud.com/tracks/${raw.id}`;
        const result = await sh(CFG.ytdlp, ['--no-warnings', '--no-progress', '--no-playlist', '-N', '4', '-f', 'bestaudio[ext=mp3]/bestaudio', '-x', '--audio-format', 'mp3', '--audio-quality', '0', '-o', target, url], 300000);
        const file = (await fsp.readdir(dir)).find((name) => name.startsWith('yt.') && name.toLowerCase().endsWith('.mp3') && !name.endsWith('.part'));
        if (result.code === 0 && file) {
          await fsp.rename(path.join(dir, file), cached);
          return cached;
        }
        warn(`yt-dlp ${id}: ${result.err.trim().split('\n').pop() || `exit ${result.code}`} → soundcloud stream`);
      }

      const stream = await scStream(raw.urn || raw.id);
      if (stream.protocol === 'hls') {
        if (!ffmpegOk) fail(503, 'download_unavailable', 'Для загрузки HLS-потока на сервере нужен ffmpeg');
        const result = await sh(CFG.ffmpeg, ['-y', '-hide_banner', '-loglevel', 'error', '-i', stream.url, '-vn', '-map', '0:a:0', '-c:a', 'libmp3lame', '-q:a', '2', '-map_metadata', '-1', out], 600000);
        if (result.code !== 0) throw new Error(result.err.trim().split('\n').pop() || `ffmpeg exit ${result.code}`);
      } else {
        const response = await fetch(stream.url, { headers: { 'user-agent': UA }, signal: AbortSignal.timeout(300000) });
        if (!response.ok || !response.body) throw new Error(`SoundCloud stream HTTP ${response.status}`);
        await pipeline(Readable.fromWeb(response.body), fs.createWriteStream(out));
      }

      const st = await fsp.stat(out).catch(() => null);
      if (!st || st.size <= 16 * 1024) throw new Error('SoundCloud не отдал аудиофайл');
      await fsp.rename(out, cached);
      return cached;
    } finally {
      await fsp.rm(dir, { recursive: true, force: true }).catch(() => {});
    }
  })();
  scDownloadJobs.set(key, job);
  try { return await job; } finally { scDownloadJobs.delete(key); }
}

route('GET', '/api/download', async (req, { query, res }) => {
  auth(req);
  rateLimit(req, 'download', 60, 10 * 60e3);
  const id = String(query.get('id') || '');
  if (!id.startsWith('sc:')) fail(400, 'bad_track', 'Скачивание SoundCloud доступно только для треков SoundCloud');
  const file = await downloadSoundCloudFile(id);
  return sendFile(req, res, file, 'audio/mpeg', 'private, max-age=86400');
});

/* Волна (waveform) — реальные данные: SoundCloud или анализ файла каталога через ffmpeg */
async function computeWaveform(file, bars = 120) {
  if (!ffmpegOk) return null;
  const r = await new Promise((resolve) => {
    const chunks = []; let p;
    try { p = spawn(CFG.ffmpeg, ['-v', 'error', '-i', file, '-ac', '1', '-ar', '2000', '-f', 's16le', '-'], { stdio: ['ignore', 'pipe', 'ignore'] }); } catch { return resolve(null); }
    const t = setTimeout(() => p.kill('SIGKILL'), 120000);
    p.stdout.on('data', (d) => chunks.push(d)); p.on('close', () => { clearTimeout(t); resolve(Buffer.concat(chunks)); }); p.on('error', () => { clearTimeout(t); resolve(null); });
  });
  if (!r || r.length < bars * 4) return null;
  const n = Math.floor(r.length / 2); const per = Math.floor(n / bars); const peaks = [];
  for (let b = 0; b < bars; b++) { let m = 0; for (let i = b * per; i < (b + 1) * per; i++) { const v = Math.abs(r.readInt16LE(i * 2)); if (v > m) m = v; } peaks.push(m); }
  const max = Math.max(...peaks) || 1;
  return peaks.map((v) => Math.round((v / max) * 100) / 100);
}
function downsample(samples, bars = 120) {
  if (!Array.isArray(samples) || !samples.length) return null;
  const per = samples.length / bars; const out = [];
  for (let b = 0; b < bars; b++) { let m = 0; for (let i = Math.floor(b * per); i < Math.floor((b + 1) * per); i++) m = Math.max(m, samples[i] || 0); out.push(m); }
  const max = Math.max(...out) || 1;
  return out.map((v) => Math.round((v / max) * 100) / 100);
}
route('POST', '/api/waveform', async (req) => {
  auth(req);
  const b = await readJSON(req); const t = sanitizeTrack(b.track); if (!t) fail(400, 'bad_track');
  const key = 'wf:' + t.id; const c = cacheGet(key); if (c !== undefined) return { peaks: c };
  let peaks = null;
  try {
    if (t.id.startsWith('sl:')) peaks = J(one('SELECT waveform FROM tracks WHERE id=?', Number(t.id.slice(3)))?.waveform, null);
    else {
      let sc = t.id.startsWith('sc:') ? t : null;
      if (!sc && (t.id.startsWith('dz:') || t.id.startsWith('sp:'))) { const m = await findPlayable(t); if (m?.id.startsWith('sc:')) sc = m; else if (m?.id.startsWith('sl:')) peaks = J(one('SELECT waveform FROM tracks WHERE id=?', Number(m.id.slice(3)))?.waveform, null); }
      if (sc && scReady()) {
        const full = sc.waveform ? sc : scTrack(await scApi(`/tracks/${encodeURIComponent(sc.id.slice(3))}`, {}, 3600e3));
        if (full?.waveform) { const w = await fetchJSON(full.waveform.replace(/\.png$/, '.json'), { timeout: 8000 }); peaks = downsample(w.samples); }
      }
    }
  } catch (e) { log('[waveform]', e.message); }
  cacheSet(key, peaks, peaks ? 30 * DAY : 3600e3);
  return { peaks };
});

/* ───────────────────────────── Поиск по источникам (кэш, пагинация) ───────────────────────────── */
const PAGE = 20;
const withTimeout = (p, ms) => Promise.race([p, new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), ms))]);
async function searchSource(src, q, page) {
  const off = page * PAGE;
  if (src === 'deezer') { const r = await dz('/search', { q, index: off, limit: PAGE }, 300000); return { tracks: (r.data || []).map(dzTrack).filter(Boolean), more: !!r.next }; }
  if (src === 'soundcloud') { if (!scReady()) return null; const r = await scApi('/tracks', { q, access: 'playable', limit: PAGE, offset: off, linked_partitioning: true }); const col = r?.collection || r || []; return { tracks: col.map(scTrack).filter(Boolean), more: !!r?.next_href }; }
  if (src === 'spotify') { if (!spReady()) return null; const r = await spApi('/search', { q, type: 'track', limit: PAGE, offset: off }); return { tracks: (r?.tracks?.items || []).map(spTrack).filter(Boolean), more: !!r?.tracks?.next }; }
  if (src === 'audius') { if (!flag('audius')) return null; const list = await auSearch(q, PAGE + off); return { tracks: list.slice(off, off + PAGE), more: list.length > off + PAGE }; }
  if (src === 'catalog') { const list = all('SELECT * FROM tracks WHERE ' + lower(q).split(/\s+/).filter(Boolean).slice(0, 6).map(() => 'q LIKE ?').join(' AND ') + ' ORDER BY plays DESC LIMIT ? OFFSET ?', ...lower(q).split(/\s+/).filter(Boolean).slice(0, 6).map((w) => `%${w}%`), PAGE + 1, off).map(slTrack); return { tracks: list.slice(0, PAGE), more: list.length > PAGE }; }
  return null;
}
route('GET', '/api/search', async (req, { query }) => {
  auth(req);
  const q = String(query.get('q') || '').trim().slice(0, 100); const type = query.get('type') || 'all';
  const source = query.get('source') || 'all'; const page = clamp(Number(query.get('page')) || 0, 0, 20);
  if (!q) return { tracks: [], artists: [], users: [], sources: {}, page, hasMore: false };
  const key = 'srch:' + sha1([lower(q), type, source, page].join('|'));
  const c = cacheGet(key); if (c) return c;
  const srcs = source === 'all' ? ['catalog', 'soundcloud', 'deezer', 'spotify', 'audius'] : [source];
  const sources = {};
  const lists = type === 'users' || type === 'artists' ? [] : await Promise.all(srcs.map(async (s) => {
    try { const r = await withTimeout(searchSource(s, q, page), 9000); sources[s] = r ? { ok: true, count: r.tracks.length, more: r.more } : { ok: false, disabled: true }; return r?.tracks || []; }
    catch (e) { sources[s] = { ok: false, error: true }; log('[search]', s, e.message); return []; }
  }));
  const seen = new Set(); const tracks = [];
  const maxLen = Math.max(0, ...lists.map((l) => l.length));
  for (let i = 0; i < maxLen; i++) for (const l of lists) { const t = l[i]; if (!t) continue; const k = trackKey(t); if (seen.has(k)) continue; seen.add(k); tracks.push(t); }
  let artists = []; let users = [];
  if (page === 0 && (type === 'all' || type === 'artists')) {
    const [dza] = await Promise.all([safe(dz('/search/artist', { q, limit: 12 }, 600000).then((r) => (r.data || []).map(dzArtist)))]);
    artists = [...catalogArtists(q, 6), ...dza];
  }
  if (page === 0 && (type === 'all' || type === 'users')) users = all('SELECT * FROM users WHERE q LIKE ? AND banned=0 ORDER BY last_seen DESC LIMIT 12', `%${lower(q).replace(/^@/, '')}%`).map((u) => publicUser(u, true));
  const out = { tracks, artists, users, sources, page, hasMore: Object.values(sources).some((s) => s.more) };
  if (tracks.length || artists.length) cacheSet(key, out, 5 * 60e3);
  return out;
});
route('GET', '/api/artist', async (req, { query }) => {
  auth(req); const id = String(query.get('id') || '');
  if (id.startsWith('dz:')) {
    const aid = Number(id.slice(3));
    const [a, top, rel, albums] = await Promise.all([
      dz('/artist/' + aid, {}, 3600e3), dz(`/artist/${aid}/top`, { limit: 50 }, 3600e3),
      safe(dz(`/artist/${aid}/related`, { limit: 12 }, 3600e3), {}),
      safe(dz(`/artist/${aid}/albums`, { limit: 100 }, 3600e3), {}),
    ]);
    const releases = (albums.data || []).map(dzAlbum).filter((x) => x && ['album', 'ep'].includes(x.type));
    return { artist: dzArtist(a), tracks: (top.data || []).map(dzTrack).filter(Boolean), related: (rel.data || []).map(dzArtist), releases };
  }
  if (id.startsWith('sl:')) {
    const name = id.slice(3);
    const tracks = all('SELECT * FROM tracks WHERE q LIKE ? ORDER BY plays DESC LIMIT 200', `%${lower(name)}%`).filter((t) => lower(t.artist).includes(lower(name))).map(slTrack);
    const art = await safe(dz('/search/artist', { q: name, limit: 1 }).then((r) => r.data?.[0]), null);
    const [top, albums] = art ? await Promise.all([
      safe(dz(`/artist/${art.id}/top`, { limit: 30 }).then((r) => (r.data || []).map(dzTrack).filter(Boolean)), []),
      safe(dz(`/artist/${art.id}/albums`, { limit: 100 }).then((r) => (r.data || []).map(dzAlbum).filter((x) => x && ['album', 'ep'].includes(x.type))), []),
    ]) : [[], []];
    return { artist: { id, source: 'soundless', name, avatar: art?.picture_xl || tracks[0]?.cover || null, tracks: tracks.length }, tracks: [...tracks, ...top], releases: albums };
  }
  if (id.startsWith('au:')) {
    const uid = id.slice(3);
    const [u, tr, albums] = await Promise.all([
      safe(audius('/users/' + encodeURIComponent(uid)), null),
      safe(audius(`/users/${encodeURIComponent(uid)}/tracks`, { limit: 50, sort: 'plays' })),
      safe(audius(`/users/${encodeURIComponent(uid)}/albums`, { limit: 100, sort_method: 'recent' })),
    ]);
    if (!u) fail(404, 'not_found', 'Исполнитель не найден');
    return { artist: auArtist(u), tracks: tr.map(auTrack).filter(Boolean), releases: albums.map(auAlbum).filter(Boolean) };
  }
  if (id.startsWith('sc:')) {
    const urn = id.slice(3);
    const [u, tr, pls] = await Promise.all([
      scApi(`/users/${encodeURIComponent(urn)}`, {}, 3600e3),
      scApi(`/users/${encodeURIComponent(urn)}/tracks`, { limit: 50, access: 'playable' }, 600000),
      scApi(`/users/${encodeURIComponent(urn)}/playlists`, { limit: 200 }, 600000),
    ]);
    if (!u) fail(404, 'not_found', 'Исполнитель не найден');
    const releases = (pls?.collection || pls || []).filter((p) => p?.kind === 'playlist' && ['album', 'ep'].includes(String(p.set_type || '').toLowerCase())).map((p) => ({
      id: 'sc:' + (p.id || p.urn || ''), source: 'soundcloud', title: p.title || 'Без названия',
      type: String(p.set_type).toLowerCase(), trackCount: p.track_count || p.tracks?.length || 0,
      cover: (p.artwork_url || '').replace('-large.', '-t500x500.') || null, releaseDate: p.release_date || p.created_at || '', permalink: p.permalink_url || null,
    }));
    return { artist: { id, source: 'soundcloud', name: u.username, avatar: (u.avatar_url || '').replace('-large.', '-t500x500.'), followers: u.followers_count || 0 }, tracks: (tr?.collection || tr || []).map(scTrack).filter(Boolean), releases };
  }
  fail(400, 'bad_id');
});
route('GET', '/api/album', async (req, { query }) => {
  auth(req);
  const id = String(query.get('id') || '');
  if (id.startsWith('dz:')) {
    const albumId = Number(id.slice(3));
    if (!Number.isSafeInteger(albumId) || albumId < 1) fail(400, 'bad_id');
    const [album, list] = await Promise.all([dz(`/album/${albumId}`, {}, 3600e3), dz(`/album/${albumId}/tracks`, { limit: 100 }, 3600e3)]);
    const meta = dzAlbum(album);
    const tracks = (list.data || []).map((t) => {
      const track = dzTrack(t);
      return track ? { ...track, album: album.title || track.album, cover: album.cover_big || album.cover_medium || track.cover, coverLarge: album.cover_xl || album.cover_big || track.coverLarge, coverSmall: album.cover_medium || album.cover_small || track.coverSmall } : null;
    }).filter(Boolean);
    return { album: meta, tracks };
  }
  if (id.startsWith('sc:')) {
    const playlistId = id.slice(3);
    if (!playlistId) fail(400, 'bad_id');
    const playlist = await scApi(`/playlists/${encodeURIComponent(playlistId)}`, {}, 3600e3);
    if (!playlist) fail(404, 'not_found', 'Альбом не найден');
    const tracks = (playlist.tracks || []).map(scTrack).filter(Boolean);
    return { album: { id, source: 'soundcloud', title: playlist.title || 'Без названия', type: String(playlist.set_type || 'album').toLowerCase(), trackCount: tracks.length, cover: (playlist.artwork_url || '').replace('-large.', '-t500x500.') || null }, tracks };
  }
  if (id.startsWith('au:')) {
    const albumId = id.slice(3);
    if (!albumId) fail(400, 'bad_id');
    const [album, list] = await Promise.all([
      audius(`/playlists/${encodeURIComponent(albumId)}`, {}, 3600e3),
      audius(`/playlists/${encodeURIComponent(albumId)}/tracks`, { limit: 100 }, 3600e3),
    ]);
    if (!album) fail(404, 'not_found', 'Альбом не найден');
    return { album: auAlbum(album), tracks: (Array.isArray(list) ? list : []).map(auTrack).filter(Boolean) };
  }
  fail(400, 'bad_id');
});

/* ───────────────────────────── Вкус, волна, миксы, умная очередь ───────────────────────────── */
route('GET', '/api/taste/artists', async (req, { query }) => {
  auth(req); const q = String(query.get('q') || '').trim();
  const r = q ? await dz('/search/artist', { q, limit: 24 }, 600000) : await dz('/chart/0/artists', { limit: 30 }, 3600e3);
  return (r.data || []).map(dzArtist);
});
route('PUT', '/api/me/taste', async (req) => {
  const { user } = auth(req); const b = await readJSON(req);
  const list = (Array.isArray(b.artists) ? b.artists : []).filter((a) => a && /^dz:\d+$/.test(String(a.id))).slice(0, 30).map((a) => ({ id: String(a.id), name: String(a.name || '').slice(0, 80), avatar: typeof a.avatar === 'string' && a.avatar.startsWith('https://') ? a.avatar.slice(0, 300) : null }));
  run('UPDATE users SET taste_artists=? WHERE id=?', JSON.stringify(list), user.id);
  return privateUser(one('SELECT * FROM users WHERE id=?', user.id));
});
function spread(list) { // не ставим один и тот же исполнитель подряд
  const out = []; const pool = shuffle(list);
  while (pool.length) { const i = pool.findIndex((t) => !out.length || primaryArtist(t.artist) !== primaryArtist(out[out.length - 1].artist)); out.push(pool.splice(i >= 0 ? i : 0, 1)[0]); }
  return out;
}
async function tasteSeeds(user) {
  let artists = J(user.taste_artists, []).map((a) => Number(String(a.id).slice(3))).filter(Boolean);
  if (!artists.length) {
    const names = topArtists(user.id, now() - 90 * DAY, 5).map((a) => a.name);
    const found = await Promise.all(names.map((n) => safe(dz('/search/artist', { q: n, limit: 1 }, 7 * DAY).then((r) => r.data?.[0]?.id), null)));
    artists = found.filter(Boolean);
  }
  return artists.slice(0, 10);
}
async function buildWave(user, advanced) {
  const seeds = await tasteSeeds(user);
  if (!seeds.length) return (await dz('/chart/0/tracks', { limit: 50 }, 3600e3)).data.map(dzTrack).filter(Boolean);
  const parts = await Promise.all(seeds.map(async (aid) => {
    const [top, rel] = await Promise.all([safe(dz(`/artist/${aid}/top`, { limit: 6 }, 6 * 3600e3), {}), safe(dz(`/artist/${aid}/related`, { limit: advanced ? 6 : 4 }, 24 * 3600e3), {})]);
    const relTop = await Promise.all((rel.data || []).map((a) => safe(dz(`/artist/${a.id}/top`, { limit: advanced ? 3 : 2 }, 6 * 3600e3), {}).then((r) => r.data || [])));
    let deep = [];
    if (advanced && rel.data?.[0]) { // второй круг похожих — только для Aura
      const r2 = await safe(dz(`/artist/${rel.data[0].id}/related`, { limit: 4 }, 24 * 3600e3), {});
      deep = (await Promise.all((r2.data || []).map((a) => safe(dz(`/artist/${a.id}/top`, { limit: 2 }, 6 * 3600e3), {}).then((r) => r.data || [])))).flat();
    }
    return [...(top.data || []), ...relTop.flat(), ...deep];
  }));
  const likes = all('SELECT data FROM likes WHERE user_id=? ORDER BY created_at DESC LIMIT 40', user.id).map((r) => J(r.data, null)).filter(Boolean);
  return spread(uniq([...parts.flat().map(dzTrack).filter(Boolean), ...shuffle(likes).slice(0, 8)])).slice(0, advanced ? 80 : 50);
}
route('GET', '/api/wave', async (req) => { const { user } = auth(req); return buildWave(user, can(user.id, 'mixes.advanced')); });
const MOODS = [
  { id: 'evening', title: 'Спокойный вечер', q: 'chill evening' },
  { id: 'city', title: 'Энергия города', q: 'city energy' },
  { id: 'inspire', title: 'Вдохновение', q: 'inspiration' },
  { id: 'night', title: 'Ночной драйв', q: 'night drive' },
];
async function moodMix(m) {
  const pls = await dz('/search/playlist', { q: m.q, limit: 6 }, 6 * 3600e3);
  const pl = (pls.data || []).find((p) => p.nb_tracks >= 15) || pls.data?.[0];
  if (!pl) return null;
  const tr = await dz(`/playlist/${pl.id}/tracks`, { limit: 60 }, 6 * 3600e3);
  const tracks = (tr.data || []).map(dzTrack).filter(Boolean);
  return { id: m.id, title: m.title, cover: pl.picture_xl || pl.picture_big || tracks[0]?.coverLarge || null, count: tracks.length, tracks };
}
route('GET', '/api/home', async (req) => {
  const { user } = auth(req);
  const advanced = can(user.id, 'mixes.advanced');
  const likes = all('SELECT data FROM likes WHERE user_id=? ORDER BY created_at DESC LIMIT 80', user.id).map((r) => J(r.data, null)).filter(Boolean);
  const favorites = uniq([...likes, ...topTracks(user.id, 0, 40)]);
  const [wave, moods, chart] = await Promise.all([
    safe(buildWave(user, advanced), []),
    Promise.all(MOODS.map((m) => safe(moodMix(m), null))),
    safe(dz('/chart/0/tracks', { limit: 30 }, 3600e3).then((r) => r.data.map(dzTrack).filter(Boolean)), []),
  ]);
  const mixes = [];
  if (favorites.length) mixes.push({ id: 'favorites', title: 'Твои любимые треки', cover: favorites.find((t) => t.coverLarge || t.cover)?.coverLarge || favorites[0]?.cover || null, count: favorites.length, tracks: favorites });
  for (const m of moods) if (m && m.tracks.length) mixes.push(m);
  if (advanced && wave.length) {
    const deep = [...wave].sort((a, b) => (a.rank || 0) - (b.rank || 0)).slice(0, 40);
    mixes.unshift({ id: 'aura-blend', title: 'Aura Blend', aura: true, cover: wave[0]?.coverLarge || null, count: Math.min(60, wave.length), tracks: spread(uniq([...wave.slice(0, 30), ...moods.filter(Boolean).flatMap((m) => m.tracks.slice(0, 8))])).slice(0, 60) });
    mixes.push({ id: 'deep-cuts', title: 'Глубже', aura: true, cover: deep[0]?.coverLarge || null, count: deep.length, tracks: deep });
  }
  const friends = all('SELECT u.* FROM follows f JOIN follows g ON g.follower=f.followee AND g.followee=f.follower JOIN users u ON u.id=f.followee WHERE f.follower=? ORDER BY u.last_seen DESC LIMIT 20', user.id)
    .map((x) => ({ ...publicUser(x, true), nowPlaying: nowPlayingOf(x) })).filter((x) => x.nowPlaying);
  return { wave, mixes, recent: recentTracks(user.id, 20), popular: chart, catalogNew: all('SELECT * FROM tracks ORDER BY id DESC LIMIT 20').map(slTrack), friends, tasteArtists: J(user.taste_artists, []) };
});
route('POST', '/api/queue/smart', async (req) => {
  const { user } = auth(req); const b = await readJSON(req);
  const seed = sanitizeTrack(b.seed); const recent = new Set((Array.isArray(b.recent) ? b.recent : []).map(String).slice(0, 300));
  const count = clamp(Number(b.count) || 10, 1, 30);
  let pool = [];
  try {
    if (seed?.id.startsWith('sc:') && scReady()) { const r = await scApi(`/tracks/${encodeURIComponent(seed.id.slice(3))}/related`, { limit: 30, access: 'playable' }, 3600e3); pool.push(...(r?.collection || r || []).map(scTrack).filter(Boolean)); }
    let aid = seed?.artistId?.startsWith('dz:') ? Number(seed.artistId.slice(3)) : null;
    if (!aid && seed?.artist) aid = await safe(dz('/search/artist', { q: primaryArtist(seed.artist), limit: 1 }, 7 * DAY).then((r) => r.data?.[0]?.id), null);
    if (aid) {
      const [top, rel] = await Promise.all([safe(dz(`/artist/${aid}/top`, { limit: 10 }, 6 * 3600e3), {}), safe(dz(`/artist/${aid}/related`, { limit: 8 }, 24 * 3600e3), {})]);
      const relTop = await Promise.all((rel.data || []).map((a) => safe(dz(`/artist/${a.id}/top`, { limit: 4 }, 6 * 3600e3), {}).then((r) => r.data || [])));
      pool.push(...[...(top.data || []).slice(0, 4), ...relTop.flat()].map(dzTrack).filter(Boolean));
    }
    if (pool.length < count) pool.push(...(await safe(buildWave(user, false), [])));
  } catch (e) { log('[smart]', e.message); }
  const seen = new Set([...recent]); if (seed) seen.add(seed.id);
  const keys = new Set(seed ? [trackKey(seed)] : []);
  const out = [];
  for (const t of spread(pool)) { if (seen.has(t.id) || keys.has(trackKey(t))) continue; seen.add(t.id); keys.add(trackKey(t)); out.push(t); if (out.length >= count) break; }
  return out;
});

/* Плейлисты по текстовому описанию */
const PROMPT_WORDS = [
  [/спокой|расслаб|chill|relax|уют/i, 'chill'], [/груст|печал|sad|меланх/i, 'sad'], [/весел|радост|happy|позитив/i, 'happy'], [/энерг|драйв|energy|бодр/i, 'energy'],
  [/трениров|спорт|зал|gym|workout|бег/i, 'workout'], [/сон|засып|sleep/i, 'sleep'], [/ноч|night/i, 'night'], [/утр|morning/i, 'morning'], [/дожд|rain/i, 'rainy'],
  [/любов|романт|love/i, 'love'], [/вечерин|туса|party/i, 'party'], [/фокус|учеб|работ|focus|study/i, 'focus'], [/дорог|поездк|машин|drive|road/i, 'road trip'],
  [/лет[оа]|summer/i, 'summer'], [/зим|winter/i, 'winter'], [/рэп|хип.?хоп|rap|hip.?hop/i, 'rap'], [/рок|rock/i, 'rock'], [/поп\b|pop/i, 'pop'], [/джаз|jazz/i, 'jazz'],
  [/электрон|edm|electronic/i, 'electronic'], [/техно|techno/i, 'techno'], [/хаус|house/i, 'house'], [/фонк|phonk/i, 'phonk'], [/lo.?fi|лоу.?фай/i, 'lofi'],
  [/класси|classical/i, 'classical'], [/инди|indie/i, 'indie'], [/метал|metal/i, 'metal'], [/панк|punk/i, 'punk'], [/r&b|рнб|rnb/i, 'rnb'], [/соул|soul/i, 'soul'],
  [/дрилл|drill/i, 'drill'], [/трэп|trap/i, 'trap'], [/80|восьмидесят/i, '80s'], [/90|девяност/i, '90s'], [/нулев|2000/i, '2000s'], [/русск|russian/i, 'russian'], [/k.?pop|кей.?поп/i, 'kpop'],
];
route('POST', '/api/playlists/generate', async (req) => {
  const { user } = auth(req); rateLimit(req, 'gen', 30, 600e3);
  const b = await readJSON(req); const prompt = String(b.prompt || '').trim().slice(0, 200);
  if (prompt.length < 3) fail(400, 'bad_prompt', 'Опишите плейлист хотя бы парой слов');
  const lim = limitsOf(user.id).textPlaylistsPerDay;
  if (lim != null && one("SELECT COUNT(*) c FROM generations WHERE user_id=? AND kind='text' AND created_at>?", user.id, now() - DAY).c >= lim) fail(402, 'aura_required', `Бесплатно — ${lim} плейлиста в сутки. С Aura — без ограничений`);
  const words = PROMPT_WORDS.filter(([re]) => re.test(prompt)).map(([, w]) => w);
  const like = /(?:как|в стиле|похож(?:ее|ие)? на|типа|like)\s+([^,.;]+)/i.exec(prompt)?.[1]?.trim();
  const query = words.slice(0, 3).join(' ') || prompt;
  const size = can(user.id, 'playlists.text.unlimited') ? 50 : 30;
  const [pls, trs, art] = await Promise.all([
    safe(dz('/search/playlist', { q: query, limit: 4 }, 6 * 3600e3), {}),
    safe(dz('/search', { q: query, limit: 25 }, 600000), {}),
    like ? safe(dz('/search/artist', { q: like, limit: 1 }, DAY).then((r) => r.data?.[0]), null) : null,
  ]);
  const fromPl = (await Promise.all((pls.data || []).slice(0, 2).map((p) => safe(dz(`/playlist/${p.id}/tracks`, { limit: 40 }, 6 * 3600e3), {}).then((r) => r.data || [])))).flat();
  let fromArtist = [];
  if (art) { const rel = await safe(dz(`/artist/${art.id}/related`, { limit: 6 }, DAY), {}); fromArtist = (await Promise.all([art, ...(rel.data || [])].map((a) => safe(dz(`/artist/${a.id}/top`, { limit: 4 }, 6 * 3600e3), {}).then((r) => r.data || [])))).flat(); }
  const tracks = spread(uniq([...fromArtist, ...fromPl, ...(trs.data || [])].map(dzTrack).filter(Boolean))).slice(0, size);
  if (!tracks.length) fail(404, 'nothing_found', 'Не нашлось подходящих треков — попробуйте описать иначе');
  const name = prompt.charAt(0).toUpperCase() + prompt.slice(1, 60);
  const r = run('INSERT INTO playlists(user_id,name,description,public,created_at,updated_at) VALUES(?,?,?,?,?,?)', user.id, name, `Собрано по описанию: «${prompt}»`, 0, now(), now());
  const pid = Number(r.lastInsertRowid);
  tx(() => tracks.forEach((t, i) => run('INSERT INTO playlist_tracks(playlist_id,pos,track_id,data,added_at) VALUES(?,?,?,?,?)', pid, i, t.id, JSON.stringify(sanitizeTrack(t)), now())));
  run("INSERT INTO generations(user_id,kind,created_at) VALUES(?, 'text', ?)", user.id, now());
  logEvent(user.id, 'playlist');
  return playlistOut(one('SELECT * FROM playlists WHERE id=?', pid), true);
});

/* ───────────────────────────── Комнаты совместного прослушивания (SSE) ───────────────────────────── */
const rooms = new Map();
const roomCode = () => { let c; do { c = Array.from({ length: 6 }, () => 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'[crypto.randomInt(0, 32)]).join(''); } while (rooms.has(c)); return c; };
function roomOut(r) {
  return { code: r.code, name: r.name, hostId: r.hostId, max: r.max, allowControl: r.allowControl, state: r.state, queue: r.queue, chat: r.chat.slice(-40),
    members: [...r.members.keys()].map((id) => { const u = one('SELECT * FROM users WHERE id=?', id); return u && { ...publicUser(u, true), host: id === r.hostId, live: [...r.streams].some((s) => s.uid === id) }; }).filter(Boolean) };
}
function roomEmit(r, type, data) { r.touched = now(); const msg = `event: ${type}\ndata: ${JSON.stringify(data)}\n\n`; for (const s of r.streams) { try { s.res.write(msg); } catch {} } }
function getRoom(code) { const r = rooms.get(String(code || '').toUpperCase()); if (!r) fail(404, 'room_not_found', 'Комната не найдена или закрыта'); return r; }
function leaveRoom(r, uid) {
  r.members.delete(uid);
  if (r.hostId === uid) { const next = [...r.members.keys()][0]; if (next) r.hostId = next; else { roomEmit(r, 'closed', {}); rooms.delete(r.code); return; } }
  roomEmit(r, 'room', roomOut(r));
}
route('POST', '/api/rooms', async (req) => {
  const { user } = auth(req); const b = await readJSON(req);
  for (const r of rooms.values()) if (r.hostId === user.id) leaveRoom(r, user.id);
  const r = { code: roomCode(), name: String(b.name || `Комната ${user.name}`).slice(0, 60), hostId: user.id, max: limitsOf(user.id).roomSize, allowControl: false, members: new Map([[user.id, now()]]), streams: new Set(), state: null, queue: [], chat: [], touched: now() };
  rooms.set(r.code, r); logEvent(user.id, 'room_host');
  return roomOut(r);
});
route('GET', '/api/rooms/:code', (req, { params }) => { auth(req); return roomOut(getRoom(params.code)); });
route('POST', '/api/rooms/:code/join', (req, { params }) => {
  const { user } = auth(req); const r = getRoom(params.code);
  if (!r.members.has(user.id)) {
    if (r.members.size >= r.max) fail(409, 'room_full', `Комната заполнена: максимум ${r.max} участников`);
    r.members.set(user.id, now()); roomEmit(r, 'room', roomOut(r));
  }
  return roomOut(r);
});
route('POST', '/api/rooms/:code/leave', (req, { params }) => { const { user } = auth(req); const r = rooms.get(String(params.code).toUpperCase()); if (r) leaveRoom(r, user.id); return { ok: true }; });
route('PATCH', '/api/rooms/:code', async (req, { params }) => {
  const { user } = auth(req); const r = getRoom(params.code); if (r.hostId !== user.id) fail(403, 'not_host', 'Только ведущий может менять настройки');
  const b = await readJSON(req); if (b.allowControl !== undefined) r.allowControl = !!b.allowControl; if (b.name) r.name = String(b.name).slice(0, 60);
  roomEmit(r, 'room', roomOut(r)); return roomOut(r);
});
route('GET', '/api/rooms/:code/events', (req, { params, res }) => {
  const { user } = auth(req); const r = getRoom(params.code);
  if (!r.members.has(user.id)) fail(403, 'not_member', 'Сначала присоединитесь к комнате');
  res.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache, no-transform', Connection: 'keep-alive', 'X-Accel-Buffering': 'no', ...CORS });
  const s = { res, uid: user.id }; r.streams.add(s);
  res.write(`event: room\ndata: ${JSON.stringify(roomOut(r))}\n\n`);
  roomEmit(r, 'room', roomOut(r));
  const ping = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 20000);
  req.on('close', () => {
    clearInterval(ping); r.streams.delete(s);
    setTimeout(() => { if (rooms.has(r.code) && ![...r.streams].some((x) => x.uid === user.id) && r.members.has(user.id)) leaveRoom(r, user.id); }, 45000);
  });
  return undefined;
});
route('POST', '/api/rooms/:code/state', async (req, { params }) => {
  const { user } = auth(req); const r = getRoom(params.code);
  if (r.hostId !== user.id && !(r.allowControl && r.members.has(user.id))) fail(403, 'not_host', 'Управлять может только ведущий');
  const b = await readJSON(req);
  r.state = { track: b.track ? sanitizeTrack(b.track) : null, position: Math.max(0, Number(b.position) || 0), playing: !!b.playing, at: now(), by: user.id };
  roomEmit(r, 'state', r.state); return { ok: true };
});
route('POST', '/api/rooms/:code/queue', async (req, { params }) => {
  const { user } = auth(req); const r = getRoom(params.code); if (!r.members.has(user.id)) fail(403, 'not_member');
  const t = sanitizeTrack((await readJSON(req)).track); if (!t) fail(400, 'bad_track');
  if (r.queue.length >= 100) fail(409, 'queue_full', 'Очередь комнаты заполнена');
  r.queue.push({ ...t, addedBy: user.id, addedByName: user.name }); roomEmit(r, 'queue', r.queue); return { ok: true };
});
route('DELETE', '/api/rooms/:code/queue/:i', (req, { params }) => {
  const { user } = auth(req); const r = getRoom(params.code); if (r.hostId !== user.id) fail(403, 'not_host');
  r.queue.splice(Number(params.i), 1); roomEmit(r, 'queue', r.queue); return { ok: true };
});
route('POST', '/api/rooms/:code/chat', async (req, { params }) => {
  const { user } = auth(req); const r = getRoom(params.code); if (!r.members.has(user.id)) fail(403, 'not_member');
  rateLimit(req, 'chat', 30, 60e3);
  const text = String((await readJSON(req)).text || '').trim().slice(0, 300); if (!text) fail(400, 'empty');
  const m = { id: token(6), user: publicUser(user, true), text, at: now() }; r.chat.push(m); if (r.chat.length > 100) r.chat.shift();
  roomEmit(r, 'chat', m); return { ok: true };
});
setInterval(() => { for (const r of rooms.values()) if (!r.streams.size && now() - r.touched > 15 * 60e3) rooms.delete(r.code); }, 60000).unref();

/* ───────────────────────────── Офлайн-библиотека: учёт квоты на сервере ───────────────────────────── */
const deviceOf = (req, b) => String(b?.device || req.headers['x-device-id'] || 'default').slice(0, 64);
route('GET', '/api/offline', (req, { query }) => {
  const { user } = auth(req); const device = String(query.get('device') || 'default').slice(0, 64);
  const used = one('SELECT COALESCE(SUM(bytes),0) s FROM offline_items WHERE user_id=? AND device=?', user.id, device).s;
  return { used, limit: limitsOf(user.id).offlineBytes, count: one('SELECT COUNT(*) c FROM offline_items WHERE user_id=? AND device=?', user.id, device).c };
});
route('POST', '/api/offline/register', async (req) => {
  const { user } = auth(req); const b = await readJSON(req); const device = deviceOf(req, b);
  const id = String(b.trackId || '').slice(0, 160); const bytes = Math.max(0, Math.round(Number(b.bytes) || 0));
  if (!id) fail(400, 'bad_track');
  const used = one('SELECT COALESCE(SUM(bytes),0) s FROM offline_items WHERE user_id=? AND device=? AND track_id<>?', user.id, device, id).s;
  const limit = limitsOf(user.id).offlineBytes;
  if (used + bytes > limit) fail(402, 'offline_limit', `Офлайн-хранилище заполнено (${Math.round(limit / GB)} ГБ). С Aura — до 10 ГБ`);
  run('INSERT OR REPLACE INTO offline_items(user_id,device,track_id,bytes,created_at) VALUES(?,?,?,?,?)', user.id, device, id, bytes, now());
  return { used: used + bytes, limit };
});
route('POST', '/api/offline/unregister', async (req) => {
  const { user } = auth(req); const b = await readJSON(req); const device = deviceOf(req, b);
  if (b.all) run('DELETE FROM offline_items WHERE user_id=? AND device=?', user.id, device);
  else run('DELETE FROM offline_items WHERE user_id=? AND device=? AND track_id=?', user.id, device, String(b.trackId || ''));
  return { ok: true };
});

/* ───────────────────────────── Ранний доступ (Aura): сборки с пометкой «early» в GitHub Releases ───────────────────────────── */
const GH_REPO = (process.env.GITHUB_REPO || CFG.downloadUrl.replace(/^https:\/\/github\.com\//, '')).replace(/\/+$/, '');
route('GET', '/api/updates/early', async (req) => {
  const { user } = auth(req); requireEnt(user.id, 'early_access', 'Ранний доступ к новым версиям — с подпиской Aura');
  if (!/^[\w.-]+\/[\w.-]+$/.test(GH_REPO)) return null;
  const key = 'gh:early'; const c = cacheGet(key); if (c !== undefined) return c;
  const list = await fetchJSON(`https://api.github.com/repos/${GH_REPO}/releases?per_page=15`, { timeout: 12000, headers: { accept: 'application/vnd.github+json' } }).catch(() => []);
  const r = (Array.isArray(list) ? list : []).find((x) => !x.draft && /early/i.test(`${x.tag_name} ${x.name}`));
  const asset = (re) => r?.assets?.find((a) => re.test(a.name));
  const out = r ? { version: r.tag_name.replace(/^v/i, ''), early: true, notes: String(r.body || '').split(/\r?\n/).map((l) => l.replace(/^[-*]\s*/, '').trim()).filter(Boolean).slice(0, 12), date: r.published_at,
    windows: asset(/\.exe$/i) ? { url: asset(/\.exe$/i).browser_download_url, size: asset(/\.exe$/i).size } : null, android: asset(/\.apk$/i) ? { url: asset(/\.apk$/i).browser_download_url, size: asset(/\.apk$/i).size } : null } : null;
  cacheSet(key, out, 10 * 60e3); return out;
});

/* ───────────────────────────── Синхронизация между устройствами (Aura) ───────────────────────────── */
route('GET', '/api/sync', (req) => { const { user } = auth(req); requireEnt(user.id, 'sync', 'Синхронизация между устройствами доступна с Aura'); return J(user.sync_state, {}); });
route('PUT', '/api/sync', async (req) => {
  const { user } = auth(req); requireEnt(user.id, 'sync', 'Синхронизация между устройствами доступна с Aura');
  const b = await readJSON(req); const cur = J(user.sync_state, {});
  const next = { ...cur, updatedAt: now(), device: String(b.device || '').slice(0, 80) };
  if (b.settings && typeof b.settings === 'object') next.settings = b.settings;
  if (b.playback && typeof b.playback === 'object') next.playback = { track: sanitizeTrack(b.playback.track), queue: (Array.isArray(b.playback.queue) ? b.playback.queue : []).slice(0, 200).map(sanitizeTrack).filter(Boolean), index: Number(b.playback.index) || 0, position: Number(b.playback.position) || 0, at: now(), device: next.device };
  const str = JSON.stringify(next); if (str.length > 200000) fail(413, 'too_large');
  run('UPDATE users SET sync_state=? WHERE id=?', str, user.id); return { ok: true, updatedAt: next.updatedAt };
});

/* ───────────────────────────── Расширенная статистика и достижения ───────────────────────────── */
function streaks(dayKeys) {
  const days = [...new Set(dayKeys)].sort((a, b) => a - b); let best = 0; let cur = 0; let prev = null;
  for (const d of days) { cur = prev != null && d === prev + 1 ? cur + 1 : 1; best = Math.max(best, cur); prev = d; }
  const today = Math.floor(now() / DAY); const last = days[days.length - 1];
  return { best, current: last != null && today - last <= 1 ? cur : 0 };
}
function userFacts(uid, tz) {
  const rows = all('SELECT played_at, listened, artist_key, data FROM plays WHERE user_id=?', uid);
  const dayKey = (t) => Math.floor((t + tz) / DAY);
  const perDay = new Map(); const genres = new Set(); let night = 0;
  for (const r of rows) { const k = dayKey(r.played_at); perDay.set(k, (perDay.get(k) || 0) + (r.listened || 0)); const h = new Date(r.played_at + tz).getUTCHours(); if (h < 5) night++; const g = J(r.data, {}).genre; if (g) genres.add(lower(g)); }
  const st = streaks([...perDay.keys()]);
  return {
    plays: rows.length, artists: new Set(rows.map((r) => r.artist_key).filter(Boolean)).size, nightPlays: night, genres: genres.size,
    likes: one('SELECT COUNT(*) c FROM likes WHERE user_id=?', uid).c, friends: friendsCount(uid), playlists: one('SELECT COUNT(*) c FROM playlists WHERE user_id=?', uid).c,
    roomsHosted: one("SELECT COUNT(*) c FROM events WHERE user_id=? AND kind='room_host'", uid).c, maxDaySeconds: Math.max(0, ...perDay.values()), bestStreak: st.best, currentStreak: st.current,
  };
}
const ACHIEVEMENTS = [
  { id: 'first', title: 'Первый трек', desc: 'Послушать первый трек', target: 1, v: (f) => f.plays },
  { id: 'p100', title: 'Сотня', desc: '100 прослушиваний', target: 100, v: (f) => f.plays },
  { id: 'p1000', title: 'Меломан', desc: '1000 прослушиваний', target: 1000, v: (f) => f.plays },
  { id: 'explorer', title: 'Исследователь', desc: '25 разных артистов', target: 25, v: (f) => f.artists },
  { id: 'night', title: 'Ночная смена', desc: 'Слушать музыку с 0:00 до 5:00', target: 1, v: (f) => f.nightPlays },
  { id: 'likes50', title: 'Коллекционер', desc: '50 любимых треков', target: 50, v: (f) => f.likes },
  { id: 'social', title: 'Своя компания', desc: '3 друга', target: 3, v: (f) => f.friends },
  { id: 'curator', title: 'Куратор', desc: 'Создать плейлист', target: 1, v: (f) => f.playlists },
  { id: 'marathon', ext: true, title: 'Марафон', desc: '3 часа музыки за день', target: 10800, v: (f) => f.maxDaySeconds },
  { id: 'streak7', ext: true, title: 'Неделя в ритме', desc: '7 дней подряд', target: 7, v: (f) => f.bestStreak },
  { id: 'streak30', ext: true, title: 'Месяц в ритме', desc: '30 дней подряд', target: 30, v: (f) => f.bestStreak },
  { id: 'genres10', ext: true, title: 'Без границ', desc: '10 разных жанров', target: 10, v: (f) => f.genres },
  { id: 'host', ext: true, title: 'Ведущий', desc: 'Провести комнату совместного прослушивания', target: 1, v: (f) => f.roomsHosted },
  { id: 'p10000', ext: true, title: 'Легенда', desc: '10 000 прослушиваний', target: 10000, v: (f) => f.plays },
];
function achievementsOf(uid, tz = 0) {
  const f = userFacts(uid, tz); const ext = can(uid, 'achievements.extended');
  return ACHIEVEMENTS.map((a) => { const value = Math.min(a.target, a.v(f)); return { id: a.id, title: a.title, desc: a.desc, ext: !!a.ext, locked: !!a.ext && !ext, target: a.target, value, unlocked: (!a.ext || ext) && value >= a.target }; });
}
route('GET', '/api/me/achievements', (req, { query }) => { const { user } = auth(req); return achievementsOf(user.id, clamp(Number(query.get('tz')) || 0, -900, 900) * 60000); });
route('GET', '/api/me/stats', (req, { query }) => {
  const { user } = auth(req); const tz = clamp(Number(query.get('tz')) || 0, -900, 900) * 60000;
  const detailed = can(user.id, 'stats.detailed');
  const range = clamp(Number(query.get('range')) || 30, 7, limitsOf(user.id).statsDays);
  const start = Math.floor((now() + tz) / DAY) * DAY - tz - (range - 1) * DAY;
  const rows = all('SELECT played_at, listened, artist_key, artist, track_id, source, data FROM plays WHERE user_id=? AND played_at>=?', user.id, start);
  const days = Array.from({ length: range }, (_, i) => ({ date: start + i * DAY, plays: 0, seconds: 0 }));
  const hours = Array(24).fill(0); const heat = detailed ? Array.from({ length: 7 }, () => Array(24).fill(0)) : null;
  const genres = new Map(); const sources = new Map(); const artistsIn = new Set();
  for (const r of rows) {
    const i = Math.floor((r.played_at - start) / DAY); if (days[i]) { days[i].plays++; days[i].seconds += r.listened || 0; }
    const d = new Date(r.played_at + tz); hours[d.getUTCHours()]++; if (heat) heat[(d.getUTCDay() + 6) % 7][d.getUTCHours()]++;
    const g = J(r.data, {}).genre; if (g) genres.set(g, (genres.get(g) || 0) + 1);
    sources.set(r.source || 'other', (sources.get(r.source || 'other') || 0) + 1); if (r.artist_key) artistsIn.add(r.artist_key);
  }
  const before = new Set(all('SELECT DISTINCT artist_key k FROM plays WHERE user_id=? AND played_at<?', user.id, start).map((r) => r.k));
  const tot = one('SELECT COUNT(*) c, SUM(listened) s, COUNT(DISTINCT artist_key) a, COUNT(DISTINCT track_id) t FROM plays WHERE user_id=?', user.id);
  const f = userFacts(user.id, tz);
  return {
    range, detailed, maxRange: limitsOf(user.id).statsDays,
    plays: tot.c, seconds: tot.s || 0, artists: tot.a, tracks: tot.t, level: levelOf(tot.c),
    period: { plays: rows.length, seconds: rows.reduce((s, r) => s + (r.listened || 0), 0), artists: artistsIn.size, avgPerDay: Math.round(rows.length / range * 10) / 10 },
    streak: { current: f.currentStreak, best: f.bestStreak }, days, hours, peakHour: hours.indexOf(Math.max(...hours)),
    topArtists: topArtists(user.id, start, 10), topTracks: topTracks(user.id, start, 10),
    heatmap: heat, genres: detailed ? [...genres.entries()].sort((a, b) => b[1] - a[1]).slice(0, 8).map(([name, n]) => ({ name, plays: n })) : null,
    sources: detailed ? [...sources.entries()].map(([name, n]) => ({ name, plays: n })) : null,
    newArtists: detailed ? [...artistsIn].filter((k) => !before.has(k)).length : null,
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
      if (!res.headersSent && out !== undefined) {
        if (req.method === 'GET') {
          const body = JSON.stringify(out); const etag = `W/"${sha1(body).slice(0, 27)}"`;
          if (req.headers['if-none-match'] === etag) { res.writeHead(304, { ETag: etag, 'Cache-Control': 'private, no-cache', ...CORS }); res.end(); }
          else { res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'private, no-cache', ETag: etag, ...CORS }); res.end(body); }
        } else send(res, 200, out);
      }
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
    text: `<b>${em('bot')} Soundless · Панель управления</b>\n\n${em('people')} Пользователи: <b>${fmtNum(users)}</b> · онлайн <b>${fmtNum(online)}</b>\n${em('box')} Каталог: <b>${fmtNum(tracks)}</b> ${plural(tracks, 'трек', 'трека', 'треков')}\n${em('growth')} За сутки: <b>${fmtNum(today)}</b> ${plural(today, 'прослушивание', 'прослушивания', 'прослушиваний')}\n${em('gift')} Aura: <b>${fmtNum(auraCount())}</b> активных\n\n<i>Отправьте аудиофайл в этот чат, чтобы добавить его в каталог.</i>`,
    kb: KB([B('Статистика', 'a:stats', 'stats'), B('Пользователи', 'a:users:0', 'people')], [B('Каталог', 'a:cat', 'box'), B('Aura', 'a:aura:0', 'gift')], [B('Рассылка', 'a:bc', 'megaphone'), B('Настройки', 'a:set', 'settings')]),
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
  const bots = c('SELECT COUNT(*) c FROM bot_users WHERE blocked=0'); const aura = auraCount(); const google = c('SELECT COUNT(*) c FROM users WHERE google_sub IS NOT NULL');
  return {
    text: `<b>${em('stats')} Статистика</b>\n\n${em('people')} <b>Пользователи</b>\n├ Всего: <b>${fmtNum(users)}</b>\n├ Новых: <b>${fmtNum(d1)}</b> за сутки · <b>${fmtNum(d7)}</b> за неделю\n├ Онлайн: <b>${fmtNum(online)}</b> · активных за сутки <b>${fmtNum(active)}</b>\n├ С Google: <b>${fmtNum(google)}</b>\n└ Aura: <b>${fmtNum(aura)}</b>\n\n${em('growth')} <b>Прослушивания</b>\n├ Сутки: <b>${fmtNum(p1)}</b> · неделя <b>${fmtNum(p7)}</b>\n└ Всего: <b>${fmtNum(pt.c)}</b> · ${fmtNum(Math.round((pt.s || 0) / 3600))} ч\n\n${em('box')} <b>Каталог</b>\n└ ${fmtNum(cat.c)} ${plural(cat.c, 'трек', 'трека', 'треков')} · ${fmtSize(cat.s || 0)}` +
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
    text: `<b>${em('profile')} ${esc(u.name)}</b>${u.verified ? ' ' + em('check') : ''}\n@${esc(u.username)} · ID <code>${u.id}</code>\n\n${em('calendar')} Регистрация: ${fmtDate(u.created_at)}\n${em('clock')} ${isOnline(u) ? '<b>В сети</b>' : 'Был(а): ' + (u.last_seen ? new Date(u.last_seen).toLocaleString('ru-RU') : '—')}\n${em('growth')} Уровень <b>${lv.level}</b> · ${fmtNum(plays)} ${plural(plays, 'прослушивание', 'прослушивания', 'прослушиваний')}\n${em('people')} Друзей: ${friendsCount(u.id)} · подписчиков: ${followersCount(u.id)}\n${em('lock')} Сессий: ${sess}\n${em('gift')} Aura: ${auraLine(u.id)}${u.google_email ? `\n${em('link')} Google: ${esc(u.google_email)}` : ''}` +
      (np ? `\n\n${em('eye')} Слушает: <b>${esc(np.track.artist)} — ${esc(np.track.title)}</b>` : '') + (u.banned ? `\n\n${em('lock')} <b>Аккаунт заблокирован</b>` : ''),
    kb: KB(
      [u.banned ? B('Разблокировать', 'a:ub:' + u.id, 'unlock', 'success') : B('Заблокировать', 'a:ub:' + u.id, 'lock', 'danger'), B(u.verified ? 'Снять галочку' : 'Верифицировать', 'a:uv:' + u.id, u.verified ? 'cross' : 'check')],
      [B('Сбросить пароль', 'a:ur:' + u.id, 'pencil'), B('Завершить сессии', 'a:us:' + u.id, 'timePassed')],
      [B('Подписка Aura', 'a:ua:' + u.id, 'gift', auraOf(u.id).active ? undefined : 'primary')],
      [B('Удалить аккаунт', 'a:ud:' + u.id, 'trash', 'danger')], [BACK('a:users:0')]),
  };
}
function auraCount() { return one('SELECT COUNT(*) c FROM subscriptions WHERE expires_at IS NULL OR expires_at>?', now()).c; }
function auraLine(uid) { const a = auraOf(uid); if (!a.active) return a.expiresAt ? `истекла ${fmtDate(a.expiresAt)}` : 'нет'; return a.expiresAt ? `<b>активна</b> до ${fmtDate(a.expiresAt)}` : '<b>активна</b> · бессрочно'; }
function vUserAura(id) {
  const u = one('SELECT * FROM users WHERE id=?', id); if (!u) return { text: `${em('cross')} Пользователь не найден`, kb: KB([BACK('a:aura:0')]) };
  const a = auraOf(u.id);
  return {
    text: `<b>${em('gift')} Soundless Aura</b>\n\n${em('profile')} ${esc(u.name)} · @${esc(u.username)}\nСтатус: ${auraLine(u.id)}\n\n<i>Продление добавляет срок к текущей дате окончания.</i>`,
    kb: KB([B('+30 дней', `a:ag:${u.id}:30`, 'calendar', 'success'), B('+90 дней', `a:ag:${u.id}:90`, 'calendar', 'success')], [B('+365 дней', `a:ag:${u.id}:365`, 'calendar', 'success'), B('Навсегда', `a:ag:${u.id}:0`, 'party', 'success')],
      a.active || a.expiresAt ? [B('Отозвать Aura', `a:ar:${u.id}`, 'cross', 'danger')] : null, [BACK('a:u:' + u.id)]),
  };
}
function vAura(page = 0) {
  const per = 8; const total = one('SELECT COUNT(*) c FROM subscriptions').c; const pages = Math.max(1, Math.ceil(total / per)); page = clamp(page, 0, pages - 1);
  const list = all('SELECT s.*, u.username, u.name FROM subscriptions s JOIN users u ON u.id=s.user_id ORDER BY (s.expires_at IS NULL) DESC, s.expires_at DESC LIMIT ? OFFSET ?', per, page * per);
  return {
    text: `<b>${em('gift')} Soundless Aura</b>\n\nАктивных подписок: <b>${fmtNum(auraCount())}</b> · всего записей: ${fmtNum(total)}\n\n${em('info')} Выдать подписку: откройте пользователя → «Подписка Aura».\n\n<b>Что даёт Aura:</b> бейдж с бликом, расширенный профиль, анимированные аватары и баннеры, продвинутые миксы, безлимит плейлистов по описанию, расширенное караоке, офлайн до 10 ГБ, синхронизация устройств, подробная статистика, комнаты до 10 человек, расширенные достижения, ранний доступ.`,
    kb: KB(...list.map((s) => [B(`${s.name} · @${s.username}${s.expires_at && s.expires_at < now() ? ' · истекла' : ''}`.slice(0, 60), 'a:ua:' + s.user_id, s.expires_at && s.expires_at < now() ? 'clock' : 'gift')]),
      pages > 1 ? [B('‹', `a:aura:${Math.max(0, page - 1)}`), B(`${page + 1} / ${pages}`, 'a:noop'), B('›', `a:aura:${Math.min(pages - 1, page + 1)}`)] : null,
      [B('Найти пользователя', 'a:usearch', 'eye'), BACK('a:main')]),
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
  computeWaveform(dest).then((w) => w && run('UPDATE tracks SET waveform=? WHERE id=?', JSON.stringify(w), id)).catch(() => {});
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
        case 'aura': await bot.show(chat, vAura(+x || 0), mid); break;
        case 'ua': await bot.show(chat, vUserAura(+x), mid); break;
        case 'ag': {
          const days = Number(y); const cur = auraOf(+x); const base = cur.active && cur.expiresAt ? cur.expiresAt : now();
          const exp = days === 0 ? null : base + days * DAY;
          run('INSERT INTO subscriptions(user_id,plan,started_at,expires_at,granted_by,note) VALUES(?,?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET expires_at=excluded.expires_at, granted_by=excluded.granted_by, started_at=CASE WHEN subscriptions.expires_at IS NOT NULL AND subscriptions.expires_at<' + now() + ' THEN excluded.started_at ELSE subscriptions.started_at END', +x, 'aura', now(), exp, from, days ? `+${days}d` : 'forever');
          toast = days ? `Aura продлена на ${days} дн.` : 'Aura выдана навсегда'; await bot.show(chat, vUserAura(+x), mid); break;
        }
        case 'ar': run('UPDATE subscriptions SET expires_at=? WHERE user_id=?', now() - 1000, +x); toast = 'Aura отозвана'; await bot.show(chat, vUserAura(+x), mid); break;
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
    } else {
      await bot.api('answerCallbackQuery', { callback_query_id: cq.id, text: 'Доступ только для администратора', show_alert: true }); return;
    }
  } catch (e) { log('[bot cb]', e.message); toast = 'Ошибка: ' + e.message.slice(0, 150); alert = true; }
  await bot.api('answerCallbackQuery', { callback_query_id: cq.id, text: toast || undefined, show_alert: alert }).catch(() => {});
}

async function onMessage(msg) {
  const chat = msg.chat.id; if (msg.chat.type !== 'private') return;
  run('INSERT INTO bot_users(chat_id,username,first_name,created_at) VALUES(?,?,?,?) ON CONFLICT(chat_id) DO UPDATE SET username=excluded.username, first_name=excluded.first_name, blocked=0', chat, msg.from?.username || null, msg.from?.first_name || null, now());
  const text = msg.text || ''; const admin = isAdmin(chat);
  if (!admin) { if (text.startsWith('/start')) await bot.send(chat, `${em('lock')} <b>Soundless</b>\n\nЭтот бот — служебная панель администратора и недоступен для других пользователей.`).catch(() => {}); return; }
  if (text.startsWith('/start') || text === '/admin' || text === 'Панель') {
    bot.clearState(chat);
    if (text.startsWith('/start')) await bot.send(chat, `${em('settings')} Панель администратора включена`, { keyboard: [[{ text: 'Панель', icon_custom_emoji_id: EMOJI.settings[0] }, { text: 'Статистика', icon_custom_emoji_id: EMOJI.stats[0] }, { text: 'Каталог', icon_custom_emoji_id: EMOJI.box[0] }]], resize_keyboard: true, is_persistent: true }).catch(() => {});
    return bot.show(chat, vMain());
  }
  if (text === 'Статистика') { bot.clearState(chat); return bot.show(chat, vStats()); }
  if (text === 'Каталог') { bot.clearState(chat); return bot.show(chat, vCatalog()); }
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
  return bot.show(chat, vMain());
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
