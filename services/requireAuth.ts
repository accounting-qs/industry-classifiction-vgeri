/**
 * One shared password for the whole app.
 *
 * APP_PASSWORD (server env) is the password. Signing in sets an httpOnly
 * cookie that lasts 90 days and is renewed while the browser keeps using
 * the app, so people are only asked again after months away. The cookie's
 * signature is keyed on APP_PASSWORD, so changing the password signs every
 * browser out.
 *
 * Everything sits behind the gate except GET /api/health and the sign-in
 * page itself, the built JS included: the bundle (and the Supabase anon
 * key inside it) is only served to people who know the password. The
 * sign-in page lives under /api/ so the Vite dev proxy forwards it too.
 */

import crypto from 'crypto';
import type { Request, RequestHandler, Response } from 'express';

const COOKIE_NAME = 'qs_auth';
const SESSION_MS = 90 * 24 * 60 * 60 * 1000;
// Re-issue the cookie once it is a month old, so active users never reach expiry.
const RENEW_AFTER_MS = 30 * 24 * 60 * 60 * 1000;
// Wrong passwords allowed per IP per window, to slow down guessing.
const MAX_FAILURES = 10;
const FAILURE_WINDOW_MS = 15 * 60 * 1000;

export const LOGIN_PATH = '/api/login';

// Random per-process secret for the server's own loopback calls to /api
// (the import worker registers lists through POST /api/import-lists).
// It is never sent anywhere but 127.0.0.1 and changes on every restart.
export const INTERNAL_API_TOKEN = crypto.randomBytes(32).toString('hex');
export const INTERNAL_API_HEADER = 'x-internal-token';

const sha256 = (s: string) => crypto.createHash('sha256').update(s).digest();

function safeEqual(a: Buffer, b: Buffer): boolean {
    return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function readCookie(header: string | undefined, name: string): string | null {
    for (const part of (header ?? '').split(';')) {
        const eq = part.indexOf('=');
        // No decoding needed: our value only uses [0-9A-Za-z._-].
        if (eq !== -1 && part.slice(0, eq).trim() === name) return part.slice(eq + 1).trim();
    }
    return null;
}

// Only same-site paths, so the sign-in form can't be used as an open redirect.
function safeNext(next: unknown): string {
    if (typeof next !== 'string' || !next.startsWith('/') || next.startsWith('//') || next.startsWith('/\\')) return '/';
    return next.startsWith(LOGIN_PATH) ? '/' : next;
}

const escapeHtml = (s: string) => s.replace(/[&<>"']/g, c => `&#${c.charCodeAt(0)};`);

export function createPasswordGate(password: string | undefined) {
    if (!password) {
        console.error('❌ CRITICAL: APP_PASSWORD is not set. Nobody can sign in until it is.');
    }
    const key = password ? sha256(`qs-app-session:${password}`) : null;
    const failures = new Map<string, { count: number; resetAt: number }>();

    const sign = (exp: number) => crypto.createHmac('sha256', key!).update(`v1.${exp}`).digest('base64url');

    const issueCookie = (req: Request, res: Response) => {
        const exp = Date.now() + SESSION_MS;
        res.cookie(COOKIE_NAME, `v1.${exp}.${sign(exp)}`, {
            httpOnly: true,
            secure: req.secure,
            sameSite: 'lax',
            maxAge: SESSION_MS,
            path: '/',
        });
    };

    // Expiry of a valid sign-in cookie, or null.
    const sessionExpiry = (req: Request): number | null => {
        if (!key) return null;
        const m = readCookie(req.headers.cookie, COOKIE_NAME)?.match(/^v1\.(\d+)\.([\w-]+)$/);
        if (!m) return null;
        const exp = Number(m[1]);
        if (exp < Date.now()) return null;
        return safeEqual(Buffer.from(m[2]), Buffer.from(sign(exp))) ? exp : null;
    };

    const sendLoginPage = (res: Response, status: number, next: string, error: string | null) => {
        res.status(status).type('html').set('Cache-Control', 'no-store').send(loginPageHtml({
            next,
            error: key ? error : 'Sign-in is not set up yet: APP_PASSWORD is missing on the server.',
        }));
    };

    const requireAuth: RequestHandler = (req, res, next) => {
        if (safeEqual(Buffer.from(req.get(INTERNAL_API_HEADER) ?? ''), Buffer.from(INTERNAL_API_TOKEN))) {
            return next();
        }
        const exp = sessionExpiry(req);
        if (exp) {
            if (exp - Date.now() < SESSION_MS - RENEW_AFTER_MS) issueCookie(req, res);
            return next();
        }
        if (req.path.startsWith('/api/')) return res.status(401).json({ error: 'Not signed in' });
        // Pages show the sign-in form in place, at the URL that was asked for.
        sendLoginPage(res, 401, req.originalUrl, null);
    };

    // GET /api/login: the UI sends the browser here when an /api call gets 401.
    const loginPage: RequestHandler = (req, res) => {
        const next = safeNext(req.query.next);
        if (sessionExpiry(req)) return res.redirect(303, next);
        sendLoginPage(res, 200, next, null);
    };

    // POST /api/login (form body: password, next).
    const login: RequestHandler = (req, res) => {
        const next = safeNext(req.body?.next);
        const ip = req.ip || 'unknown';
        const now = Date.now();
        const record = failures.get(ip);
        if (record && record.resetAt > now && record.count >= MAX_FAILURES) {
            return sendLoginPage(res, 429, next, 'Too many wrong passwords. Try again in 15 minutes.');
        }

        const given = typeof req.body?.password === 'string' ? req.body.password : '';
        if (!password || !safeEqual(sha256(given), sha256(password))) {
            if (failures.size > 1000) {
                for (const [k, v] of failures) if (v.resetAt <= now) failures.delete(k);
            }
            const current = record && record.resetAt > now ? record : { count: 0, resetAt: now + FAILURE_WINDOW_MS };
            current.count++;
            failures.set(ip, current);
            return sendLoginPage(res, 401, next, 'Wrong password.');
        }

        failures.delete(ip);
        issueCookie(req, res);
        res.redirect(303, next);
    };

    return { requireAuth, loginPage, login };
}

// Self-contained page (no app JS), styled like the app in both themes.
function loginPageHtml({ next, error }: { next: string; error: string | null }): string {
    return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>Sign in · Quantum Scaling</title>
<script>
  try {
    var saved = localStorage.getItem('qs_theme');
    var light = saved === 'light' || (saved !== 'dark' && matchMedia('(prefers-color-scheme: light)').matches);
    document.documentElement.dataset.theme = light ? 'light' : 'dark';
  } catch (e) {}
</script>
<style>
  :root { --bg: #1c1c1c; --card: #0e0e0e; --border: #2e2e2e; --input: #1c1c1c; --text: #ffffff; --muted: #6b7280; }
  :root[data-theme="light"] { --bg: #f8fafc; --card: #ffffff; --border: #e5e7eb; --input: #f8fafc; --text: #0f172a; --muted: #4b5563; }
  * { box-sizing: border-box; }
  body { margin: 0; min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 16px;
         background: var(--bg); color: var(--text); font: 13px/1.4 Inter, system-ui, -apple-system, sans-serif; }
  main { width: 100%; max-width: 360px; }
  .brand { display: flex; align-items: center; justify-content: center; gap: 10px; margin-bottom: 24px; font-weight: 700; font-size: 14px; }
  .logo { width: 28px; height: 28px; border-radius: 6px; background: #3ecf8e; }
  .card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 20px; }
  h1 { margin: 0 0 16px; font-size: 14px; }
  label { display: block; margin-bottom: 4px; font-size: 10px; font-weight: 700; letter-spacing: .1em; text-transform: uppercase; color: var(--muted); }
  input[type=password] { width: 100%; padding: 8px 12px; border-radius: 4px; border: 1px solid var(--border); background: var(--input); color: var(--text); font: inherit; }
  input[type=password]:focus { outline: none; border-color: #3ecf8e; }
  button { width: 100%; margin-top: 12px; padding: 8px 12px; border: 0; border-radius: 4px; background: #3ecf8e; color: #000; font: inherit; font-weight: 700; cursor: pointer; }
  button:hover { background: #2fb37a; }
  .error { margin-bottom: 16px; padding: 10px 14px; border-radius: 4px; border: 1px solid rgba(239, 68, 68, .3); background: rgba(239, 68, 68, .1); color: #f87171; font-size: 12px; }
</style>
</head>
<body>
<main>
  <div class="brand"><span class="logo"></span>Quantum Scaling</div>
  <form class="card" method="post" action="${LOGIN_PATH}">
    <h1>Enter the team password</h1>
    ${error ? `<div class="error" role="alert">${escapeHtml(error)}</div>` : ''}
    <input type="hidden" name="next" value="${escapeHtml(next)}" />
    <label for="password">Password</label>
    <input id="password" name="password" type="password" autocomplete="current-password" required autofocus />
    <button type="submit">Sign in</button>
  </form>
</main>
</body>
</html>`;
}
