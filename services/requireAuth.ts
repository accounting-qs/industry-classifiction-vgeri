/**
 * Express middleware that only lets a request through when it carries a
 * valid Supabase Auth session.
 *
 * The browser signs in through @supabase/ssr's createBrowserClient, which
 * keeps the session in cookies. Same-origin fetch() calls and plain
 * <a href> downloads send those cookies on their own, so none of the
 * /api call sites in the UI need to attach anything. Scripts can send
 * `Authorization: Bearer <access token>` instead.
 *
 * getClaims() verifies the JWT locally against the project's published
 * ES256 signing keys (cached process-wide by supabase-js), so there is no
 * Auth round trip per request. When the access token has expired it uses
 * the refresh-token cookie to mint a new one, and setAll() below writes
 * the new cookies onto this response.
 */

import crypto from 'crypto';
import type { RequestHandler } from 'express';
import { createServerClient, parseCookieHeader, serializeCookieHeader } from '@supabase/ssr';

export interface AuthUser {
    id: string;
    email: string | null;
}

// Random per-process secret for the server's own loopback calls to /api
// (the import worker registers lists through POST /api/import-lists).
// It is never sent anywhere but 127.0.0.1 and changes on every restart.
export const INTERNAL_API_TOKEN = crypto.randomBytes(32).toString('hex');
export const INTERNAL_API_HEADER = 'x-internal-token';

function isInternalCall(header: string | undefined): boolean {
    if (!header || header.length !== INTERNAL_API_TOKEN.length) return false;
    return crypto.timingSafeEqual(Buffer.from(header), Buffer.from(INTERNAL_API_TOKEN));
}

export function requireAuth(supabaseUrl: string | undefined, supabaseAnonKey: string | undefined): RequestHandler {
    if (!supabaseUrl || !supabaseAnonKey) {
        console.error(
            '❌ CRITICAL: auth has no Supabase URL or anon key (VITE_SUPABASE_URL / VITE_SUPABASE_ANON_KEY). ' +
            'Every /api request will be refused with 401 until both are set.'
        );
    }

    return async (req, res, next) => {
        if (isInternalCall(req.get(INTERNAL_API_HEADER))) {
            res.locals.user = { id: 'server', email: null } satisfies AuthUser;
            return next();
        }
        if (!supabaseUrl || !supabaseAnonKey) {
            return res.status(401).json({ error: 'Not signed in' });
        }
        const bearer = req.headers.authorization?.match(/^Bearer\s+(.+)$/i)?.[1];

        try {
            // One client per request: it reads this request's cookies and
            // writes any refreshed session onto this response.
            const supabase = createServerClient(supabaseUrl, supabaseAnonKey, {
                cookies: {
                    getAll: () => parseCookieHeader(req.headers.cookie ?? '')
                        .map(c => ({ name: c.name, value: c.value ?? '' })),
                    setAll: (cookies, headers) => {
                        for (const { name, value, options } of cookies) {
                            res.append('Set-Cookie', serializeCookieHeader(name, value, options));
                        }
                        for (const [key, value] of Object.entries(headers)) res.setHeader(key, value);
                    },
                },
            });

            const { data, error } = await supabase.auth.getClaims(bearer);
            const claims = data?.claims;
            // The anon key is itself a JWT, so require a real user: a subject,
            // the authenticated role, and not an anonymous sign-in.
            if (error || !claims?.sub || claims.role !== 'authenticated' || claims.is_anonymous) {
                return res.status(401).json({ error: 'Not signed in' });
            }

            res.locals.user = { id: claims.sub, email: claims.email ?? null } satisfies AuthUser;
            next();
        } catch (e: any) {
            console.warn(`[auth] session check failed: ${e?.message || e}`);
            res.status(401).json({ error: 'Not signed in' });
        }
    };
}
