/**
 * Sign-in gate around the whole app (Supabase Auth, email + password).
 *
 * Accounts are invite-only: people are invited from the Supabase
 * dashboard and public sign-ups are switched off there, so this screen
 * only signs in, resets passwords, and sets a password from an invite.
 *
 * Links from Supabase emails land here in one of two shapes:
 *  - Dashboard invites come back with `#access_token=…&type=invite` in the
 *    hash (implicit flow). The browser client runs in PKCE mode and
 *    refuses that shape, so we read the hash ourselves and setSession().
 *  - "Forgot password" links come back to /set-password?code=… (PKCE, as
 *    they are requested from this browser); the client exchanges the
 *    code on its own while it initialises.
 * Either way the user ends up signed in on the "set a password" screen.
 */

import React, { createContext, useContext, useEffect, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import type { Session, SupabaseClient, User } from '@supabase/supabase-js';
import { Loader2, AlertCircle, CheckCircle2, DatabaseZap } from 'lucide-react';
import { getBrowserSupabase } from './services/supabaseClient';

const SET_PASSWORD_PATH = '/set-password';
const MIN_PASSWORD_LENGTH = 8;

interface AuthContextValue {
  user: User;
  signOut: () => Promise<void>;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used inside <AuthGate>');
  return ctx;
}

function getClient(): { supabase: SupabaseClient | null; configError: string | null } {
  try {
    return { supabase: getBrowserSupabase(), configError: null };
  } catch (e: any) {
    return { supabase: null, configError: e?.message || String(e) };
  }
}

// Reads tokens or an error that a Supabase email link left in the URL
// hash, then strips the hash so a reload doesn't replay it.
function takeHashParams(): URLSearchParams | null {
  const hash = window.location.hash;
  if (!hash.includes('access_token=') && !hash.includes('error_description=')) return null;
  const params = new URLSearchParams(hash.slice(1));
  window.history.replaceState(window.history.state, '', window.location.pathname + window.location.search);
  return params;
}

function isApiRequest(input: RequestInfo | URL): boolean {
  const raw = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
  const url = new URL(raw, window.location.origin);
  return url.origin === window.location.origin && url.pathname.startsWith('/api/');
}

// A 401 from /api means the server no longer accepts this browser's
// session (expired refresh token, user removed). Try one refresh; if that
// fails, drop the local session so the sign-in screen comes back instead
// of every panel showing its own error. Concurrent 401s share one attempt.
function installUnauthorizedHandler(supabase: SupabaseClient): () => void {
  const originalFetch = window.fetch;
  let recovering: Promise<void> | null = null;
  window.fetch = async (input, init) => {
    const res = await originalFetch(input, init);
    if (res.status === 401 && !recovering && isApiRequest(input)) {
      recovering = supabase.auth.refreshSession()
        .then(async ({ error }) => {
          if (error) await supabase.auth.signOut({ scope: 'local' });
        })
        .finally(() => { recovering = null; });
    }
    return res;
  };
  return () => { window.fetch = originalFetch; };
}

export function AuthGate({ children }: { children: React.ReactNode }) {
  const [{ supabase, configError }] = useState(getClient);
  const location = useLocation();
  const navigate = useNavigate();
  // undefined = still checking, null = signed out.
  const [session, setSession] = useState<Session | null | undefined>(undefined);
  const [needsPassword, setNeedsPassword] = useState(location.pathname === SET_PASSWORD_PATH);
  const [linkError, setLinkError] = useState<string | null>(null);

  useEffect(() => {
    if (!supabase) return;
    let cancelled = false;

    const { data: { subscription } } = supabase.auth.onAuthStateChange((event, next) => {
      if (cancelled) return;
      setSession(next);
      if (event === 'PASSWORD_RECOVERY') setNeedsPassword(true);
    });

    (async () => {
      const hash = takeHashParams();
      if (hash?.get('error_description')) {
        setLinkError(hash.get('error_description'));
      } else if (hash?.get('access_token') && hash.get('refresh_token')) {
        const { error } = await supabase.auth.setSession({
          access_token: hash.get('access_token')!,
          refresh_token: hash.get('refresh_token')!,
        });
        if (error) setLinkError(error.message);
        else if (hash.get('type') === 'invite' || hash.get('type') === 'recovery') setNeedsPassword(true);
      }
      const { data } = await supabase.auth.getSession();
      if (!cancelled) setSession(data.session);
    })();

    return () => {
      cancelled = true;
      subscription.unsubscribe();
    };
  }, [supabase]);

  const signedIn = !!session;
  useEffect(() => {
    if (!supabase || !signedIn) return;
    return installUnauthorizedHandler(supabase);
  }, [supabase, signedIn]);

  if (configError || !supabase) {
    return (
      <AuthShell>
        <Banner kind="error">{configError || 'Supabase is not configured.'}</Banner>
      </AuthShell>
    );
  }

  if (session === undefined) {
    return (
      <div className="flex h-screen items-center justify-center bg-[#1c1c1c] text-gray-500">
        <Loader2 className="w-5 h-5 animate-spin" />
      </div>
    );
  }

  if (!session) {
    const linkFailed = linkError
      || (location.pathname === SET_PASSWORD_PATH
        ? "That link didn't sign you in. It may have expired or been opened in a different browser. Request a new one below."
        : null);
    return <SignInForm supabase={supabase} linkError={linkFailed} />;
  }

  if (needsPassword) {
    return (
      <SetPasswordForm
        supabase={supabase}
        email={session.user.email ?? ''}
        onDone={() => {
          setNeedsPassword(false);
          navigate('/', { replace: true });
        }}
      />
    );
  }

  const value: AuthContextValue = {
    user: session.user,
    signOut: async () => { await supabase.auth.signOut({ scope: 'local' }); },
  };
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

function SignInForm({ supabase, linkError }: { supabase: SupabaseClient; linkError: string | null }) {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(linkError);
  const [notice, setNotice] = useState<string | null>(null);

  const signIn = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setError(null);
    setNotice(null);
    const { error } = await supabase.auth.signInWithPassword({ email: email.trim(), password });
    // On success onAuthStateChange swaps this screen for the app.
    if (error) setError(error.message);
    setBusy(false);
  };

  const sendReset = async () => {
    if (!email.trim()) {
      setError('Enter your email first, then choose "Forgot password?".');
      return;
    }
    setBusy(true);
    setError(null);
    setNotice(null);
    const { error } = await supabase.auth.resetPasswordForEmail(email.trim(), {
      redirectTo: `${window.location.origin}${SET_PASSWORD_PATH}`,
    });
    if (error) setError(error.message);
    else setNotice(`If ${email.trim()} has an account, a reset link is on its way. Open it in this browser.`);
    setBusy(false);
  };

  return (
    <AuthShell>
      <h2 className="text-sm font-bold text-white">Sign in</h2>
      <p className="text-[11px] text-gray-500 mt-0.5 mb-4">Accounts are invite-only. Ask an admin if you need one.</p>
      {error && <Banner kind="error">{error}</Banner>}
      {notice && <Banner kind="success">{notice}</Banner>}
      <form onSubmit={signIn} className="space-y-3">
        <Field label="Email">
          <input
            type="email"
            autoComplete="username"
            required
            autoFocus
            value={email}
            onChange={e => setEmail(e.target.value)}
            className={INPUT_CLASS}
          />
        </Field>
        <Field label="Password">
          <input
            type="password"
            autoComplete="current-password"
            required
            value={password}
            onChange={e => setPassword(e.target.value)}
            className={INPUT_CLASS}
          />
        </Field>
        <button type="submit" disabled={busy} className={PRIMARY_BUTTON_CLASS}>
          {busy && <Loader2 className="w-3 h-3 animate-spin" />}
          Sign in
        </button>
      </form>
      <button
        type="button"
        onClick={sendReset}
        disabled={busy}
        className="mt-3 text-[11px] text-gray-400 hover:text-[#3ecf8e] disabled:opacity-50"
      >
        Forgot password?
      </button>
    </AuthShell>
  );
}

function SetPasswordForm({ supabase, email, onDone }: { supabase: SupabaseClient; email: string; onDone: () => void }) {
  const [password, setPassword] = useState('');
  const [confirm, setConfirm] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const save = async (e: React.FormEvent) => {
    e.preventDefault();
    if (password.length < MIN_PASSWORD_LENGTH) {
      setError(`Use at least ${MIN_PASSWORD_LENGTH} characters.`);
      return;
    }
    if (password !== confirm) {
      setError("The two passwords don't match.");
      return;
    }
    setBusy(true);
    setError(null);
    const { error } = await supabase.auth.updateUser({ password });
    setBusy(false);
    if (error) setError(error.message);
    else onDone();
  };

  return (
    <AuthShell>
      <h2 className="text-sm font-bold text-white">Set your password</h2>
      <p className="text-[11px] text-gray-500 mt-0.5 mb-4">
        For {email}. You'll use it to sign in from now on.
      </p>
      {error && <Banner kind="error">{error}</Banner>}
      <form onSubmit={save} className="space-y-3">
        {/* Lets password managers file the new password under the right account. */}
        <input type="email" autoComplete="username" value={email} readOnly hidden />
        <Field label="New password">
          <input
            type="password"
            autoComplete="new-password"
            required
            autoFocus
            value={password}
            onChange={e => setPassword(e.target.value)}
            className={INPUT_CLASS}
          />
        </Field>
        <Field label="Repeat password">
          <input
            type="password"
            autoComplete="new-password"
            required
            value={confirm}
            onChange={e => setConfirm(e.target.value)}
            className={INPUT_CLASS}
          />
        </Field>
        <button type="submit" disabled={busy} className={PRIMARY_BUTTON_CLASS}>
          {busy && <Loader2 className="w-3 h-3 animate-spin" />}
          Save password
        </button>
      </form>
    </AuthShell>
  );
}

const INPUT_CLASS = 'w-full px-3 py-2 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-xs text-white placeholder-gray-600 focus:outline-none focus:border-[#3ecf8e]';
const PRIMARY_BUTTON_CLASS = 'w-full px-3 py-2 rounded text-xs font-bold bg-[#3ecf8e] text-black hover:bg-[#2fb37a] disabled:opacity-50 flex items-center justify-center gap-1';

function AuthShell({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex h-screen items-center justify-center bg-[#1c1c1c] text-[#ededed] px-4">
      <div className="w-full max-w-sm">
        <div className="flex items-center justify-center gap-3 mb-6">
          <div className="bg-[#3ecf8e] p-1.5 rounded-md">
            <DatabaseZap className="text-black w-4 h-4" />
          </div>
          <h1 className="text-sm font-bold text-white">Quantum Scaling</h1>
        </div>
        <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] px-5 py-5">
          {children}
        </div>
      </div>
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="block">
      <span className="block text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-1">{label}</span>
      {children}
    </label>
  );
}

function Banner({ kind, children }: { kind: 'error' | 'success'; children: React.ReactNode }) {
  return kind === 'error' ? (
    <div className="mb-4 px-4 py-2.5 bg-red-500/10 border border-red-500/30 rounded text-red-400 text-xs flex items-start gap-2">
      <AlertCircle className="w-3.5 h-3.5 shrink-0 mt-0.5" />
      <span className="flex-1">{children}</span>
    </div>
  ) : (
    <div className="mb-4 px-4 py-2.5 bg-[#3ecf8e]/10 border border-[#3ecf8e]/30 rounded text-[#3ecf8e] text-xs flex items-start gap-2">
      <CheckCircle2 className="w-3.5 h-3.5 shrink-0 mt-0.5" />
      <span className="flex-1">{children}</span>
    </div>
  );
}
