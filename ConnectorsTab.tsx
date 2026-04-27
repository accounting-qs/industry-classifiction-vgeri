/**
 * Connectors — runtime-configurable secrets and integrations.
 *
 * Today: Anthropic API key (used by Phase 1a discovery in the Bucketing
 * feature). Stored server-side in app_settings; the server never echoes the
 * full value back, only a masked preview ("sk-ant-…wxyz") so the UI can
 * confirm a key is configured without exposing it.
 */

import React, { useEffect, useState } from 'react';
import { Loader2, AlertCircle, CheckCircle2, X, KeyRound, Trash2, Save, ExternalLink } from 'lucide-react';

interface AnthropicStatus {
  configured: boolean;
  source: 'app_settings' | 'env' | 'none';
  masked: string | null;
}

export function ConnectorsTab() {
  const [status, setStatus] = useState<AnthropicStatus | null>(null);
  const [keyInput, setKeyInput] = useState('');
  const [saving, setSaving] = useState(false);
  const [removing, setRemoving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  const refresh = async () => {
    try {
      const res = await fetch('/api/settings/anthropic-key');
      const data = await res.json();
      setStatus(data);
    } catch (e: any) {
      setError(e.message);
    }
  };

  useEffect(() => { refresh(); }, []);

  const save = async () => {
    setError(null); setSuccess(null);
    if (!keyInput.trim()) { setError('Paste a key first'); return; }
    setSaving(true);
    try {
      const res = await fetch('/api/settings/anthropic-key', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key: keyInput.trim() })
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || `Failed (${res.status})`);
      setKeyInput('');
      setSuccess('Anthropic key saved. Phase 1a will now use Claude Sonnet.');
      await refresh();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setSaving(false);
    }
  };

  const remove = async () => {
    if (!confirm('Remove the Anthropic API key? Phase 1a will fall back to gpt-4.1.')) return;
    setError(null); setSuccess(null);
    setRemoving(true);
    try {
      const res = await fetch('/api/settings/anthropic-key', { method: 'DELETE' });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.error || `Failed (${res.status})`);
      }
      setSuccess('Anthropic key removed.');
      await refresh();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setRemoving(false);
    }
  };

  return (
    <div className="flex-1 overflow-y-auto custom-scrollbar p-6 bg-[#1c1c1c] text-[#ededed]">
      <div className="max-w-3xl mx-auto">
        <div className="mb-6">
          <h2 className="text-lg font-bold text-white flex items-center gap-2">
            <KeyRound className="w-5 h-5 text-[#3ecf8e]" /> Connectors
          </h2>
          <p className="text-xs text-gray-500 mt-1">
            Runtime-configurable secrets and integrations. Stored server-side, never echoed back in full.
          </p>
        </div>

        {error && (
          <div className="mb-4 px-4 py-2.5 bg-red-500/10 border border-red-500/30 rounded text-red-400 text-xs flex items-start gap-2">
            <AlertCircle className="w-3.5 h-3.5 shrink-0 mt-0.5" />
            <span className="flex-1">{error}</span>
            <button onClick={() => setError(null)} className="text-red-400 hover:text-white"><X className="w-3 h-3" /></button>
          </div>
        )}
        {success && (
          <div className="mb-4 px-4 py-2.5 bg-[#3ecf8e]/10 border border-[#3ecf8e]/30 rounded text-[#3ecf8e] text-xs flex items-start gap-2">
            <CheckCircle2 className="w-3.5 h-3.5 shrink-0 mt-0.5" />
            <span className="flex-1">{success}</span>
            <button onClick={() => setSuccess(null)} className="text-[#3ecf8e] hover:text-white"><X className="w-3 h-3" /></button>
          </div>
        )}

        <div className="border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] overflow-hidden">
          <div className="px-5 py-4 border-b border-[#2e2e2e]">
            <div className="flex items-center justify-between">
              <div>
                <h3 className="text-sm font-bold text-white flex items-center gap-2">
                  Anthropic Claude
                  {status?.configured && (
                    <span className="text-[10px] font-bold text-[#3ecf8e] bg-[#3ecf8e]/10 border border-[#3ecf8e]/30 px-2 py-0.5 rounded">CONFIGURED</span>
                  )}
                </h3>
                <p className="text-[11px] text-gray-500 mt-0.5">
                  Powers Phase 1a bucket discovery (Claude Sonnet 4.6). If unset, Phase 1a falls back to gpt-4.1 transparently.
                </p>
              </div>
              <a
                href="https://console.anthropic.com/settings/keys"
                target="_blank"
                rel="noopener noreferrer"
                className="text-[11px] text-gray-400 hover:text-[#3ecf8e] flex items-center gap-1"
              >
                Get a key <ExternalLink className="w-3 h-3" />
              </a>
            </div>
          </div>

          <div className="px-5 py-4 space-y-3">
            {status === null ? (
              <div className="text-xs text-gray-500 flex items-center gap-2">
                <Loader2 className="w-3 h-3 animate-spin" /> Checking status…
              </div>
            ) : status.configured ? (
              <div className="space-y-3">
                <div className="flex items-center justify-between bg-[#1c1c1c] border border-[#2e2e2e] rounded px-3 py-2">
                  <div>
                    <div className="text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-0.5">Current key</div>
                    <div className="font-mono text-sm text-gray-200">{status.masked}</div>
                  </div>
                  <span className="text-[10px] font-bold text-gray-500 uppercase">
                    Source: {status.source === 'app_settings' ? 'Connectors' : 'Environment'}
                  </span>
                </div>
                <div>
                  <div className="text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-1">Replace key</div>
                  <div className="flex gap-2">
                    <input
                      type="password"
                      value={keyInput}
                      onChange={e => setKeyInput(e.target.value)}
                      placeholder="sk-ant-…"
                      className="flex-1 px-3 py-2 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-xs font-mono text-white placeholder-gray-600 focus:outline-none focus:border-[#3ecf8e]"
                    />
                    <button
                      onClick={save}
                      disabled={saving || !keyInput.trim()}
                      className="px-3 py-2 rounded text-xs font-bold bg-[#3ecf8e] text-black hover:bg-[#2fb37a] disabled:opacity-50 flex items-center gap-1"
                    >
                      {saving ? <Loader2 className="w-3 h-3 animate-spin" /> : <Save className="w-3 h-3" />}
                      Replace
                    </button>
                    <button
                      onClick={remove}
                      disabled={removing || status.source !== 'app_settings'}
                      title={status.source !== 'app_settings' ? 'Env-sourced keys are managed in your hosting environment, not here.' : 'Remove the saved key'}
                      className="px-3 py-2 rounded text-xs font-bold bg-[#1c1c1c] border border-[#2e2e2e] text-gray-400 hover:text-red-400 hover:border-red-500/40 disabled:opacity-50 flex items-center gap-1"
                    >
                      {removing ? <Loader2 className="w-3 h-3 animate-spin" /> : <Trash2 className="w-3 h-3" />}
                      Remove
                    </button>
                  </div>
                </div>
              </div>
            ) : (
              <div>
                <div className="text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-1">Anthropic API Key</div>
                <div className="flex gap-2">
                  <input
                    type="password"
                    value={keyInput}
                    onChange={e => setKeyInput(e.target.value)}
                    placeholder="sk-ant-…"
                    className="flex-1 px-3 py-2 bg-[#1c1c1c] border border-[#2e2e2e] rounded text-xs font-mono text-white placeholder-gray-600 focus:outline-none focus:border-[#3ecf8e]"
                  />
                  <button
                    onClick={save}
                    disabled={saving || !keyInput.trim()}
                    className="px-3 py-2 rounded text-xs font-bold bg-[#3ecf8e] text-black hover:bg-[#2fb37a] disabled:opacity-50 flex items-center gap-1"
                  >
                    {saving ? <Loader2 className="w-3 h-3 animate-spin" /> : <Save className="w-3 h-3" />}
                    Save
                  </button>
                </div>
                <p className="text-[10px] text-gray-600 mt-2 italic">
                  Without a key, Phase 1a uses gpt-4.1 — still functional, marginally lower quality than Sonnet on this kind of multi-section structured analysis.
                </p>
              </div>
            )}
          </div>
        </div>

        <div className="mt-6 border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] overflow-hidden opacity-60">
          <div className="px-5 py-4 border-b border-[#2e2e2e]">
            <h3 className="text-sm font-bold text-white">OpenAI</h3>
            <p className="text-[11px] text-gray-500 mt-0.5">
              Powers enrichment, Phase 1b matching, embeddings. Set via environment variable; not editable here.
            </p>
          </div>
          <div className="px-5 py-3 text-[11px] text-gray-500">
            Configured via <code className="bg-[#1c1c1c] px-1 rounded text-[10px]">VITE_OPENAI_API_KEY</code> env var.
          </div>
        </div>

        <div className="mt-6 border border-[#2e2e2e] rounded-xl bg-[#0e0e0e] overflow-hidden opacity-60">
          <div className="px-5 py-4 border-b border-[#2e2e2e]">
            <h3 className="text-sm font-bold text-white">Supabase</h3>
            <p className="text-[11px] text-gray-500 mt-0.5">
              Database + persistence. Set via environment variable.
            </p>
          </div>
          <div className="px-5 py-3 text-[11px] text-gray-500">
            Configured via <code className="bg-[#1c1c1c] px-1 rounded text-[10px]">VITE_SUPABASE_URL</code> and <code className="bg-[#1c1c1c] px-1 rounded text-[10px]">SUPABASE_SERVICE_ROLE_KEY</code>.
          </div>
        </div>
      </div>
    </div>
  );
}
