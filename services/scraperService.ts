
import {
  stripCssJs,
  extractOne,
  extractMany,
  getVisibleText,
  MAX_HTML_BYTES,
  VISIBLE_TEXT_CHARS
} from './htmlCleaner';

/**
 * High-performance Scraper Service with Proxy Racing.
 * Fires multiple proxy requests in parallel and takes the first success.
 */

const getZenRowsKey = () => {
  // @ts-ignore
  if (typeof import.meta !== 'undefined' && import.meta.env && import.meta.env.VITE_ZENROWS_API_KEY) {
    return import.meta.env.VITE_ZENROWS_API_KEY;
  }
  if (typeof process !== 'undefined' && process.env && process.env.VITE_ZENROWS_API_KEY) {
    return process.env.VITE_ZENROWS_API_KEY;
  }
  return undefined;
};

const PROXY_LIST = [
  // Codetabs proxy works well most of the time
  (url: string) => `https://api.codetabs.com/v1/proxy?quest=${encodeURIComponent(url)}`,
  (url: string) => `https://corsproxy.io/?${encodeURIComponent(url)}`,
  (url: string) => `https://proxy.corsfix.com/?url=${encodeURIComponent(url)}`,
  (url: string) => `https://api.allorigins.win/get?url=${encodeURIComponent(url)}`,
];

const PROXY_NAMES = ["Codetabs", "Corsproxy.io", "Corsfix", "AllOrigins"];

function normalizeUrl(url_or_domain: string): string {
  let s = (url_or_domain || "").trim();
  if (!s) return "";
  if (!s.startsWith("http://") && !s.startsWith("https://")) {
    s = "https://" + s;
  }
  return s;
}

export async function fetchDigest(
  urlOrDomain: string,
  onProgress?: (msg: string) => void
): Promise<{ digest: string, proxyName: string }> {
  const start_url = normalizeUrl(urlOrDomain);
  if (!start_url) throw new Error("Invalid URL or domain");

  // --- Waterfall Logic ---
  for (let i = 0; i < PROXY_LIST.length; i++) {
    const proxyName = PROXY_NAMES[i];
    try {
      if (onProgress) onProgress(`📡 [Scraper] ${proxyName} for: ${start_url}`);
      const raw_html = await attemptStandardProxy(i, start_url);
      return {
        digest: processHtmlToDigest(raw_html, start_url, proxyName),
        proxyName
      };
    } catch (err: any) {
      const errorMsg = `⏳ [Scraper] ${proxyName} failed for ${start_url}: ${err.message}`;
      console.warn(errorMsg);
      if (onProgress) onProgress(errorMsg);
    }
  }

  // --- Final Fallback: ZenRows ---
  const zenKey = getZenRowsKey();
  if (zenKey) {
    try {
      const pName = "ZenRows (Premium)";
      if (onProgress) onProgress(`🚀 [Scraper] Standard proxies exhausted for ${start_url}. Attempting ZenRows...`);
      const raw_html = await attemptZenRows(zenKey, start_url);
      return {
        digest: processHtmlToDigest(raw_html, start_url, pName),
        proxyName: pName
      };
    } catch (err: any) {
      const errorMsg = `❌ [Scraper] ZenRows premium also failed for ${start_url}: ${err.message}`;
      console.error(errorMsg);
      if (onProgress) onProgress(errorMsg);
    }
  }

  throw new Error(`All proxies failed for ${start_url}`);
}

async function attemptStandardProxy(index: number, targetUrl: string): Promise<string> {
  const proxyUrl = PROXY_LIST[index](targetUrl);
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 20000);

  try {
    const response = await fetch(proxyUrl, {
      signal: controller.signal,
      headers: {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Origin': 'https://corsproxy.io', // Spoof origin to satisfy some strict proxies
        'Referer': 'https://corsproxy.io/'
      }
    });
    clearTimeout(timeoutId);

    if (!response.ok) throw new Error(`HTTP ${response.status}`);

    let raw_html = "";
    if (proxyUrl.includes('allorigins')) {
      const json = await response.json();
      raw_html = json.contents || "";
    } else {
      raw_html = await response.text();
    }

    validateHtml(raw_html, targetUrl);
    return raw_html;
  } catch (e: any) {
    clearTimeout(timeoutId);
    throw e;
  }
}

async function attemptZenRows(apiKey: string, targetUrl: string): Promise<string> {
  // NOTE: removed autoparse=true to get raw HTML for processHtmlToDigest consistency
  const zenUrl = `https://api.zenrows.com/v1/?apikey=${apiKey}&url=${encodeURIComponent(targetUrl)}`;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 30000); // ZenRows gets 30s as it's deep fallback

  try {
    const response = await fetch(zenUrl, { signal: controller.signal });
    clearTimeout(timeoutId);
    if (!response.ok) throw new Error(`ZenRows HTTP ${response.status}`);

    const raw_html = await response.text();
    validateHtml(raw_html, targetUrl);
    return raw_html;
  } catch (e: any) {
    clearTimeout(timeoutId);
    throw e;
  }
}

function validateHtml(html: string, targetUrl: string) {
  const lowerHtml = (html || "").toLowerCase();
  const proxySignatures = [
    "api.codetabs.com", "corsproxy.io", "corsfix.com", "corsfix_error", "zenrows.com", "allorigins.win",
    "cloudflare.com/5xx-error", "access denied", "checking your browser",
    "captcha-delivery.com", "error 403", "forbidden", "not found",
    "cors error", "proxy error", "unusual traffic"
  ];

  const matchedSignature = proxySignatures.find(sig =>
    lowerHtml.includes(sig) && !targetUrl.toLowerCase().includes(sig)
  );

  if (matchedSignature) throw new Error(`Blocked by proxy: ${matchedSignature}`);
  if (!html || html.trim().length < 200) throw new Error("Insufficient content length");
}

function processHtmlToDigest(raw_html: string, start_url: string, proxyName: string): string {
  const capped_html = raw_html.slice(0, MAX_HTML_BYTES);
  const clean = stripCssJs(capped_html);

  const title = extractOne(clean, /<title[^>]*>([\s\S]*?)<\/title>/i);
  const meta_desc = extractOne(
    clean,
    /<meta[^>]*name\s*=\s*["']description["'][^>]*content\s*=\s*["']([\s\S]*?)["']/i
  );
  const h1s = extractMany(clean, "h1", 4);
  const h2s = extractMany(clean, "h2", 8);
  const vis = getVisibleText(clean, VISIBLE_TEXT_CHARS);

  const parts: string[] = [];
  parts.push(`START_URL: ${start_url}`);
  try { parts.push(`DOMAIN: ${new URL(start_url).hostname}`); } catch (e) { }
  parts.push(`PROXY_VIA: ${proxyName}`);

  parts.push("\n=== HOMEPAGE DIGEST ===");
  if (title) parts.push(`TITLE: ${title}`);
  if (meta_desc) parts.push(`META_DESCRIPTION: ${meta_desc}`);
  if (h1s.length > 0) parts.push("H1: " + h1s.join(" | "));
  if (h2s.length > 0) parts.push("H2: " + h2s.join(" | "));
  parts.push("\nVISIBLE_TEXT_SNIPPET:");
  parts.push(vis);

  return parts.join("\n");
}
