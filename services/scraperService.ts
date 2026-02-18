
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

async function promiseAny<T>(promises: Promise<T>[]): Promise<T> {
  return new Promise((resolve, reject) => {
    let rejectedCount = 0;
    if (promises.length === 0) {
      return reject(new Error("No promises provided"));
    }
    promises.forEach((p) => {
      Promise.resolve(p)
        .then(resolve)
        .catch((err) => {
          rejectedCount++;
          if (rejectedCount === promises.length) {
            reject(new Error("All proxies failed"));
          }
        });
    });
  });
}

const PROXY_LIST = [
  // Codetabs proxy works well most of the time
  (url: string) => `https://api.codetabs.com/v1/proxy?quest=${encodeURIComponent(url)}`,
  (url: string) => `https://corsproxy.io/?${encodeURIComponent(url)}`,
  // (url: string) => `https://cors.x2u.in/?url=${encodeURIComponent(url)}`,
];

function normalizeUrl(url_or_domain: string): string {
  let s = (url_or_domain || "").trim();
  if (!s) return "";
  if (!s.startsWith("http://") && !s.startsWith("https://")) {
    s = "https://" + s;
  }
  return s;
}

const PROXY_NAMES = ["Codetabs", "CORS.lol", "CORS.x2u.in"];

export async function fetchDigest(urlOrDomain: string, onRetry?: (msg: string) => void): Promise<{ digest: string, proxyName: string }> {
  const start_url = normalizeUrl(urlOrDomain);
  if (!start_url) throw new Error("Invalid URL or domain");

  const attemptProxy = async (index: number): Promise<{ raw_html: string, proxyName: string }> => {
    const proxyUrl = PROXY_LIST[index](start_url);
    const proxyName = PROXY_NAMES[index];
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 25000);

    try {
      console.log(`📡 [Scraper] Attempting ${proxyName} for: ${start_url}`);
      const response = await fetch(proxyUrl, {
        signal: controller.signal,
        headers: {
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
      });
      clearTimeout(timeoutId);

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      let raw_html = "";
      if (proxyUrl.includes('allorigins')) {
        const json = await response.json();
        if (json.status && json.status.http_code >= 400) throw new Error(`Origin Error ${json.status.http_code}`);
        raw_html = json.contents || "";
      } else {
        raw_html = await response.text();
      }

      const lowerHtml = (raw_html || "").toLowerCase();

      // Validation: Detect if proxy is returning its own page instead of the target
      const proxySignatures = [
        "api.codetabs.com",
        "cors.lol",
        "cors.x2u.in",
        "cloudflare.com/5xx-error",
        "access denied",
        "checking your browser",
        "captcha-delivery.com"
      ];

      const matchedSignature = proxySignatures.find(sig => lowerHtml.includes(sig));

      if (matchedSignature && !start_url.toLowerCase().includes(matchedSignature)) {
        console.warn(`⚠️ [Scraper] ${proxyName} returned proxy-specific or blocked content: ${matchedSignature}`);
        throw new Error(`Proxy spoofing detected (${matchedSignature})`);
      }

      if (!raw_html || raw_html.trim().length < 200) {
        throw new Error("Empty or insufficient content");
      }

      console.log(`✅ [Scraper] ${proxyName} success! (${raw_html.length} chars)`);
      return { raw_html, proxyName };
    } catch (e: any) {
      clearTimeout(timeoutId);
      console.error(`❌ [Scraper] ${proxyName} error:`, e.message || e);
      throw e;
    }
  };

  try {
    const { raw_html, proxyName } = await promiseAny([attemptProxy(0), attemptProxy(1)]);
    return {
      digest: processHtmlToDigest(raw_html, start_url),
      proxyName
    };
  } catch (err: any) {
    console.warn(`⏳ [Scraper] Primary proxies failed. Detail: ${err.message}`);
    if (onRetry) onRetry(`Initial proxies failed. Attempting deep fallback...`);
    try {
      const { raw_html, proxyName } = await attemptProxy(2);
      return {
        digest: processHtmlToDigest(raw_html, start_url),
        proxyName
      };
    } catch (err2: any) {
      console.error(`❌ [Scraper] All proxies exhausted for ${start_url}`);
      throw new Error(err2.message || "All proxies failed");
    }
  }
}

function processHtmlToDigest(raw_html: string, start_url: string): string {
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

  parts.push("\n=== HOMEPAGE DIGEST ===");
  if (title) parts.push(`TITLE: ${title}`);
  if (meta_desc) parts.push(`META_DESCRIPTION: ${meta_desc}`);
  if (h1s.length > 0) parts.push("H1: " + h1s.join(" | "));
  if (h2s.length > 0) parts.push("H2: " + h2s.join(" | "));
  parts.push("\nVISIBLE_TEXT_SNIPPET:");
  parts.push(vis);

  return parts.join("\n");
}
