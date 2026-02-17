
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
  (url: string) => `https://corsproxy.io/?${encodeURIComponent(url)}`,
  (url: string) => `https://api.allorigins.win/get?url=${encodeURIComponent(url)}`,
  (url: string) => `https://api.codetabs.com/v1/proxy?quest=${encodeURIComponent(url)}`,
];

function normalizeUrl(url_or_domain: string): string {
  let s = (url_or_domain || "").trim();
  if (!s) return "";
  if (!s.startsWith("http://") && !s.startsWith("https://")) {
    s = "https://" + s;
  }
  return s;
}

export async function fetchDigest(urlOrDomain: string, onRetry?: (msg: string) => void): Promise<string> {
  const start_url = normalizeUrl(urlOrDomain);
  if (!start_url) throw new Error("Invalid URL or domain");

  const attemptProxy = async (index: number): Promise<string> => {
    const proxyUrl = PROXY_LIST[index](start_url);
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 25000); // Increased to 25s

    try {
      console.log(`📡 [Scraper] Attempting proxy ${index} for: ${start_url}`);
      const response = await fetch(proxyUrl, {
        signal: controller.signal,
        headers: {
          'Accept': 'application/json, text/plain, */*',
          'User-Agent': 'Mozilla/5.0 (compatible; RowZeroBot/1.0)'
        }
      });
      clearTimeout(timeoutId);

      if (!response.ok) {
        console.warn(`⚠️ [Scraper] Proxy ${index} failed with HTTP ${response.status}`);
        throw new Error(`HTTP ${response.status}`);
      }

      let raw_html = "";
      if (proxyUrl.includes('allorigins')) {
        const json = await response.json();
        if (json.status && json.status.http_code >= 400) {
          console.warn(`⚠️ [Scraper] AllOrigins proxy returned error ${json.status.http_code}`);
          throw new Error(`Origin Error ${json.status.http_code}`);
        }
        raw_html = json.contents || "";
      } else {
        raw_html = await response.text();
      }

      if (!raw_html || raw_html.trim().length < 100) {
        console.warn(`⚠️ [Scraper] Proxy ${index} returned empty or too short content (${raw_html?.length || 0} chars)`);
        throw new Error("Empty content");
      }

      console.log(`✅ [Scraper] Proxy ${index} success! Content length: ${raw_html.length} chars`);
      return raw_html;
    } catch (e: any) {
      clearTimeout(timeoutId);
      console.error(`❌ [Scraper] Proxy ${index} error:`, e.message || e);
      throw e;
    }
  };

  try {
    const raw_html = await promiseAny([attemptProxy(0), attemptProxy(1)]);
    return processHtmlToDigest(raw_html, start_url);
  } catch (err: any) {
    console.warn(`⏳ [Scraper] Primary proxies failed or timed out. Detail: ${err.message}`);
    if (onRetry) onRetry(`Initial proxies failed. Attempting deep fallback...`);
    try {
      const raw_html = await attemptProxy(2);
      return processHtmlToDigest(raw_html, start_url);
    } catch (err2: any) {
      console.error(`❌ [Scraper] All proxies exhausted for ${start_url}. Final error:`, err2.message);
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
