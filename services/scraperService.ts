
import dns from 'dns/promises';
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
  try {
    // @ts-ignore
    const key = (import.meta as any)?.env?.VITE_ZENROWS_API_KEY || (process as any)?.env?.VITE_ZENROWS_API_KEY || (typeof process !== 'undefined' ? process.env.VITE_ZENROWS_API_KEY : undefined);
    return key;
  } catch (e) {
    return undefined;
  }
};

const getScrapingBeeKey = () => {
  try {
    // @ts-ignore
    const key = (import.meta as any)?.env?.VITE_SCRAPINGBEE_API_KEY || (process as any)?.env?.VITE_SCRAPINGBEE_API_KEY || (typeof process !== 'undefined' ? process.env.VITE_SCRAPINGBEE_API_KEY : undefined);
    return key;
  } catch (e) {
    return undefined;
  }
};

const CORSPROXY_API_KEY = "cf93a78a";

const PROXY_LIST = [
  (url: string) => `https://corsproxy.io/?key=${CORSPROXY_API_KEY}&url=${encodeURIComponent(url)}`,
  // Free fallbacks if CorsProxy Business fails for some reason
  (url: string) => `https://api.codetabs.com/v1/proxy?quest=${encodeURIComponent(url)}`,
  (url: string) => `https://proxy.corsfix.com/?url=${encodeURIComponent(url)}`,
  (url: string) => `https://api.allorigins.win/get?url=${encodeURIComponent(url)}`,
];

const PROXY_NAMES = [
  "Corsproxy.io (Business)",
  "Codetabs",
  "Corsfix",
  "AllOrigins"
];
const PROXY_DOMAINS = [
  "https://corsproxy.io",
  "https://api.codetabs.com",
  "https://proxy.corsfix.com",
  "https://api.allorigins.win"
];

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

  // --- Phase 0: Fast Fail DNS Check ---
  let hostname = start_url;
  try {
    hostname = new URL(start_url).hostname;
    // Use resolve4 instead of lookup to bypass Node's libuv threadpool limits.
    // lookup can get jammed under high concurrency and falsely throw ESERVFAIL.
    await dns.resolve4(hostname);
  } catch (e: any) {
    // We strictly catch ENOTFOUND & ENODATA. ESERVFAIL and others might be temporary or DNS firewall issues (like Cloudflare)
    if (e.code === 'ENOTFOUND' || e.code === 'ENODATA') {
      const errorMsg = `❌ [FastFail] Domain ${hostname} is definitely not found (DNS ${e.code}). Skipping proxies.`;
      console.warn(errorMsg);
      if (onProgress) onProgress(errorMsg);
      // Immediately throw so we skip Phase 1 & 2
      throw new Error(`FastFail: Domain completely unreachable (${e.code})`);
    }
  }

  // --- Phase 1: Direct Fetch & Premium CorsProxy Racing (with Free Fallbacks) ---
  const racePromises: Promise<{ raw_html: string, proxyName: string }>[] = [];
  const raceController = new AbortController();

  // 1. Direct Fetch
  racePromises.push((async () => {
    try {
      if (onProgress) onProgress(`📡 [Scraper] Direct Fetch for: ${start_url}`);
      const raw_html = await attemptDirectFetch(start_url, 8000, raceController.signal);
      return { raw_html, proxyName: "Direct Fetch" };
    } catch (e: any) {
      if (onProgress) onProgress(`⏳ [Scraper] Direct Fetch failed: ${e.message}`);
      throw e;
    }
  })());

  // 2. Premium CorsProxy + Free Fallbacks
  for (let i = 0; i < PROXY_LIST.length; i++) {
    const proxyName = PROXY_NAMES[i];
    racePromises.push((async () => {
      try {
        if (onProgress) onProgress(`📡 [Scraper] ${proxyName} for: ${start_url}`);
        const raw_html = await attemptStandardProxy(i, start_url, 8000, raceController.signal);
        return { raw_html, proxyName };
      } catch (e: any) {
        if (onProgress) onProgress(`⏳ [Scraper] ${proxyName} failed: ${e.message}`);
        throw e;
      }
    })());
  }

  try {
    // Wait for the FIRST successful response
    const { raw_html, proxyName } = await Promise.any(racePromises);
    raceController.abort(); // Cancel all the losers immediately to free memory/connections

    if (onProgress) onProgress(`✅ [Scraper] ${proxyName} won the race for: ${start_url}`);
    return {
      digest: processHtmlToDigest(raw_html, start_url, proxyName),
      proxyName
    };
  } catch (err: any) {
    const errorMsg = `⏳ [Scraper] All free methods failed/timed out for ${start_url}.`;
    console.warn(errorMsg);
    if (onProgress) onProgress(errorMsg);
  }

  // --- Phase 2: Premium Fallbacks (ZenRows then ScrapingBee) ---
  const zenKey = getZenRowsKey();
  // ScrapingBee is fully disabled per user request
  // const beeKey = getScrapingBeeKey();

  if (zenKey) {
    try {
      const pName = "ZenRows (Premium)";
      if (onProgress) onProgress(`🚀 [Scraper] Attempting ZenRows premium for ${start_url}...`);
      const raw_html = await attemptZenRows(zenKey, start_url);
      return {
        digest: processHtmlToDigest(raw_html, start_url, pName),
        proxyName: pName
      };
    } catch (err: any) {
      const errorMsg = `❌ [Scraper] ZenRows premium failed for ${start_url}: ${err.message}`;
      console.warn(errorMsg);
      if (onProgress) onProgress(errorMsg);
    }
  }

  // disabled
  /*
  if (beeKey) {
    try {
      const pName = "ScrapingBee (Premium)";
      if (onProgress) onProgress(`🚀 [Scraper] Attempting ScrapingBee premium for ${start_url}...`);
      const raw_html = await attemptScrapingBee(beeKey, start_url);
      return {
        digest: processHtmlToDigest(raw_html, start_url, pName),
        proxyName: pName
      };
    } catch (err: any) {
      const errorMsg = `❌ [Scraper] ScrapingBee premium failed for ${start_url}: ${err.message}`;
      console.warn(errorMsg);
      if (onProgress) onProgress(errorMsg);
    }
  }
  */

  throw new Error(`Bulletproof scraping failed for ${start_url}`);
}

async function attemptDirectFetch(targetUrl: string, timeoutMs: number = 15000, parentSignal?: AbortSignal): Promise<string> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  const onParentAbort = () => {
    controller.abort();
    clearTimeout(timeoutId);
  };

  if (parentSignal) {
    if (parentSignal.aborted) {
      onParentAbort();
      throw new Error('Aborted by parent before starting');
    }
    parentSignal.addEventListener('abort', onParentAbort);
  }

  try {
    const response = await fetch(targetUrl, {
      signal: controller.signal,
      redirect: "follow",
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'Sec-Ch-Ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1'
      }
    });
    clearTimeout(timeoutId);

    if (!response.ok) {
      // If HTTPS fails, try a quick HTTP fallback for old sites
      if (targetUrl.startsWith('https://')) {
        return attemptDirectFetch(targetUrl.replace('https://', 'http://'), timeoutMs);
      }
      throw new Error(`HTTP ${response.status}`);
    }
    const raw_html = await response.text();
    validateHtml(raw_html, targetUrl);
    return raw_html;
  } catch (e: any) {
    clearTimeout(timeoutId);
    throw e;
  } finally {
    if (parentSignal) {
      parentSignal.removeEventListener('abort', onParentAbort);
    }
    clearTimeout(timeoutId);
  }
}

async function attemptStandardProxy(index: number, targetUrl: string, timeoutMs: number = 20000, parentSignal?: AbortSignal): Promise<string> {
  const proxyUrl = PROXY_LIST[index](targetUrl);
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  const onParentAbort = () => {
    controller.abort();
    clearTimeout(timeoutId);
  };

  if (parentSignal) {
    if (parentSignal.aborted) {
      onParentAbort();
      throw new Error('Aborted by parent before starting');
    }
    parentSignal.addEventListener('abort', onParentAbort);
  }

  try {
    const proxyDomain = PROXY_DOMAINS[index] || "https://corsproxy.io";
    const response = await fetch(proxyUrl, {
      signal: controller.signal,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': proxyDomain,
        'Referer': proxyDomain + '/'
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
  } finally {
    if (parentSignal) {
      parentSignal.removeEventListener('abort', onParentAbort);
    }
    clearTimeout(timeoutId);
  }
}

export async function attemptZenRows(apiKey: string, targetUrl: string): Promise<string> {
  // Use the user's successful "formula" first: apikey then url correctly encoded + adaptive stealth mode
  const zenUrl = `https://api.zenrows.com/v1/?apikey=${apiKey}&url=${encodeURIComponent(targetUrl)}&mode=auto`;

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 45000);

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

export async function attemptScrapingBee(apiKey: string, targetUrl: string): Promise<string> {
  const params = new URLSearchParams({
    api_key: apiKey,
    url: targetUrl,
    render_js: 'true'
  });

  const beeUrl = `https://app.scrapingbee.com/api/v1/?${params.toString()}`;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 45000);

  try {
    const response = await fetch(beeUrl, { signal: controller.signal });
    clearTimeout(timeoutId);
    if (!response.ok) throw new Error(`ScrapingBee HTTP ${response.status}`);

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
    "cloudflare-error", "px-captcha", "captcha-delivery.com",
    "access denied", "checking your browser", "unusual traffic",
    "corsfix_error", "zenrows.com/error", "scrapingbee.com/error",
    "attention required! | cloudflare", "sorry, you have been blocked"
  ];

  const matchedSignature = proxySignatures.find(sig =>
    lowerHtml.includes(sig) && !targetUrl.toLowerCase().includes(sig)
  );

  if (matchedSignature) throw new Error(`Blocked by proxy: ${matchedSignature}`);
  if (!html || html.trim().length < 200) throw new Error("Insufficient content length");
}

function processHtmlToDigest(raw_html: string, start_url: string, proxyName: string): string {
  // CRITICAL FIX: Strip CSS/JS *before* capping the HTML length!
  // Otherwise a large style/script tag might get cut in half, leaving an unclosed <style> tag
  // that defeats all regex stripping and spills CSS into the visible text.
  const clean_full = stripCssJs(raw_html);
  const clean = clean_full.slice(0, MAX_HTML_BYTES);

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
