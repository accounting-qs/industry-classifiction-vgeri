
/**
 * Port of the specific Python cleaning logic provided by the user.
 * Ensures CSS, JS, and styling are removed to optimize for LLM and storage.
 */

export const MAX_HTML_BYTES = 300_000;
export const VISIBLE_TEXT_CHARS = 6000;

/**
 * Port of Python _strip_css_js
 */
export function stripCssJs(html: string): string {
  if (!html) return "";
  let clean = html;
  
  // Remove scripts + styles
  // Using [\s\S]*? to handle multi-line content (equivalent to Python's re.DOTALL)
  clean = clean.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, "");
  clean = clean.replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, "");
  
  // Remove stylesheet link tags
  clean = clean.replace(/<link\b[^>]*\brel\s*=\s*(['"])stylesheet\1[^>]*>/gi, "");
  
  // Remove inline styles
  clean = clean.replace(/\sstyle\s*=\s*(['"])[\s\S]*?\1/gi, "");
  
  return clean;
}

/**
 * Port of Python _extract_one
 */
export function extractOne(html: string, pattern: RegExp): string {
  const match = html.match(pattern);
  if (!match || !match[1]) return "";
  return match[1].replace(/\s+/g, " ").trim();
}

/**
 * Port of Python _extract_many
 */
export function extractMany(html: string, tag: string, limit: number): string[] {
  const pattern = new RegExp(`<${tag}\\b[^>]*>([\\s\\S]*?)</${tag}>`, "gi");
  const matches = [...html.matchAll(pattern)];
  const out: string[] = [];
  
  for (const match of matches) {
    if (out.length >= limit) break;
    let txt = match[1].replace(/<[^>]+>/gi, " ");
    txt = txt.replace(/\s+/g, " ").trim();
    if (txt) out.push(txt);
  }
  
  return out;
}

/**
 * Port of Python _visible_text
 */
export function getVisibleText(html: string, maxChars: number): string {
  let txt = html.replace(/<[^>]+>/gi, " ");
  txt = txt.replace(/\s+/g, " ").trim();
  return txt.slice(0, maxChars);
}
