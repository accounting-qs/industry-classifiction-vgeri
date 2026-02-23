
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

  // Remove non-visible block elements and their contents
  const tagsToRemove = ['script', 'style', 'noscript', 'svg', 'path', 'template'];
  for (const tag of tagsToRemove) {
    // Aggressive pattern to catch <tag ...> ... </tag> across newlines
    // \s* matches optional whitespace inside the tag itself
    const pattern = new RegExp(`<${tag}[^>]*>[\\s\\S]*?<\\/${tag}\\s*>`, 'gi');
    clean = clean.replace(pattern, " ");

    // Catch self-closing tags
    const patternSelfClosing = new RegExp(`<${tag}[^>]*\\/?>`, 'gi');
    clean = clean.replace(patternSelfClosing, " ");
  }

  // Remove HTML comments
  clean = clean.replace(/<!--[\s\S]*?-->/g, "");

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
  let txt = html;

  // Safety pass using foolproof string indexing
  const tagsToNuke = ['script', 'style'];
  for (const tag of tagsToNuke) {
    const openTag = `<${tag}`;
    const closeTag = `</${tag}>`;
    let startIndex = txt.toLowerCase().indexOf(openTag);
    while (startIndex !== -1) {
      let endIndex = txt.toLowerCase().indexOf(closeTag, startIndex);
      if (endIndex !== -1) {
        const actualEndIndex = txt.indexOf('>', endIndex) + 1;
        txt = txt.substring(0, startIndex) + " " + txt.substring(actualEndIndex);
      } else {
        const endOfOpenTag = txt.indexOf('>', startIndex) + 1;
        if (endOfOpenTag > startIndex) {
          txt = txt.substring(0, startIndex) + " " + txt.substring(endOfOpenTag);
        } else {
          break;
        }
      }
      startIndex = txt.toLowerCase().indexOf(openTag);
    }
  }

  // Strip all remaining HTML tags
  txt = txt.replace(/<[^>]+>/gi, " ");

  // Safety pass 2: Sometimes React/Wix injects raw CSS into text nodes that bypass tag stripping
  // Strip CSS classes commonly dumped without tags (safe, non-greedy, no backtracking)
  txt = txt.replace(/\.[a-zA-Z0-9_-]+[^{]*?{[^}]*?}/g, " ");

  // Strip inline variable definitions like --var-name: value;
  txt = txt.replace(/--[a-zA-Z0-9_\-]+:[^;]+;/g, " ");

  // Collapse whitespace
  txt = txt.replace(/\s+/g, " ").trim();

  return txt.slice(0, maxChars);
}
