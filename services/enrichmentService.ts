
/**
 * Quantum Enrichment Service (OpenAI Edition)
 * Uses the specific v1/responses endpoint and prompt ID architecture.
 */

const getEnv = (key: string) => {
  if (typeof import.meta !== 'undefined' && (import.meta as any).env && (import.meta as any).env[key]) {
    return (import.meta as any).env[key];
  }
  if (typeof process !== 'undefined' && process.env && process.env[key]) {
    return process.env[key];
  }
  return undefined;
};

const getOpenAIKey = () => {
  const key = getEnv('VITE_OPENAI_API_KEY');
  return key ? key.trim() : undefined;
};

const INPUT_COST_PER_1M = 0.80;
const OUTPUT_COST_PER_1M = 3.20;
// 60s hard timeout on OpenAI fetch. Without it, a degraded OpenAI can hang
// sockets indefinitely and freeze the entire JobProcessor pollLoop.
const OPENAI_TIMEOUT_MS = 60000;
const getPromptId = () => getEnv('VITE_OPENAI_PROMPT_ID');

console.log('🔍 OpenAI Config Check (Module Load):', {
  key: getOpenAIKey() ? 'PRESENT' : 'MISSING'
});

export interface BatchItem {
  contact_id: string;
  email: string;
  digest: string;
}

/**
 * Normalizes confidence to an integer between 1 and 10 to satisfy DB constraints.
 * Handles cases where model might return 0-1, 0-100, or strings.
 */
function normalizeConfidence(val: any): number {
  if (val === null || val === undefined) return 5;
  let num = parseFloat(val);
  if (isNaN(num)) return 5;

  // If model returns 0.0 - 1.0 (probabilities)
  if (num > 0 && num <= 1) num = num * 10;
  // If model returns 0 - 100
  else if (num > 10) num = num / 10;

  // Clamp to 1-10 range and round
  let clamped = Math.round(num);
  if (clamped < 1) clamped = 1;
  if (clamped > 10) clamped = 10;

  return clamped;
}

/**
 * Enriches a single contact using the OpenAI responses API.
 */
export async function enrichSingle(item: BatchItem): Promise<any> {
  const html_snippet = (item.digest || "").slice(0, 12000);
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), OPENAI_TIMEOUT_MS);
  try {
    const payload = {
      model: "gpt-4.1-mini",
      prompt: {
        id: getPromptId(),
        version: "2",
        variables: {
          html: html_snippet
        }
      }
    };

    const response = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${getOpenAIKey()}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
      signal: controller.signal
    });

    if (!response.ok) {
      let errorBody = "";
      try {
        errorBody = await response.text();
      } catch (e) {
        errorBody = "Could not parse error body";
      }
      console.error(`❌ OpenAI API Error [${response.status}]:`, errorBody);
      const err: any = new Error(`OpenAI API Error: ${response.status} - ${errorBody.slice(0, 500)}`);
      err.http_status = response.status;
      throw err;
    }

    const data = await response.json();

    let output_text = null;
    if (data.output && Array.isArray(data.output)) {
      for (const itemOut of data.output) {
        if (itemOut.content && Array.isArray(itemOut.content)) {
          for (const content of itemOut.content) {
            if (content.type === "output_text") {
              output_text = content.text;
              break;
            }
          }
        }
        if (output_text) break;
      }
    }

    if (!output_text) {
      throw new Error("No model output returned.");
    }

    const parsed = JSON.parse(output_text);
    const usage = data.usage || {};
    const input_tokens = usage.input_tokens || 0;
    const output_tokens = usage.output_tokens || 0;

    const estimated_cost = parseFloat(
      ((input_tokens / 1_000_000 * INPUT_COST_PER_1M) +
        (output_tokens / 1_000_000 * OUTPUT_COST_PER_1M)).toFixed(6)
    );

    return {
      contact_id: item.contact_id,
      classification: parsed.classification || "Unknown",
      industry: parsed.classification || "Unknown",
      confidence: normalizeConfidence(parsed.confidence),
      reasoning: parsed.reasoning || "No reasoning provided.",
      input_tokens,
      output_tokens,
      cost: estimated_cost,
      status: 'completed'
    };
  } catch (err: any) {
    console.error(`Enrichment failed for ${item.contact_id}:`, err);
    const isAbort = err?.name === 'AbortError' || /aborted/i.test(err?.message || '');
    const httpStatus: number | undefined = err?.http_status;
    let errorCategory: 'openai_5xx' | 'openai_4xx' | 'openai_timeout' | 'parse_error' | 'unknown';
    if (isAbort) errorCategory = 'openai_timeout';
    else if (httpStatus && httpStatus >= 500) errorCategory = 'openai_5xx';
    else if (httpStatus && httpStatus >= 400) errorCategory = 'openai_4xx';
    else if (/JSON|parse|No model output/i.test(err?.message || '')) errorCategory = 'parse_error';
    else errorCategory = 'unknown';

    return {
      contact_id: item.contact_id,
      classification: "ERROR",
      industry: "ERROR",
      confidence: 1,
      reasoning: isAbort ? `OpenAI request timed out after ${OPENAI_TIMEOUT_MS}ms` : err.message,
      input_tokens: 0,
      output_tokens: 0,
      cost: 0,
      status: 'failed',
      error_category: errorCategory,
      http_status: httpStatus
    };
  } finally {
    clearTimeout(timeoutId);
  }
}

/**
 * High-concurrency batch enrichment.
 */
export async function enrichBatch(items: BatchItem[]): Promise<any[]> {
  return await Promise.all(items.map(item => enrichSingle(item)));
}
