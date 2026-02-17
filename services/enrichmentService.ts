
/**
 * Quantum Enrichment Service (OpenAI Edition)
 * Uses the specific v1/responses endpoint and prompt ID architecture.
 */

const OPENAI_API_KEY = "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";
const INPUT_COST_PER_1M = 0.80;
const OUTPUT_COST_PER_1M = 3.20;
const PROMPT_ID = "pmpt_698605f8b9008197b84a3fffee0e34cf0edc8f4086b94066";

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
async function enrichSingle(item: BatchItem): Promise<any> {
  const html_snippet = (item.digest || "").slice(0, 12000);

  try {
    const response = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${OPENAI_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: "gpt-4.1-mini",
        prompt: {
          id: PROMPT_ID,
          version: "1",
          variables: {
            html: html_snippet
          }
        }
      })
    });

    if (!response.ok) {
      throw new Error(`OpenAI API Error: ${response.status}`);
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
    return {
      contact_id: item.contact_id,
      classification: "ERROR",
      industry: "ERROR",
      confidence: 1, 
      reasoning: err.message,
      input_tokens: 0,
      output_tokens: 0,
      cost: 0,
      status: 'failed'
    };
  }
}

/**
 * High-concurrency batch enrichment.
 */
export async function enrichBatch(items: BatchItem[]): Promise<any[]> {
  return await Promise.all(items.map(item => enrichSingle(item)));
}
