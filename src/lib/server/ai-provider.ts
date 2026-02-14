import { createAnthropic } from "@ai-sdk/anthropic";
import { createOpenAI } from "@ai-sdk/openai";
import { createAzure } from "@ai-sdk/azure";
import { getServerEnv, looksConfigured } from "@/lib/server/env";

/**
 * Provider priority:
 * 1. Anthropic Claude (ANTHROPIC_API_KEY) — claude-sonnet-4-5-20250929
 * 2. Direct OpenAI API (OPENAI_API_KEY) — gpt-5-mini
 * 3. Azure OpenAI (SOZO_OPENAI_*) — fallback
 */

let _anthropic: ReturnType<typeof createAnthropic> | null = null;
let _openai: ReturnType<typeof createOpenAI> | null = null;
let _azure: ReturnType<typeof createAzure> | null = null;

function getAnthropicProvider() {
  if (_anthropic) return _anthropic;

  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) return null;

  _anthropic = createAnthropic({ apiKey });
  return _anthropic;
}

function getOpenAIProvider() {
  if (_openai) return _openai;

  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) return null;

  _openai = createOpenAI({ apiKey });
  return _openai;
}

function getAzureProvider() {
  if (_azure) return _azure;

  const env = getServerEnv();
  if (!looksConfigured(env.openAiEndpoint) || !looksConfigured(env.openAiKey)) {
    return null;
  }

  _azure = createAzure({
    resourceName: extractResourceName(env.openAiEndpoint!),
    apiKey: env.openAiKey!,
  });

  return _azure;
}

/** Extract resource name from endpoint URL (e.g., "https://foo.openai.azure.com" → "foo") */
function extractResourceName(endpoint: string): string {
  try {
    const url = new URL(endpoint);
    const parts = url.hostname.split(".");
    return parts[0];
  } catch {
    return endpoint;
  }
}

/** Primary model — Claude Sonnet 4.5 → OpenAI → Azure fallback */
export function getReasoningModel() {
  // 1. Try Anthropic Claude
  const anthropic = getAnthropicProvider();
  if (anthropic) {
    const model = process.env.ANTHROPIC_MODEL ?? "claude-sonnet-4-5-20250929";
    return anthropic(model);
  }

  // 2. Try direct OpenAI API
  const openai = getOpenAIProvider();
  if (openai) {
    const model = process.env.OPENAI_MODEL ?? "gpt-5-mini";
    return openai(model);
  }

  // 3. Fall back to Azure OpenAI
  const azure = getAzureProvider();
  if (azure) {
    const env = getServerEnv();
    const deployment = env.openAiReasoningDeployment ?? env.openAiDeployment;
    if (deployment) return azure(deployment);
  }

  throw new Error(
    "No AI provider configured. Set ANTHROPIC_API_KEY, OPENAI_API_KEY, or SOZO_OPENAI_ENDPOINT + SOZO_OPENAI_API_KEY.",
  );
}

/** Quick model — same priority chain */
export function getQuickModel() {
  // 1. Try Anthropic Claude
  const anthropic = getAnthropicProvider();
  if (anthropic) {
    const model = process.env.ANTHROPIC_MODEL ?? "claude-sonnet-4-5-20250929";
    return anthropic(model);
  }

  // 2. Try direct OpenAI API
  const openai = getOpenAIProvider();
  if (openai) {
    const model = process.env.OPENAI_MODEL ?? "gpt-5-mini";
    return openai(model);
  }

  // 3. Fall back to Azure OpenAI
  const azure = getAzureProvider();
  if (azure) {
    const env = getServerEnv();
    const deployment = env.openAiDeployment;
    if (deployment) return azure(deployment);
  }

  throw new Error(
    "No AI provider configured. Set ANTHROPIC_API_KEY, OPENAI_API_KEY, or SOZO_OPENAI_ENDPOINT + SOZO_OPENAI_API_KEY.",
  );
}
