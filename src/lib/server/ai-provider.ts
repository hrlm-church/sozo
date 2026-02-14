import { createOpenAI } from "@ai-sdk/openai";
import { createAzure } from "@ai-sdk/azure";
import { getServerEnv, looksConfigured } from "@/lib/server/env";

/**
 * Provider priority:
 * 1. Direct OpenAI API (OPENAI_API_KEY) — GPT-4.1-mini
 * 2. Azure OpenAI (SOZO_OPENAI_*) — fallback
 */

let _openai: ReturnType<typeof createOpenAI> | null = null;
let _azure: ReturnType<typeof createAzure> | null = null;

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

/** Primary model — GPT-4.1-mini via OpenAI API, or Azure gpt-4o fallback */
export function getReasoningModel() {
  // 1. Try direct OpenAI API
  const openai = getOpenAIProvider();
  if (openai) {
    const model = process.env.OPENAI_MODEL ?? "gpt-5-mini";
    return openai(model);
  }

  // 2. Fall back to Azure OpenAI
  const azure = getAzureProvider();
  if (azure) {
    const env = getServerEnv();
    const deployment = env.openAiReasoningDeployment ?? env.openAiDeployment;
    if (deployment) return azure(deployment);
  }

  throw new Error(
    "No AI provider configured. Set OPENAI_API_KEY for OpenAI, or SOZO_OPENAI_ENDPOINT + SOZO_OPENAI_API_KEY for Azure.",
  );
}

/** Quick model — GPT-4.1-mini via OpenAI API, or Azure gpt-4o-mini fallback */
export function getQuickModel() {
  // 1. Try direct OpenAI API
  const openai = getOpenAIProvider();
  if (openai) {
    const model = process.env.OPENAI_MODEL ?? "gpt-5-mini";
    return openai(model);
  }

  // 2. Fall back to Azure OpenAI
  const azure = getAzureProvider();
  if (azure) {
    const env = getServerEnv();
    const deployment = env.openAiDeployment;
    if (deployment) return azure(deployment);
  }

  throw new Error(
    "No AI provider configured. Set OPENAI_API_KEY for OpenAI, or SOZO_OPENAI_ENDPOINT + SOZO_OPENAI_API_KEY for Azure.",
  );
}
