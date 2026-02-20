import { createAnthropic } from "@ai-sdk/anthropic";
import { createOpenAI } from "@ai-sdk/openai";
import { createAzure } from "@ai-sdk/azure";
import { getServerEnv, looksConfigured } from "@/lib/server/env";
import type { LanguageModel } from "ai";

/**
 * Provider chain: Claude → OpenAI → Azure OpenAI
 * Returns all configured models in priority order for fallback.
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
  if (!looksConfigured(env.openAiEndpoint) || !looksConfigured(env.openAiKey)) return null;
  _azure = createAzure({
    resourceName: extractResourceName(env.openAiEndpoint!),
    apiKey: env.openAiKey!,
  });
  return _azure;
}

function extractResourceName(endpoint: string): string {
  try {
    return new URL(endpoint).hostname.split(".")[0];
  } catch {
    return endpoint;
  }
}

/**
 * Returns all configured models in fallback order.
 * The chat route tries each in sequence — if one fails (rate limit, error),
 * it falls back to the next.
 */
export function getModelChain(): LanguageModel[] {
  const models: LanguageModel[] = [];

  // 1. Anthropic Claude (primary — best tool use, high rate limits)
  const anthropic = getAnthropicProvider();
  if (anthropic) {
    const model = process.env.ANTHROPIC_MODEL ?? "claude-sonnet-4-5-20250929";
    models.push(anthropic(model));
  }

  // 2. OpenAI (fallback — 500K TPM limit can be hit on large queries)
  const openai = getOpenAIProvider();
  if (openai) {
    const model = process.env.OPENAI_MODEL ?? "gpt-5.2";
    models.push(openai(model));
  }

  // 3. Azure OpenAI
  const azure = getAzureProvider();
  if (azure) {
    const env = getServerEnv();
    const deployment = env.openAiReasoningDeployment ?? env.openAiDeployment;
    if (deployment) models.push(azure(deployment));
  }

  if (models.length === 0) {
    throw new Error(
      "No AI provider configured. Set ANTHROPIC_API_KEY, OPENAI_API_KEY, or SOZO_OPENAI_ENDPOINT + SOZO_OPENAI_API_KEY.",
    );
  }

  return models;
}
