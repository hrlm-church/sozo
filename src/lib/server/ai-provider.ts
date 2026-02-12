import { createAzure } from "@ai-sdk/azure";
import { getServerEnv, looksConfigured } from "@/lib/server/env";

let _provider: ReturnType<typeof createAzure> | null = null;

function getProvider() {
  if (_provider) return _provider;

  const env = getServerEnv();
  if (!looksConfigured(env.openAiEndpoint) || !looksConfigured(env.openAiKey)) {
    throw new Error(
      "Azure OpenAI not configured. Set SOZO_OPENAI_ENDPOINT and SOZO_OPENAI_API_KEY.",
    );
  }

  _provider = createAzure({
    resourceName: extractResourceName(env.openAiEndpoint!),
    apiKey: env.openAiKey!,
  });

  return _provider;
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

/** gpt-4o for reasoning, tool calling, and complex queries */
export function getReasoningModel() {
  const env = getServerEnv();
  const deployment = env.openAiReasoningDeployment ?? env.openAiDeployment;
  if (!deployment) {
    throw new Error("No OpenAI deployment configured.");
  }
  return getProvider()(deployment);
}

/** gpt-4o-mini for quick, low-cost queries */
export function getQuickModel() {
  const env = getServerEnv();
  const deployment = env.openAiDeployment;
  if (!deployment) {
    throw new Error("No OpenAI deployment configured (SOZO_OPENAI_CHAT_DEPLOYMENT).");
  }
  return getProvider()(deployment);
}
