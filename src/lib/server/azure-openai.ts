import { getServerEnv, looksConfigured } from "@/lib/server/env";

export interface ChatMessage {
  role: "system" | "user" | "assistant";
  content: string;
}

interface OpenAiChatResult {
  ok: boolean;
  content?: string;
  model?: string;
  error?: string;
}

export const isOpenAiConfigured = () => {
  const env = getServerEnv();
  return (
    looksConfigured(env.openAiEndpoint) &&
    looksConfigured(env.openAiKey) &&
    looksConfigured(env.openAiDeployment)
  );
};

export const runAzureOpenAiChat = async (
  messages: ChatMessage[],
): Promise<OpenAiChatResult> => {
  const env = getServerEnv();
  if (!isOpenAiConfigured()) {
    return {
      ok: false,
      error:
        "Azure OpenAI is not configured. Set SOZO_OPENAI_ENDPOINT, SOZO_OPENAI_API_KEY, and SOZO_OPENAI_CHAT_DEPLOYMENT.",
    };
  }

  const endpoint = env.openAiEndpoint!.replace(/\/$/, "");
  const deployment = env.openAiDeployment!;
  const url = `${endpoint}/openai/deployments/${deployment}/chat/completions?api-version=${encodeURIComponent(
    env.openAiApiVersion,
  )}`;

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "api-key": env.openAiKey!,
    },
    body: JSON.stringify({
      messages,
      temperature: 0.2,
      max_tokens: 800,
    }),
    cache: "no-store",
  });

  if (!response.ok) {
    return {
      ok: false,
      error: `Azure OpenAI call failed (${response.status})`,
    };
  }

  const body = (await response.json()) as {
    model?: string;
    choices?: Array<{ message?: { content?: string } }>;
  };

  const content = body.choices?.[0]?.message?.content;
  if (!content) {
    return {
      ok: false,
      error: "Azure OpenAI returned an empty answer.",
    };
  }

  return {
    ok: true,
    content,
    model: body.model,
  };
};
