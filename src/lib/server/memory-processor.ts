/**
 * Conversation memory extraction via gpt-4o-mini.
 * Extracts summaries, knowledge items, and topics from conversation transcripts.
 */

export interface ExtractionResult {
  summary: string;
  topics: string[];
  knowledge: Array<{
    category: "correction" | "preference" | "pattern" | "fact" | "persona";
    content: string;
    confidence: number;
  }>;
}

const EXTRACTION_PROMPT = `You are a conversation analyst for Sozo, a ministry intelligence platform.
Analyze this conversation and extract:

1. SUMMARY: A 2-3 sentence natural language summary of what was discussed and discovered. Write it as if briefing someone who will continue this conversation later. Include specific names, numbers, and findings.

2. TOPICS: An array of 3-8 topic tags. Use lowercase, specific terms like "donor retention", "year-end giving", "top donors", "event attendance", "subscription churn", "commerce trends", "wealth screening", "donor analysis", "360 profile", "lifecycle analysis".

3. KNOWLEDGE: An array of things the AI should remember permanently. Each item has:
   - category: "correction" | "preference" | "pattern" | "fact" | "persona"
   - content: The specific thing to remember (1-2 sentences max)
   - confidence: 0.0-1.0

Rules:
- "correction": ONLY when the user explicitly corrected the AI
- "preference": When the user expressed how they want data shown or analyzed
- "pattern": When data analysis revealed a notable, reusable finding
- "fact": Organizational facts stated by the user not in the database
- "persona": Information about this specific user's role or interests
- Do NOT extract trivial or one-time facts
- Keep to 0-5 items. Most conversations produce 0-2.

Respond with ONLY valid JSON (no markdown, no code fences):
{"summary": "...", "topics": ["..."], "knowledge": [{"category": "...", "content": "...", "confidence": 0.9}]}`;

/**
 * Call gpt-4o-mini to extract memory from a conversation transcript.
 */
export async function extractConversationMemory(
  transcript: string,
): Promise<ExtractionResult | null> {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) return null;

  try {
    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          { role: "system", content: EXTRACTION_PROMPT },
          { role: "user", content: transcript.slice(0, 12000) },
        ],
        temperature: 0.3,
        max_tokens: 1000,
        response_format: { type: "json_object" },
      }),
    });

    if (!response.ok) {
      console.error("[memory-processor] OpenAI error:", response.status);
      return null;
    }

    const body = (await response.json()) as {
      choices?: Array<{ message?: { content?: string } }>;
    };
    const content = body.choices?.[0]?.message?.content;
    if (!content) return null;

    const parsed = JSON.parse(content) as ExtractionResult;

    // Validate structure
    if (!parsed.summary || !Array.isArray(parsed.topics)) return null;
    if (!Array.isArray(parsed.knowledge)) parsed.knowledge = [];

    // Clamp confidence values
    parsed.knowledge = parsed.knowledge
      .filter((k) => k.category && k.content)
      .map((k) => ({
        ...k,
        confidence: Math.max(0, Math.min(1, k.confidence ?? 0.8)),
      }));

    return parsed;
  } catch (err) {
    console.error("[memory-processor] Extraction failed:", err);
    return null;
  }
}

/**
 * Build a plain text transcript from conversation messages.
 * Strips widget JSON, tool calls, and system messages.
 */
export function buildTranscript(
  messages: Array<{ role: string; content_json: string }>,
): string {
  const lines: string[] = [];

  for (const msg of messages) {
    if (msg.role !== "user" && msg.role !== "assistant") continue;

    let text = "";
    try {
      const parsed = JSON.parse(msg.content_json);
      // UIMessage format: content can be string or array of parts
      if (typeof parsed.content === "string") {
        text = parsed.content;
      } else if (Array.isArray(parsed.content)) {
        text = parsed.content
          .filter((p: { type: string }) => p.type === "text")
          .map((p: { text: string }) => p.text || "")
          .join(" ");
      } else if (typeof parsed === "string") {
        text = parsed;
      }
    } catch {
      text = msg.content_json?.slice(0, 500) || "";
    }

    // Skip greeting triggers and empty messages
    if (!text || text === "[GREETING]") continue;
    // Trim very long assistant responses (widget descriptions etc)
    if (text.length > 2000) text = text.slice(0, 2000) + "...";

    const role = msg.role === "user" ? "User" : "Assistant";
    lines.push(`${role}: ${text}`);
  }

  return lines.join("\n\n");
}
