/**
 * Hybrid search client for Azure AI Search with vector embeddings.
 * Supports keyword + vector hybrid search for semantic "any question" capability.
 */
import { getServerEnv, looksConfigured } from "@/lib/server/env";

interface SearchResult {
  ok: boolean;
  error?: string;
  results: PersonProfile[];
  totalCount: number;
}

export interface PersonProfile {
  person_id: number;
  display_name: string;
  email: string;
  location: string;
  lifecycle_stage: string;
  content: string;
  giving_total: number;
  order_count: number;
  event_count: number;
  has_subscription: boolean;
  score: number;
}

const INDEX_NAME = "sozo-360-v1";

/**
 * Generate embedding for a text query using OpenAI API.
 */
async function getQueryEmbedding(text: string): Promise<number[] | null> {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) return null;

  try {
    const response = await fetch("https://api.openai.com/v1/embeddings", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model: "text-embedding-3-small",
        input: text.slice(0, 8000),
      }),
    });

    if (!response.ok) return null;
    const body = (await response.json()) as {
      data?: Array<{ embedding: number[] }>;
    };
    return body.data?.[0]?.embedding ?? null;
  } catch {
    return null;
  }
}

/**
 * Run hybrid search (keyword + vector) against Azure AI Search.
 */
export async function hybridSearch(
  query: string,
  top: number = 10,
  filter?: string,
): Promise<SearchResult> {
  const env = getServerEnv();
  if (!looksConfigured(env.searchAdminKey)) {
    return { ok: false, error: "Search not configured", results: [], totalCount: 0 };
  }

  const indexName = env.searchIndexName || INDEX_NAME;
  const endpoint = `https://${env.searchServiceName}.search.windows.net/indexes/${encodeURIComponent(indexName)}/docs/search?api-version=2024-07-01`;

  // Build search request — hybrid if embedding succeeds, keyword-only fallback
  const embedding = await getQueryEmbedding(query);

  const searchBody: Record<string, unknown> = {
    search: query,
    top,
    count: true,
    select:
      "id,doc_type,person_id,display_name,email,location,lifecycle_stage,content,giving_total,order_count,event_count,has_subscription,tags_text",
  };

  if (filter) {
    searchBody.filter = filter;
  }

  if (embedding) {
    searchBody.vectorQueries = [
      {
        vector: embedding,
        fields: "content_vector",
        kind: "vector",
        k: top,
      },
    ];
  }

  try {
    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "api-key": env.searchAdminKey!,
      },
      body: JSON.stringify(searchBody),
      cache: "no-store",
    });

    if (!response.ok) {
      const errText = await response.text().catch(() => "");
      return {
        ok: false,
        error: `Search returned ${response.status}: ${errText.slice(0, 200)}`,
        results: [],
        totalCount: 0,
      };
    }

    const body = (await response.json()) as {
      value?: Array<Record<string, unknown>>;
      "@odata.count"?: number;
    };

    const docs = body.value ?? [];
    const results: PersonProfile[] = docs.map((doc) => ({
      person_id: (doc.person_id as number) ?? -1,
      display_name: (doc.display_name as string) ?? "Unknown",
      email: (doc.email as string) ?? "",
      location: (doc.location as string) ?? "",
      lifecycle_stage: (doc.lifecycle_stage as string) ?? "",
      content: (doc.content as string) ?? "",
      giving_total: (doc.giving_total as number) ?? 0,
      order_count: (doc.order_count as number) ?? 0,
      event_count: (doc.event_count as number) ?? 0,
      has_subscription: (doc.has_subscription as boolean) ?? false,
      score: (doc["@search.score"] as number) ?? 0,
    }));

    return {
      ok: true,
      results,
      totalCount: body["@odata.count"] ?? results.length,
    };
  } catch (err) {
    return {
      ok: false,
      error: err instanceof Error ? err.message : "Search failed",
      results: [],
      totalCount: 0,
    };
  }
}
