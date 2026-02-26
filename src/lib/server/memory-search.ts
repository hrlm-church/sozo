/**
 * Semantic search over conversation memory using Azure AI Search.
 * Searches the sozo-memory-v1 index for relevant past conversation summaries.
 */
import { getServerEnv, looksConfigured } from "@/lib/server/env";
import { getQueryEmbedding } from "@/lib/server/search-client";

const MEMORY_INDEX_NAME = "sozo-memory-v1";

export interface MemorySearchResult {
  ok: boolean;
  error?: string;
  results: MemoryDocument[];
}

export interface MemoryDocument {
  id: string;
  conversation_id: string;
  title: string;
  content: string;
  topics: string;
  category: string;
  confidence: number;
  created_at: string;
  score: number;
}

/**
 * Hybrid search (keyword + vector) against the conversation memory index.
 * Filters by owner_email to ensure per-user isolation.
 */
export async function memorySearch(
  query: string,
  ownerEmail: string,
  top: number = 5,
): Promise<MemorySearchResult> {
  const env = getServerEnv();
  if (!looksConfigured(env.searchAdminKey)) {
    return { ok: false, error: "Search not configured", results: [] };
  }

  const endpoint = `https://${env.searchServiceName}.search.windows.net/indexes/${MEMORY_INDEX_NAME}/docs/search?api-version=2024-07-01`;
  const embedding = await getQueryEmbedding(query);

  const searchBody: Record<string, unknown> = {
    search: query,
    top,
    count: true,
    select: "id,conversation_id,title,content,topics,category,confidence,created_at",
    filter: `owner_email eq '${ownerEmail}'`,
    orderby: "created_at desc",
  };

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
        error: `Memory search returned ${response.status}: ${errText.slice(0, 200)}`,
        results: [],
      };
    }

    const body = (await response.json()) as {
      value?: Array<Record<string, unknown>>;
    };

    const docs = body.value ?? [];
    const results: MemoryDocument[] = docs.map((doc) => ({
      id: (doc.id as string) ?? "",
      conversation_id: (doc.conversation_id as string) ?? "",
      title: (doc.title as string) ?? "Untitled",
      content: (doc.content as string) ?? "",
      topics: (doc.topics as string) ?? "",
      category: (doc.category as string) ?? "",
      confidence: (doc.confidence as number) ?? 0,
      created_at: (doc.created_at as string) ?? "",
      score: (doc["@search.score"] as number) ?? 0,
    }));

    return { ok: true, results };
  } catch (err) {
    return {
      ok: false,
      error: err instanceof Error ? err.message : "Memory search failed",
      results: [],
    };
  }
}

/**
 * Upload a document to the memory search index (for conversation summaries).
 */
export async function uploadMemoryDocument(doc: {
  id: string;
  owner_email: string;
  conversation_id: string;
  title: string;
  content: string;
  content_vector: number[];
  topics: string;
  category: string;
  confidence: number;
  created_at: string;
}): Promise<{ ok: boolean; error?: string }> {
  const env = getServerEnv();
  if (!looksConfigured(env.searchAdminKey)) {
    return { ok: false, error: "Search not configured" };
  }

  const endpoint = `https://${env.searchServiceName}.search.windows.net/indexes/${MEMORY_INDEX_NAME}/docs/index?api-version=2024-07-01`;

  try {
    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "api-key": env.searchAdminKey!,
      },
      body: JSON.stringify({
        value: [{ ...doc, "@search.action": "mergeOrUpload" }],
      }),
    });

    if (!response.ok) {
      const errText = await response.text().catch(() => "");
      return { ok: false, error: `Upload failed (${response.status}): ${errText.slice(0, 200)}` };
    }

    return { ok: true };
  } catch (err) {
    return {
      ok: false,
      error: err instanceof Error ? err.message : "Memory upload failed",
    };
  }
}
