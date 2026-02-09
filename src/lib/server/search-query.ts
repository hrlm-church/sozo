import { getServerEnv, looksConfigured } from "@/lib/server/env";
import type { Citation, TableArtifact } from "@/lib/server/dashboard-summary";

interface SearchDoc {
  [key: string]: unknown;
}

interface SearchResult {
  ok: boolean;
  reason?: string;
  indexName?: string;
  citations: Citation[];
  table?: TableArtifact;
}

const pickTitle = (doc: SearchDoc, fallback: string) => {
  const candidates = ["title", "name", "id", "key", "documentId"];
  for (const key of candidates) {
    const value = doc[key];
    if (typeof value === "string" && value.trim()) {
      return value;
    }
  }
  return fallback;
};

const pickSnippet = (doc: SearchDoc) => {
  const candidates = ["content", "text", "summary", "description"];
  for (const key of candidates) {
    const value = doc[key];
    if (typeof value === "string" && value.trim()) {
      return value.slice(0, 220);
    }
  }
  return "Search result matched query text.";
};

const selectIndex = async (): Promise<string | null> => {
  const env = getServerEnv();
  if (env.searchIndexName) {
    return env.searchIndexName;
  }

  const endpoint = `https://${env.searchServiceName}.search.windows.net/indexes?api-version=2024-07-01`;
  const response = await fetch(endpoint, {
    headers: {
      "api-key": env.searchAdminKey!,
    },
    cache: "no-store",
  });

  if (!response.ok) {
    return null;
  }

  const body = (await response.json()) as { value?: Array<{ name?: string }> };
  return body.value?.[0]?.name ?? null;
};

export const runSearchQuery = async (queryText: string): Promise<SearchResult> => {
  const env = getServerEnv();
  if (!looksConfigured(env.searchAdminKey)) {
    return {
      ok: false,
      reason: "SOZO_SEARCH_ADMIN_KEY is missing",
      citations: [],
    };
  }

  const indexName = await selectIndex();
  if (!indexName) {
    return {
      ok: false,
      reason: "No Azure AI Search index found. Set SOZO_SEARCH_INDEX_NAME or create an index.",
      citations: [],
    };
  }

  const endpoint = `https://${env.searchServiceName}.search.windows.net/indexes/${encodeURIComponent(
    indexName,
  )}/docs/search?api-version=2024-07-01`;

  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "api-key": env.searchAdminKey!,
    },
    body: JSON.stringify({
      search: queryText,
      top: 5,
      queryType: "simple",
      count: true,
    }),
    cache: "no-store",
  });

  if (!response.ok) {
    return {
      ok: false,
      reason: `Azure Search returned ${response.status}`,
      citations: [],
    };
  }

  const body = (await response.json()) as {
    value?: SearchDoc[];
  };

  const docs = body.value ?? [];
  const citations = docs.slice(0, 3).map((doc, index) => ({
    title: pickTitle(doc, `Search match #${index + 1}`),
    source: `Azure AI Search (${indexName})`,
    snippet: pickSnippet(doc),
  }));

  const table: TableArtifact = {
    id: "search-results",
    title: "Search Matches",
    columns: ["Title", "Key Fields"],
    rows: docs.slice(0, 5).map((doc, index) => {
      const title = pickTitle(doc, `Result ${index + 1}`);
      const fields = Object.entries(doc)
        .slice(0, 4)
        .map(([key, value]) => `${key}=${String(value)}`)
        .join("; ");
      return [title, fields];
    }),
  };

  return {
    ok: true,
    indexName,
    citations,
    table,
  };
};
