/**
 * Keap (Infusionsoft) API client for write-back operations.
 * Uses the Keap REST API v2 with a Personal Access Token.
 */

const KEAP_API_BASE = "https://api.infusionsoft.com/crm/rest/v2";

function getAccessToken(): string | null {
  return process.env.KEAP_ACCESS_TOKEN ?? null;
}

async function keapFetch(
  path: string,
  method: "GET" | "POST" | "PUT" | "PATCH" = "GET",
  body?: Record<string, unknown>,
): Promise<{ ok: boolean; data?: unknown; error?: string; status: number }> {
  const token = getAccessToken();
  if (!token) {
    return { ok: false, error: "KEAP_ACCESS_TOKEN not configured", status: 0 };
  }

  try {
    const res = await fetch(`${KEAP_API_BASE}${path}`, {
      method,
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: body ? JSON.stringify(body) : undefined,
    });

    const text = await res.text();
    const data = text ? JSON.parse(text) : null;

    if (!res.ok) {
      return {
        ok: false,
        error: data?.message ?? `Keap API error: ${res.status}`,
        status: res.status,
        data,
      };
    }

    return { ok: true, data, status: res.status };
  } catch (error) {
    return {
      ok: false,
      error: error instanceof Error ? error.message : "Keap API request failed",
      status: 0,
    };
  }
}

/**
 * Apply a tag to a contact in Keap.
 */
export async function applyTag(
  contactId: number,
  tagId: number,
): Promise<{ ok: boolean; error?: string }> {
  const result = await keapFetch(`/contacts/${contactId}/tags`, "POST", {
    tagIds: [tagId],
  });
  return { ok: result.ok, error: result.error };
}

/**
 * Remove a tag from a contact in Keap.
 */
export async function removeTag(
  contactId: number,
  tagId: number,
): Promise<{ ok: boolean; error?: string }> {
  const result = await keapFetch(`/contacts/${contactId}/tags/${tagId}`, "PATCH", {});
  return { ok: result.ok, error: result.error };
}

/**
 * Create a note on a contact in Keap.
 */
export async function createNote(
  contactId: number,
  title: string,
  body: string,
): Promise<{ ok: boolean; noteId?: number; error?: string }> {
  const result = await keapFetch(`/contacts/${contactId}/notes`, "POST", {
    title,
    body,
    contact_id: contactId,
  });

  if (!result.ok) return { ok: false, error: result.error };
  const data = result.data as Record<string, unknown> | null;
  return { ok: true, noteId: data?.id as number | undefined };
}

/**
 * Search for a tag by name in Keap.
 */
export async function searchTag(
  tagName: string,
): Promise<{ ok: boolean; tags?: { id: number; name: string }[]; error?: string }> {
  const result = await keapFetch(`/tags?filter=name==${encodeURIComponent(tagName)}&limit=10`);
  if (!result.ok) return { ok: false, error: result.error };
  const data = result.data as { tags?: { id: number; name: string }[] } | null;
  return { ok: true, tags: data?.tags ?? [] };
}

export function isKeapConfigured(): boolean {
  return !!getAccessToken();
}
