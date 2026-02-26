/**
 * POST /api/conversation/process
 *
 * Post-conversation processing: extracts summary, knowledge items, and topics
 * from a completed conversation, then embeds and indexes for future retrieval.
 *
 * Called fire-and-forget by the client after conversation save.
 */
import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";
import {
  extractConversationMemory,
  buildTranscript,
} from "@/lib/server/memory-processor";
import { saveConversationSummary, saveKnowledge } from "@/lib/server/memory";
import { getQueryEmbedding } from "@/lib/server/search-client";
import { uploadMemoryDocument } from "@/lib/server/memory-search";

export const dynamic = "force-dynamic";
export const maxDuration = 30; // Allow up to 30s for this background-ish work

function esc(val: string): string {
  return val.replace(/'/g, "''");
}

interface ProcessRequest {
  conversationId: string;
}

export async function POST(request: Request) {
  try {
    const ownerEmail = (await getSessionEmail()) ?? "anonymous@sozo.local";
    const body = (await request.json()) as ProcessRequest;
    const { conversationId } = body;

    if (!conversationId) {
      return NextResponse.json(
        { error: "conversationId required" },
        { status: 400 },
      );
    }

    // Check if already processed recently (debounce)
    const existing = await executeSql(`
      SELECT updated_at FROM sozo.conversation_summary
      WHERE id = '${esc(conversationId)}'
    `);
    const conv = await executeSql(`
      SELECT updated_at, title FROM sozo.conversation
      WHERE id = '${esc(conversationId)}' AND owner_email = N'${esc(ownerEmail)}'
    `);

    if (!conv.ok || conv.rows.length === 0) {
      return NextResponse.json(
        { error: "Conversation not found" },
        { status: 404 },
      );
    }

    // Skip if summary is newer than or equal to conversation update
    if (existing.ok && existing.rows.length > 0) {
      const summaryUpdated = new Date(
        existing.rows[0].updated_at as string,
      ).getTime();
      const convUpdated = new Date(
        conv.rows[0].updated_at as string,
      ).getTime();
      if (summaryUpdated >= convUpdated) {
        return NextResponse.json({ ok: true, skipped: true, reason: "already processed" });
      }
    }

    const convTitle = (conv.rows[0].title as string) || "Untitled";

    // 1. Load conversation messages
    const msgResult = await executeSql(`
      SELECT role, content_json
      FROM sozo.conversation_message
      WHERE conversation_id = '${esc(conversationId)}'
      ORDER BY created_at ASC
    `);

    if (!msgResult.ok || msgResult.rows.length < 3) {
      // Need at least greeting + user message + assistant response
      return NextResponse.json({ ok: true, skipped: true, reason: "too few messages" });
    }

    // 2. Build transcript
    const transcript = buildTranscript(
      msgResult.rows as Array<{ role: string; content_json: string }>,
    );

    if (transcript.length < 50) {
      return NextResponse.json({ ok: true, skipped: true, reason: "transcript too short" });
    }

    // 3. Extract memory via gpt-4o-mini
    const extraction = await extractConversationMemory(transcript);
    if (!extraction) {
      return NextResponse.json({ ok: false, error: "Extraction failed" }, { status: 500 });
    }

    // 4. Extract SQL queries from transcript for reference
    const sqlPatterns = transcript
      .match(/SELECT\s+.{10,}?(?:FROM|$)/gi)
      ?.slice(0, 5)
      ?.map((s) => s.slice(0, 200)) ?? [];

    // 5. Save conversation summary to SQL
    const summaryResult = await saveConversationSummary(
      conversationId,
      ownerEmail,
      convTitle,
      extraction.summary,
      extraction.topics,
      sqlPatterns,
      msgResult.rows.length,
    );

    if (!summaryResult.ok) {
      console.error("[process] Summary save failed:", summaryResult.error);
    }

    // 6. Embed and upload to search index
    const embedding = await getQueryEmbedding(extraction.summary);
    if (embedding) {
      const uploadResult = await uploadMemoryDocument({
        id: `conv-${conversationId}`,
        owner_email: ownerEmail,
        conversation_id: conversationId,
        title: convTitle,
        content: extraction.summary,
        content_vector: embedding,
        topics: JSON.stringify(extraction.topics),
        category: "conversation_summary",
        confidence: 1.0,
        created_at: new Date().toISOString(),
      });

      if (!uploadResult.ok) {
        console.error("[process] Search upload failed:", uploadResult.error);
      }
    }

    // 7. Save extracted knowledge items
    let knowledgeCount = 0;
    for (const item of extraction.knowledge) {
      const result = await saveKnowledge(
        ownerEmail,
        item.category,
        item.content,
        item.confidence,
        undefined,
        conversationId,
      );
      if (result.ok) knowledgeCount++;
    }

    console.log(
      `[process] Conversation ${conversationId}: summary saved, ${knowledgeCount} knowledge items, ${extraction.topics.length} topics`,
    );

    return NextResponse.json({
      ok: true,
      summary: extraction.summary.slice(0, 200),
      topics: extraction.topics,
      knowledgeCount,
    });
  } catch (error) {
    console.error("[process] Error:", error);
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Processing failed",
      },
      { status: 500 },
    );
  }
}
