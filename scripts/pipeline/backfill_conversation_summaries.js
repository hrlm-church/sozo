/**
 * Backfill conversation summaries for existing conversations.
 * Processes all unprocessed conversations through the memory extraction pipeline.
 *
 * Usage: node scripts/pipeline/backfill_conversation_summaries.js [--from=N]
 */
const fs = require('fs');
const path = require('path');
const sql = require('mssql');

function loadEnv() {
  const p = path.join(process.cwd(), '.env.local');
  for (const line of fs.readFileSync(p, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
}

const EXTRACTION_PROMPT = `You are a conversation analyst for Sozo, a ministry intelligence platform.
Analyze this conversation and extract:

1. SUMMARY: A 2-3 sentence natural language summary of what was discussed and discovered. Include specific names, numbers, and findings.

2. TOPICS: An array of 3-8 topic tags. Use lowercase terms like "donor retention", "year-end giving", "top donors", "event attendance", "subscription churn", "commerce trends", "wealth screening".

3. KNOWLEDGE: An array of permanent learnings (0-5 items). Each has:
   - category: "correction" | "preference" | "pattern" | "fact" | "persona"
   - content: 1-2 sentences
   - confidence: 0.0-1.0

Rules:
- "correction": ONLY when the user explicitly corrected the AI
- "preference": How user wants data shown
- "pattern": Reusable data patterns
- "fact": Organizational facts not in the database
- "persona": About the user
- Keep to 0-5 items. Most conversations produce 0-2.

Respond with ONLY valid JSON:
{"summary": "...", "topics": ["..."], "knowledge": [{"category": "...", "content": "...", "confidence": 0.9}]}`;

async function extractMemory(transcript) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) throw new Error('OPENAI_API_KEY required');

  const response = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
    body: JSON.stringify({
      model: 'gpt-4o-mini',
      messages: [
        { role: 'system', content: EXTRACTION_PROMPT },
        { role: 'user', content: transcript.slice(0, 12000) },
      ],
      temperature: 0.3,
      max_tokens: 1000,
      response_format: { type: 'json_object' },
    }),
  });

  if (!response.ok) throw new Error(`OpenAI ${response.status}`);
  const body = await response.json();
  return JSON.parse(body.choices[0].message.content);
}

async function getEmbedding(text) {
  const apiKey = process.env.OPENAI_API_KEY;
  const response = await fetch('https://api.openai.com/v1/embeddings', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
    body: JSON.stringify({ model: 'text-embedding-3-small', input: text.slice(0, 8000) }),
  });
  if (!response.ok) return null;
  const body = await response.json();
  return body.data?.[0]?.embedding ?? null;
}

function buildTranscript(messages) {
  const lines = [];
  for (const msg of messages) {
    if (msg.role !== 'user' && msg.role !== 'assistant') continue;
    let text = '';
    try {
      const parsed = JSON.parse(msg.content_json);
      if (typeof parsed.content === 'string') text = parsed.content;
      else if (Array.isArray(parsed.content)) {
        text = parsed.content.filter(p => p.type === 'text').map(p => p.text || '').join(' ');
      } else if (Array.isArray(parsed.parts)) {
        text = parsed.parts.filter(p => p.type === 'text').map(p => p.text || '').join(' ');
      } else if (typeof parsed === 'string') text = parsed;
    } catch { text = msg.content_json?.slice(0, 500) || ''; }
    if (!text || text === '[GREETING]') continue;
    if (text.length > 2000) text = text.slice(0, 2000) + '...';
    lines.push(`${msg.role === 'user' ? 'User' : 'Assistant'}: ${text}`);
  }
  return lines.join('\n\n');
}

function esc(s) { return s.replace(/'/g, "''"); }

async function main() {
  loadEnv();
  const fromArg = process.argv.find(a => a.startsWith('--from='));
  const fromIdx = fromArg ? parseInt(fromArg.split('=')[1]) : 0;

  const pool = await sql.connect({
    server: process.env.SOZO_SQL_HOST,
    database: 'sozov2',
    user: process.env.SOZO_SQL_USER,
    password: process.env.SOZO_SQL_PASSWORD,
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 30000,
  });

  const serviceName = process.env.SOZO_SEARCH_SERVICE_NAME;
  const adminKey = process.env.SOZO_SEARCH_ADMIN_KEY;

  // Get all conversations not yet processed
  const convResult = await pool.request().query(`
    SELECT c.id, c.title, c.owner_email, c.message_count
    FROM sozo.conversation c
    LEFT JOIN sozo.conversation_summary cs ON cs.id = c.id
    WHERE cs.id IS NULL AND c.message_count >= 3
    ORDER BY c.created_at ASC
  `);

  const conversations = convResult.recordset;
  console.log(`Found ${conversations.length} unprocessed conversations (starting from ${fromIdx})\n`);

  let processed = 0, skipped = 0, failed = 0;

  for (let i = fromIdx; i < conversations.length; i++) {
    const conv = conversations[i];
    console.log(`[${i + 1}/${conversations.length}] ${conv.title} (${conv.message_count} msgs)...`);

    try {
      // Load messages
      const msgResult = await pool.request().query(`
        SELECT role, content_json FROM sozo.conversation_message
        WHERE conversation_id = '${esc(conv.id)}' ORDER BY created_at ASC
      `);

      if (msgResult.recordset.length < 3) {
        console.log('  Skipped (too few messages)');
        skipped++;
        continue;
      }

      // Build transcript
      const transcript = buildTranscript(msgResult.recordset);
      if (transcript.length < 50) {
        console.log('  Skipped (transcript too short)');
        skipped++;
        continue;
      }

      // Extract via gpt-4o-mini
      const extraction = await extractMemory(transcript);
      console.log(`  Summary: ${extraction.summary.slice(0, 80)}...`);
      console.log(`  Topics: ${extraction.topics.join(', ')}`);
      console.log(`  Knowledge: ${extraction.knowledge.length} items`);

      // Save summary to SQL
      const topicsJson = JSON.stringify(extraction.topics);
      await pool.request().query(`
        INSERT INTO sozo.conversation_summary (id, owner_email, title, summary_text, topics, message_count)
        VALUES (
          '${esc(conv.id)}',
          N'${esc(conv.owner_email)}',
          N'${esc((conv.title || 'Untitled').slice(0, 256))}',
          N'${esc(extraction.summary)}',
          N'${esc(topicsJson)}',
          ${conv.message_count}
        )
      `);

      // Embed and upload to search index
      const embedding = await getEmbedding(extraction.summary);
      if (embedding && serviceName && adminKey) {
        const searchDoc = {
          id: `conv-${conv.id}`,
          owner_email: conv.owner_email,
          conversation_id: conv.id,
          title: conv.title || 'Untitled',
          content: extraction.summary,
          content_vector: embedding,
          topics: topicsJson,
          category: 'conversation_summary',
          confidence: 1.0,
          created_at: new Date().toISOString(),
          '@search.action': 'mergeOrUpload',
        };

        const uploadRes = await fetch(
          `https://${serviceName}.search.windows.net/indexes/sozo-memory-v1/docs/index?api-version=2024-07-01`,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'api-key': adminKey },
            body: JSON.stringify({ value: [searchDoc] }),
          },
        );
        if (!uploadRes.ok) console.log(`  Search upload: ${uploadRes.status}`);
      }

      // Save knowledge items
      for (const k of extraction.knowledge) {
        const kid = require('crypto').randomUUID();
        await pool.request().query(`
          INSERT INTO sozo.knowledge (id, owner_email, category, content, source_conv_id, confidence)
          VALUES (
            '${kid}',
            N'${esc(conv.owner_email)}',
            N'${esc(k.category)}',
            N'${esc(k.content.slice(0, 2000))}',
            '${esc(conv.id)}',
            ${Math.max(0, Math.min(1, k.confidence || 0.8))}
          )
        `);
      }

      processed++;
      // Rate limit protection
      await new Promise(r => setTimeout(r, 500));

    } catch (err) {
      console.log(`  FAILED: ${err.message}`);
      failed++;
    }
  }

  // Summary
  const summaryCount = await pool.request().query('SELECT COUNT(*) AS c FROM sozo.conversation_summary');
  const knowledgeCount = await pool.request().query('SELECT COUNT(*) AS c FROM sozo.knowledge');

  console.log(`\nDone. Processed: ${processed}, Skipped: ${skipped}, Failed: ${failed}`);
  console.log(`Total summaries: ${summaryCount.recordset[0].c}`);
  console.log(`Total knowledge items: ${knowledgeCount.recordset[0].c}`);

  await pool.close();
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
