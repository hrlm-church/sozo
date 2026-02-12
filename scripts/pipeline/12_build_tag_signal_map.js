const fs = require('fs');
const path = require('path');

const inputPath = process.argv[2] || 'reports/catalog/DONOR_DIRECT_ALL_TAGS_FROM_TAGS_COLUMN.tsv';
const outputDir = process.argv[3] || 'reports/catalog';

const SIGNAL_GROUPS = [
  'identity_profile',
  'demographics_household',
  'contact_channel',
  'consent_privacy',
  'fundraising_giving',
  'recurring_commitment',
  'commerce_orders',
  'billing_invoicing',
  'payments_financial_events',
  'subscription_box_lifecycle',
  'event_ticketing_attendance',
  'campaign_marketing',
  'engagement_behavior',
  'journey_automation',
  'content_interest_topic',
  'support_service',
  'relationship_affiliation',
  'identity_resolution_quality',
  'system_ingestion_lineage',
  'data_quality_anomaly',
  'predictive_scores_segments',
  'experimental_holdout',
  'manual_curated_flags',
  'unknown_unmapped',
];

const parseTsv = (raw) => {
  const lines = raw.split(/\r?\n/).map((x) => x.trim()).filter(Boolean);
  if (!lines.length) return [];
  const header = lines[0].split('\t');
  const countIdx = header.indexOf('count');
  const tagIdx = header.indexOf('tag');
  if (countIdx < 0 || tagIdx < 0) {
    throw new Error(`Invalid input header. Expected: count\\ttag in ${inputPath}`);
  }
  return lines.slice(1).map((line) => {
    const cols = line.split('\t');
    return {
      count: Number(cols[countIdx] || 0),
      tag: cols[tagIdx] || '',
    };
  });
};

const includesAny = (text, parts) => parts.some((p) => text.includes(p));
const startsWithAny = (text, parts) => parts.some((p) => text.startsWith(p));

const classifyTag = (tag) => {
  const raw = String(tag || '').trim();
  const lower = raw.toLowerCase();
  const prefix = (raw.includes('->') ? raw.split('->')[0] : '').trim();
  const prefixLower = prefix.toLowerCase();

  const match = (signalGroup, confidence, rule) => ({
    signal_group: signalGroup,
    confidence,
    rule,
    needs_review: confidence < 0.8 ? 1 : 0,
  });

  if (!raw) return match('unknown_unmapped', 0.2, 'empty_tag');

  if (prefixLower === 'imported' || prefixLower === 'updated' || startsWithAny(lower, ['imported ->', 'updated ->'])) {
    return match('system_ingestion_lineage', 0.98, 'prefix_imported_or_updated');
  }

  if (prefixLower === 'donor assignment' || includesAny(lower, ['donor', 'giving total', 'fundraising', 'tax letter'])) {
    return match('fundraising_giving', 0.92, 'donor_fundraising_keywords');
  }

  if (includesAny(lower, ['subscription', 'box', 'shipstation', 'renewal', 'cancel', 'paused'])) {
    return match('subscription_box_lifecycle', 0.95, 'subscription_box_keywords');
  }

  if (includesAny(lower, ['recurring gift', 'monthly', 'yearly physical'])) {
    return match('recurring_commitment', 0.9, 'recurring_commitment_keywords');
  }

  if (includesAny(lower, ['invoice', 'payplan', 'payment plan'])) {
    return match('billing_invoicing', 0.9, 'billing_keywords');
  }

  if (includesAny(lower, ['chargeback', 'refund', 'declined', 'credit card expiring'])) {
    return match('payments_financial_events', 0.9, 'payment_event_keywords');
  }

  if (includesAny(lower, ['order', 'shopify', 'store member', 'purchase'])) {
    return match('commerce_orders', 0.86, 'order_commerce_keywords');
  }

  if (includesAny(lower, ['event', 'tour', 'ticket', 'registered', 'workshop', 'webinar', 'live', 'conference', 'farm'])) {
    return match('event_ticketing_attendance', 0.9, 'event_ticket_keywords');
  }

  if (includesAny(lower, ['newsletter', 'campaign', 'marketing', 'mailchimp', 'email blast', 'open', 'text message', 'vm'])) {
    return match('campaign_marketing', 0.88, 'campaign_marketing_keywords');
  }

  if (prefixLower === 'nurture tags' || includesAny(lower, ['nurture', 'journey', 'flash sale'])) {
    return match('journey_automation', 0.89, 'nurture_journey_keywords');
  }

  if (includesAny(lower, ['clicked', 'attendee email rec', 'unable to deliver', 'invalid sms'])) {
    return match('engagement_behavior', 0.84, 'engagement_behavior_keywords');
  }

  if (includesAny(lower, ['consent', 'privacy', 'opt in', 'opt out', 'do not contact'])) {
    return match('consent_privacy', 0.9, 'consent_privacy_keywords');
  }

  if (includesAny(lower, ['mom', 'dad', 'daughter', 'son', 'family', 'guardian'])) {
    return match('demographics_household', 0.8, 'family_household_keywords');
  }

  if (includesAny(lower, ['church', 'partner', 'ministry', 'promoter', 'affiliate'])) {
    return match('relationship_affiliation', 0.83, 'affiliation_keywords');
  }

  if (includesAny(lower, ['score', 'high intent', 'propensity', 'segment'])) {
    return match('predictive_scores_segments', 0.86, 'scoring_segmentation_keywords');
  }

  if (includesAny(lower, ['test', 'holdout', 'control'])) {
    return match('experimental_holdout', 0.86, 'experiment_keywords');
  }

  if (includesAny(lower, ['error', 'duplicate', 'mismatch', 'invalid'])) {
    return match('data_quality_anomaly', 0.85, 'data_quality_keywords');
  }

  if (includesAny(lower, ['support', 'case', 'ticket #', 'help'])) {
    return match('support_service', 0.88, 'support_keywords');
  }

  if (prefixLower === 'no category' || prefixLower === 'smart lists' || prefixLower === 'prospect tags') {
    return match('manual_curated_flags', 0.72, 'manual_or_legacy_prefix');
  }

  if (prefixLower === 'true productions' || prefixLower === 'true girl' || prefixLower === 'lies moms believe') {
    return match('content_interest_topic', 0.84, 'program_content_prefix');
  }

  if (prefixLower === 'customer tags' || prefixLower === 'tracking campaign') {
    return match('campaign_marketing', 0.8, 'customer_tracking_prefix');
  }

  if (prefixLower === 'memberships') {
    return match('recurring_commitment', 0.78, 'membership_prefix');
  }

  return match('unknown_unmapped', 0.4, 'fallback_unknown');
};

const main = () => {
  if (!fs.existsSync(inputPath)) {
    throw new Error(`Input not found: ${inputPath}`);
  }

  const rows = parseTsv(fs.readFileSync(inputPath, 'utf8'));
  const mapped = rows.map((row) => {
    const cls = classifyTag(row.tag);
    return {
      count: row.count,
      tag: row.tag,
      tag_prefix: row.tag.includes('->') ? row.tag.split('->')[0].trim() : 'Unscoped',
      signal_group: cls.signal_group,
      confidence: cls.confidence,
      needs_review: cls.needs_review,
      rule: cls.rule,
    };
  });

  const byGroup = new Map();
  for (const row of mapped) {
    const cur = byGroup.get(row.signal_group) || { tag_count: 0, row_count_sum: 0 };
    cur.tag_count += 1;
    cur.row_count_sum += row.count;
    byGroup.set(row.signal_group, cur);
  }

  const groupSummary = SIGNAL_GROUPS.map((group) => ({
    signal_group: group,
    tag_count: (byGroup.get(group) || { tag_count: 0 }).tag_count || 0,
    row_count_sum: (byGroup.get(group) || { row_count_sum: 0 }).row_count_sum || 0,
  }));

  const reviewRows = mapped.filter((r) => r.needs_review === 1);

  fs.mkdirSync(outputDir, { recursive: true });
  const mappingJson = path.join(outputDir, 'TAG_SIGNAL_GROUP_MAPPING.json');
  const mappingTsv = path.join(outputDir, 'TAG_SIGNAL_GROUP_MAPPING.tsv');
  const summaryMd = path.join(outputDir, 'TAG_SIGNAL_GROUP_MAPPING_SUMMARY.md');
  const reviewTsv = path.join(outputDir, 'TAG_SIGNAL_GROUP_REVIEW_QUEUE.tsv');

  fs.writeFileSync(mappingJson, JSON.stringify({ generated_at: new Date().toISOString(), total_tags: mapped.length, signal_groups: SIGNAL_GROUPS, group_summary: groupSummary, mapping: mapped }, null, 2));
  fs.writeFileSync(
    mappingTsv,
    ['count\ttag\ttag_prefix\tsignal_group\tconfidence\tneeds_review\trule']
      .concat(mapped.map((r) => `${r.count}\t${r.tag}\t${r.tag_prefix}\t${r.signal_group}\t${r.confidence}\t${r.needs_review}\t${r.rule}`))
      .join('\n') + '\n'
  );
  fs.writeFileSync(
    reviewTsv,
    ['count\ttag\ttag_prefix\tsignal_group\tconfidence\trule']
      .concat(reviewRows.map((r) => `${r.count}\t${r.tag}\t${r.tag_prefix}\t${r.signal_group}\t${r.confidence}\t${r.rule}`))
      .join('\n') + '\n'
  );

  let md = '# Tag -> Canonical Signal Group Mapping\n\n';
  md += `- Input: \`${inputPath}\`\n`;
  md += `- Total distinct tags mapped: **${mapped.length}**\n`;
  md += `- Review queue size (confidence < 0.80): **${reviewRows.length}**\n\n`;
  md += '## Group Coverage\n\n';
  md += '| Signal group | Distinct tags | Weighted rows |\n|---|---:|---:|\n';
  for (const g of groupSummary) {
    md += `| ${g.signal_group} | ${g.tag_count} | ${g.row_count_sum} |\n`;
  }
  md += '\n## Output Files\n\n';
  md += `- \`${mappingJson}\`\n`;
  md += `- \`${mappingTsv}\`\n`;
  md += `- \`${reviewTsv}\`\n`;
  fs.writeFileSync(summaryMd, md);

  console.log(JSON.stringify({
    total_tags: mapped.length,
    review_queue: reviewRows.length,
    outputs: { mappingJson, mappingTsv, reviewTsv, summaryMd },
    top_groups: groupSummary.sort((a, b) => b.tag_count - a.tag_count).slice(0, 10),
  }, null, 2));
};

try {
  main();
} catch (err) {
  console.error(`ERROR: ${err.message}`);
  process.exit(1);
}
