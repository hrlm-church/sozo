const { withDb } = require('./_db');

async function main() {
  await withDb(async (pool) => {
    await pool.request().batch(`
-- Keep bronze + lineage immutable. Reset only derived layers.
DELETE FROM gold_intel.next_best_action;
DELETE FROM gold_intel.microsegment_membership;
DELETE FROM gold_intel.model_score;
DELETE FROM gold_intel.feature_snapshot;
DELETE FROM gold_intel.sentiment_signal;

DELETE FROM gold.signal_fact;
DELETE FROM silver.person_tag_signal;

DELETE FROM gold.communication_touch;
DELETE FROM gold.consent_preferences;
DELETE FROM gold.engagement_activity;
DELETE FROM gold.fund;
DELETE FROM gold.campaign;
DELETE FROM gold.order_line;
DELETE FROM gold.[order];
DELETE FROM gold.invoice_line;
DELETE FROM gold.invoice;
DELETE FROM gold.recurring_plan;
DELETE FROM gold.payment_transaction;
DELETE FROM gold.donation_transaction;
DELETE FROM gold.relationship;
DELETE FROM gold.contact_point;
DELETE FROM gold.identity_resolution;
DELETE FROM gold.crosswalk;
DELETE FROM gold.subscription_shipment;
DELETE FROM gold.subscription_status_history;
DELETE FROM gold.subscription_contract;
DELETE FROM gold.support_case;
DELETE FROM gold.designation;
DELETE FROM gold.appeal;
DELETE FROM gold.refund_chargeback;
DELETE FROM gold.pledge_commitment;
DELETE FROM gold.ticket_sale;
DELETE FROM gold.event;
DELETE FROM gold.organization;
DELETE FROM gold.household;
DELETE FROM gold.person;
DELETE FROM gold.source_file_lineage;

DELETE FROM silver.engagement_source;
DELETE FROM silver.transaction_source;
DELETE FROM silver.person_source;
`);

    const counts = await pool.request().query(`
SELECT
  (SELECT COUNT(1) FROM silver.person_source) AS silver_person_source,
  (SELECT COUNT(1) FROM silver.transaction_source) AS silver_transaction_source,
  (SELECT COUNT(1) FROM silver.engagement_source) AS silver_engagement_source,
  (SELECT COUNT(1) FROM gold.person) AS gold_person,
  (SELECT COUNT(1) FROM gold.crosswalk) AS gold_crosswalk,
  (SELECT COUNT(1) FROM gold.signal_fact) AS gold_signal_fact;
`);

    console.log('OK: derived layers reset.');
    console.log(JSON.stringify(counts.recordset[0], null, 2));
  });
}

main().catch((err) => {
  console.error('ERROR:', err.message);
  process.exit(1);
});
