const { withDb } = require('./_db');

async function main() {
  await withDb(async (pool) => {
    await pool.request().batch(`
DECLARE @snapshot_date DATE = CAST(SYSUTCDATETIME() AS DATE);

DELETE FROM gold_intel.feature_snapshot WHERE snapshot_date = @snapshot_date;
DELETE FROM gold_intel.model_score WHERE snapshot_date = @snapshot_date;
DELETE FROM gold_intel.microsegment_membership WHERE snapshot_date = @snapshot_date;
DELETE FROM gold_intel.next_best_action WHERE snapshot_date = @snapshot_date;

;WITH p AS (
  SELECT person_id FROM gold.person
),
pay AS (
  SELECT person_id,
         COUNT(1) AS payment_count,
         SUM(COALESCE(amount,0)) AS payment_amount,
         MAX(payment_ts) AS last_payment_ts
  FROM gold.payment_transaction
  WHERE person_id IS NOT NULL
  GROUP BY person_id
),
tix AS (
  SELECT person_id,
         SUM(COALESCE(tickets_purchased,0)) AS tickets_count,
         COUNT(1) AS ticket_txn_count
  FROM gold.ticket_sale
  WHERE person_id IS NOT NULL
  GROUP BY person_id
),
sub AS (
  SELECT person_id,
         COUNT(1) AS subscription_count,
         SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) AS active_subscriptions
  FROM gold.subscription_contract
  WHERE person_id IS NOT NULL
  GROUP BY person_id
),
eng AS (
  SELECT person_id,
         COUNT(1) AS engagement_count,
         MAX(activity_ts) AS last_engagement_ts
  FROM gold.engagement_activity
  WHERE person_id IS NOT NULL
  GROUP BY person_id
),
base AS (
  SELECT
    p.person_id,
    COALESCE(pay.payment_count,0) AS payment_count,
    COALESCE(pay.payment_amount,0) AS payment_amount,
    pay.last_payment_ts,
    COALESCE(tix.tickets_count,0) AS tickets_count,
    COALESCE(tix.ticket_txn_count,0) AS ticket_txn_count,
    COALESCE(sub.subscription_count,0) AS subscription_count,
    COALESCE(sub.active_subscriptions,0) AS active_subscriptions,
    COALESCE(eng.engagement_count,0) AS engagement_count,
    eng.last_engagement_ts,
    DATEDIFF(DAY, COALESCE(pay.last_payment_ts,'2000-01-01'), SYSUTCDATETIME()) AS days_since_payment,
    DATEDIFF(DAY, COALESCE(eng.last_engagement_ts,'2000-01-01'), SYSUTCDATETIME()) AS days_since_engagement
  FROM p
  LEFT JOIN pay ON pay.person_id=p.person_id
  LEFT JOIN tix ON tix.person_id=p.person_id
  LEFT JOIN sub ON sub.person_id=p.person_id
  LEFT JOIN eng ON eng.person_id=p.person_id
)
INSERT INTO gold_intel.feature_snapshot(snapshot_date,canonical_type,canonical_id,feature_name,feature_value_float,feature_value_text)
SELECT @snapshot_date,'person',b.person_id, f.feature_name, f.feature_value_float, f.feature_value_text
FROM base b
CROSS APPLY (VALUES
  ('payment_count_365d', CAST(b.payment_count AS FLOAT), NULL),
  ('payment_amount_365d', CAST(b.payment_amount AS FLOAT), NULL),
  ('days_since_payment', CAST(b.days_since_payment AS FLOAT), NULL),
  ('tickets_count_365d', CAST(b.tickets_count AS FLOAT), NULL),
  ('subscription_count', CAST(b.subscription_count AS FLOAT), NULL),
  ('active_subscriptions', CAST(b.active_subscriptions AS FLOAT), NULL),
  ('engagement_count_365d', CAST(b.engagement_count AS FLOAT), NULL),
  ('days_since_engagement', CAST(b.days_since_engagement AS FLOAT), NULL)
) f(feature_name, feature_value_float, feature_value_text);

;WITH base AS (
  SELECT
    p.person_id,
    COALESCE(MAX(CASE WHEN fs.feature_name='payment_amount_365d' THEN fs.feature_value_float END),0) AS payment_amount,
    COALESCE(MAX(CASE WHEN fs.feature_name='payment_count_365d' THEN fs.feature_value_float END),0) AS payment_count,
    COALESCE(MAX(CASE WHEN fs.feature_name='tickets_count_365d' THEN fs.feature_value_float END),0) AS tickets_count,
    COALESCE(MAX(CASE WHEN fs.feature_name='active_subscriptions' THEN fs.feature_value_float END),0) AS active_subscriptions,
    COALESCE(MAX(CASE WHEN fs.feature_name='days_since_payment' THEN fs.feature_value_float END),9999) AS days_since_payment,
    COALESCE(MAX(CASE WHEN fs.feature_name='days_since_engagement' THEN fs.feature_value_float END),9999) AS days_since_engagement
  FROM gold.person p
  LEFT JOIN gold_intel.feature_snapshot fs
    ON fs.canonical_type='person' AND fs.canonical_id=p.person_id AND fs.snapshot_date=@snapshot_date
  GROUP BY p.person_id
),
scored AS (
  SELECT
    person_id,
    -- Rule-based donor propensity (0-1)
    CAST(
      (CASE WHEN payment_amount >= 500 THEN 0.35 WHEN payment_amount >= 100 THEN 0.25 WHEN payment_amount > 0 THEN 0.15 ELSE 0 END) +
      (CASE WHEN payment_count >= 12 THEN 0.25 WHEN payment_count >= 4 THEN 0.15 WHEN payment_count > 0 THEN 0.08 ELSE 0 END) +
      (CASE WHEN days_since_payment <= 30 THEN 0.20 WHEN days_since_payment <= 90 THEN 0.12 WHEN days_since_payment <= 180 THEN 0.05 ELSE 0 END) +
      (CASE WHEN days_since_engagement <= 30 THEN 0.10 WHEN days_since_engagement <= 90 THEN 0.05 ELSE 0 END) +
      (CASE WHEN active_subscriptions > 0 THEN 0.10 ELSE 0 END)
    AS FLOAT) AS donor_score,
    -- Rule-based buyer propensity (subscription/event)
    CAST(
      (CASE WHEN tickets_count >= 4 THEN 0.30 WHEN tickets_count >= 1 THEN 0.18 ELSE 0 END) +
      (CASE WHEN active_subscriptions > 0 THEN 0.35 WHEN payment_count >= 2 THEN 0.15 ELSE 0 END) +
      (CASE WHEN days_since_payment <= 45 THEN 0.20 WHEN days_since_payment <= 120 THEN 0.10 ELSE 0 END) +
      (CASE WHEN days_since_engagement <= 30 THEN 0.15 WHEN days_since_engagement <= 90 THEN 0.08 ELSE 0 END)
    AS FLOAT) AS buyer_score
  FROM base
)
INSERT INTO gold_intel.model_score(snapshot_date,canonical_type,canonical_id,model_name,model_version,score,score_band,rationale)
SELECT
  @snapshot_date,
  'person',
  s.person_id,
  m.model_name,
  'v1-rules',
  CASE WHEN m.score > 1 THEN 1 ELSE m.score END,
  CASE WHEN m.score >= 0.75 THEN 'high' WHEN m.score >= 0.45 THEN 'medium' ELSE 'low' END,
  m.rationale
FROM scored s
CROSS APPLY (VALUES
  ('propensity_donate', s.donor_score, 'RFM + engagement + subscription indicators'),
  ('propensity_buy', s.buyer_score, 'Ticket/event + subscription + recency indicators')
) m(model_name, score, rationale);

INSERT INTO gold_intel.microsegment_membership(snapshot_date,canonical_type,canonical_id,segment_code,segment_name,reason_codes)
SELECT
  @snapshot_date,
  'person',
  person_id,
  segment_code,
  segment_name,
  reason_codes
FROM (
  SELECT
    ms.canonical_id AS person_id,
    CASE
      WHEN ms.model_name='propensity_donate' AND ms.score_band='high' THEN 'DONOR_HIGH_INTENT'
      WHEN ms.model_name='propensity_buy' AND ms.score_band='high' THEN 'BUYER_HIGH_INTENT'
      WHEN ms.model_name='propensity_buy' AND ms.score_band='medium' THEN 'BUYER_NURTURE'
      ELSE 'GENERAL_NURTURE'
    END AS segment_code,
    CASE
      WHEN ms.model_name='propensity_donate' AND ms.score_band='high' THEN 'High Intent Donor'
      WHEN ms.model_name='propensity_buy' AND ms.score_band='high' THEN 'High Intent Buyer'
      WHEN ms.model_name='propensity_buy' AND ms.score_band='medium' THEN 'Buyer Nurture'
      ELSE 'General Nurture'
    END AS segment_name,
    CONCAT(ms.model_name,':',ms.score_band) AS reason_codes,
    ROW_NUMBER() OVER (PARTITION BY ms.canonical_id ORDER BY ms.score DESC) AS rn
  FROM gold_intel.model_score ms
  WHERE ms.snapshot_date=@snapshot_date
) x
WHERE x.rn=1;

INSERT INTO gold_intel.next_best_action(snapshot_date,canonical_type,canonical_id,action_code,action_channel,expected_lift,confidence,rationale)
SELECT
  @snapshot_date,
  'person',
  m.canonical_id,
  CASE
    WHEN m.segment_code='DONOR_HIGH_INTENT' THEN 'ASK_DONATION_CAMPAIGN'
    WHEN m.segment_code='BUYER_HIGH_INTENT' THEN 'OFFER_SUBSCRIPTION_BOX_UPSELL'
    WHEN m.segment_code='BUYER_NURTURE' THEN 'SEND_EVENT_TO_BOX_JOURNEY'
    ELSE 'SEND_RELATIONSHIP_NURTURE'
  END,
  CASE
    WHEN m.segment_code IN ('DONOR_HIGH_INTENT','BUYER_HIGH_INTENT') THEN 'email_sms'
    ELSE 'email'
  END,
  CASE
    WHEN m.segment_code='DONOR_HIGH_INTENT' THEN 0.22
    WHEN m.segment_code='BUYER_HIGH_INTENT' THEN 0.18
    WHEN m.segment_code='BUYER_NURTURE' THEN 0.10
    ELSE 0.06
  END,
  CASE
    WHEN m.segment_code IN ('DONOR_HIGH_INTENT','BUYER_HIGH_INTENT') THEN 0.78
    WHEN m.segment_code='BUYER_NURTURE' THEN 0.65
    ELSE 0.52
  END,
  CONCAT('segment=',m.segment_code,' reason=',m.reason_codes)
FROM gold_intel.microsegment_membership m
WHERE m.snapshot_date=@snapshot_date;
`);

    const counts = await pool.request().query(`
DECLARE @snapshot_date DATE = CAST(SYSUTCDATETIME() AS DATE);
SELECT
  (SELECT COUNT(1) FROM gold_intel.feature_snapshot WHERE snapshot_date=@snapshot_date) AS feature_rows,
  (SELECT COUNT(1) FROM gold_intel.model_score WHERE snapshot_date=@snapshot_date) AS model_rows,
  (SELECT COUNT(1) FROM gold_intel.microsegment_membership WHERE snapshot_date=@snapshot_date) AS segment_rows,
  (SELECT COUNT(1) FROM gold_intel.next_best_action WHERE snapshot_date=@snapshot_date) AS nba_rows;
`);

    console.log('OK: intelligence v1 generated', counts.recordset[0]);
  });
}

main().catch((err) => {
  console.error('ERROR:', err.message);
  process.exit(1);
});
