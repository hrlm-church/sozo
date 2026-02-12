const { withDb } = require('./_db');

async function main() {
  await withDb(async (pool) => {
    await pool.request().batch(`
IF NOT EXISTS (SELECT 1 FROM meta.source_to_canonical_rule)
BEGIN
  INSERT INTO meta.source_to_canonical_rule(source_system,file_pattern,canonical_entity,rule_type,rule_description)
  VALUES
    ('keap','%Contact.csv','gold.person','deterministic','Map keap contacts to person identity and household.'),
    ('keap','%Orders known as Jobs.csv','gold.order','deterministic','Map keap orders/jobs to order and ticket_sale entities.'),
    ('keap','%Subscriptions known as JobRecurring.csv','gold.subscription_contract','deterministic','Map recurring job subscriptions to subscription contracts.'),
    ('donor_direct','%Kindful Donors - All Fields from Keap.csv','gold.person','deterministic','Map donor direct contact export to canonical person.'),
    ('donor_direct','%Transactions%.csv','gold.payment_transaction','deterministic','Map donor direct transactions into payment/donation entities.'),
    ('givebutter','%.csv','gold.payment_transaction','heuristic','Map Givebutter exports into payment and designation entities.'),
    ('stripe','%Customers.csv','gold.person','deterministic','Map stripe customers to person identity by email.'),
    ('stripe','%Charges%.csv','gold.payment_transaction','deterministic','Map stripe charges to payment and refunds.');
END;
`);

    await pool.request().batch(`
INSERT INTO gold.event(source_system,source_record_id,event_name,event_date,event_start_time,location_name,location_address,location_city_state,venue_promoter)
SELECT
  r.source_system,
  CONCAT(r.source_record_id,':event') AS source_record_id,
  COALESCE(
    JSON_VALUE(r.record_json,'$.MarketingEventURL'),
    JSON_VALUE(r.record_json,'$."How Did You Hear About this Event?"'),
    JSON_VALUE(r.record_json,'$.HowDidYouHearAboutthisEvent'),
    JSON_VALUE(r.record_json,'$.CampaignName'),
    'Event from source'
  ) AS event_name,
  TRY_CONVERT(date, COALESCE(
    JSON_VALUE(r.record_json,'$.TicketDate'),
    JSON_VALUE(r.record_json,'$."CHT Ticket Date"'),
    JSON_VALUE(r.record_json,'$."CHT Marketing Event Date"'),
    JSON_VALUE(r.record_json,'$."B2BB Ticket Date"'),
    JSON_VALUE(r.record_json,'$.B2BBTicketDate'),
    JSON_VALUE(r.record_json,'$.MarketingEventDate')
  )) AS event_date,
  COALESCE(
    JSON_VALUE(r.record_json,'$.EventStartTime'),
    JSON_VALUE(r.record_json,'$."CHT Event Start Time"'),
    JSON_VALUE(r.record_json,'$."CHT Marketing Event Start Time"'),
    JSON_VALUE(r.record_json,'$.MarketingEventStartTime')
  ) AS event_start_time,
  COALESCE(
    JSON_VALUE(r.record_json,'$.TicketLocationNameofChurchBuilding'),
    JSON_VALUE(r.record_json,'$."CHT Ticket Location - Name of Church/Building"'),
    JSON_VALUE(r.record_json,'$."B2BB Ticket location - Name of Church"')
  ) AS location_name,
  COALESCE(
    JSON_VALUE(r.record_json,'$.TicketLocationAddress'),
    JSON_VALUE(r.record_json,'$."CHT Ticket Location - Address"'),
    JSON_VALUE(r.record_json,'$."B2BB Ticket Location - Address"')
  ) AS location_address,
  COALESCE(
    JSON_VALUE(r.record_json,'$.TicketLocationCityState'),
    JSON_VALUE(r.record_json,'$."CHT Ticket Location - City, State"'),
    JSON_VALUE(r.record_json,'$."B2BB Ticket Location - City, State"')
  ) AS location_city_state,
  COALESCE(
    JSON_VALUE(r.record_json,'$.VenuePromoter'),
    JSON_VALUE(r.record_json,'$."Venue/Promoter"')
  ) AS venue_promoter
FROM bronze.raw_record r
WHERE (
  JSON_VALUE(r.record_json,'$.HowManyTicketsPurchased') IS NOT NULL
  OR JSON_VALUE(r.record_json,'$."How Many Tickets Purchased"') IS NOT NULL
  OR JSON_VALUE(r.record_json,'$.TicketDate') IS NOT NULL
  OR JSON_VALUE(r.record_json,'$."CHT Ticket Date"') IS NOT NULL
)
AND NOT EXISTS (
  SELECT 1 FROM gold.event e
  WHERE e.source_system = r.source_system
    AND e.source_record_id = CONCAT(r.source_record_id,':event')
);
`);

    await pool.request().batch(`
INSERT INTO gold.ticket_sale(event_id,person_id,source_system,source_record_id,tickets_purchased,gross_amount,coupon_code,referral_code,purchased_at)
SELECT
  e.event_id,
  cw.canonical_id,
  r.source_system,
  CONCAT(r.source_record_id,':ticket') AS source_record_id,
  TRY_CONVERT(int, COALESCE(
    JSON_VALUE(r.record_json,'$.HowManyTicketsPurchased'),
    JSON_VALUE(r.record_json,'$."How Many Tickets Purchased"')
  )) AS tickets_purchased,
  TRY_CONVERT(decimal(18,2), REPLACE(COALESCE(
    JSON_VALUE(r.record_json,'$.LastOrderTotal'),
    JSON_VALUE(r.record_json,'$.amount')
  ),',','')) AS gross_amount,
  COALESCE(
    JSON_VALUE(r.record_json,'$.CouponCodeUsed'),
    JSON_VALUE(r.record_json,'$."CHT Marketing Coupon"')
  ) AS coupon_code,
  COALESCE(
    JSON_VALUE(r.record_json,'$.ReferralCouponCode'),
    JSON_VALUE(r.record_json,'$."Referral Code"')
  ) AS referral_code,
  TRY_CONVERT(datetime2, COALESCE(
    JSON_VALUE(r.record_json,'$.LastPurchaseDate'),
    JSON_VALUE(r.record_json,'$.DateCreated'),
    JSON_VALUE(r.record_json,'$.created')
  )) AS purchased_at
FROM bronze.raw_record r
LEFT JOIN gold.crosswalk cw
  ON cw.canonical_type='person'
 AND cw.source_system=r.source_system
 AND cw.source_record_id=r.source_record_id
LEFT JOIN gold.event e
  ON e.source_system=r.source_system
 AND e.source_record_id=CONCAT(r.source_record_id,':event')
WHERE (
  JSON_VALUE(r.record_json,'$.HowManyTicketsPurchased') IS NOT NULL
  OR JSON_VALUE(r.record_json,'$."How Many Tickets Purchased"') IS NOT NULL
)
AND NOT EXISTS (
  SELECT 1 FROM gold.ticket_sale t
  WHERE t.source_system = r.source_system
    AND t.source_record_id = CONCAT(r.source_record_id,':ticket')
);
`);

    await pool.request().batch(`
INSERT INTO gold.subscription_contract(person_id,source_system,source_record_id,plan_type,start_date,renewal_date,next_box_month,quantity,is_gift,status)
SELECT
  cw.canonical_id,
  r.source_system,
  CONCAT(r.source_record_id,':sub') AS source_record_id,
  COALESCE(JSON_VALUE(r.record_json,'$.PlanType'),'Subscription Box') AS plan_type,
  TRY_CONVERT(date, COALESCE(
    JSON_VALUE(r.record_json,'$.SubscriberSince'),
    JSON_VALUE(r.record_json,'$."Annual Subscription Start/Bill Date"'),
    JSON_VALUE(r.record_json,'$.AnnualSubscriptionStartBillDate')
  )) AS start_date,
  TRY_CONVERT(date, COALESCE(
    JSON_VALUE(r.record_json,'$.SubscriptionBoxRenewalDate'),
    JSON_VALUE(r.record_json,'$."Physical Subscription Re-Start Date"'),
    JSON_VALUE(r.record_json,'$.PhysicalSubscriptionReStartDate')
  )) AS renewal_date,
  COALESCE(
    JSON_VALUE(r.record_json,'$.NextBoxMonth'),
    JSON_VALUE(r.record_json,'$."Next Box Month"')
  ) AS next_box_month,
  TRY_CONVERT(int, COALESCE(
    JSON_VALUE(r.record_json,'$.SubscriptionBoxQuantity'),
    JSON_VALUE(r.record_json,'$."Subscription Box Quantity"')
  )) AS quantity,
  CASE WHEN COALESCE(JSON_VALUE(r.record_json,'$.SubscriptionBoxGift'),JSON_VALUE(r.record_json,'$."Subscription Box Gift?"')) IN ('1','true','True','Yes','yes') THEN 1 ELSE 0 END AS is_gift,
  CASE
    WHEN COALESCE(JSON_VALUE(r.record_json,'$.PhysicalSubscriptionCancelDate'),JSON_VALUE(r.record_json,'$."Physical Subscription Cancel Date"')) IS NOT NULL THEN 'cancelled'
    WHEN COALESCE(JSON_VALUE(r.record_json,'$.PhysicalSubscriptionPauseDate'),JSON_VALUE(r.record_json,'$."Physical Subscription Pause Date"')) IS NOT NULL THEN 'paused'
    ELSE 'active'
  END AS status
FROM bronze.raw_record r
LEFT JOIN gold.crosswalk cw
  ON cw.canonical_type='person'
 AND cw.source_system=r.source_system
 AND cw.source_record_id=r.source_record_id
WHERE (
  JSON_VALUE(r.record_json,'$.PlanType') IS NOT NULL
  OR JSON_VALUE(r.record_json,'$.SubscriberSince') IS NOT NULL
  OR JSON_VALUE(r.record_json,'$.SubscriptionBoxRenewalDate') IS NOT NULL
  OR JSON_VALUE(r.record_json,'$.SubscriptionBoxQuantity') IS NOT NULL
)
AND NOT EXISTS (
  SELECT 1 FROM gold.subscription_contract s
  WHERE s.source_system=r.source_system
    AND s.source_record_id=CONCAT(r.source_record_id,':sub')
);
`);

    await pool.request().batch(`
INSERT INTO gold.subscription_status_history(subscription_contract_id,source_system,source_record_id,status,status_reason,effective_date)
SELECT
  sc.subscription_contract_id,
  sc.source_system,
  CONCAT(sc.source_record_id,':status') AS source_record_id,
  sc.status,
  COALESCE(
    JSON_VALUE(r.record_json,'$.SubscriptionCancellationReason'),
    JSON_VALUE(r.record_json,'$.ReasonforCancellingSubscription'),
    JSON_VALUE(r.record_json,'$.SubscriptionPauseReasoning')
  ) AS status_reason,
  TRY_CONVERT(date, COALESCE(
    JSON_VALUE(r.record_json,'$.PhysicalSubscriptionCancelDate'),
    JSON_VALUE(r.record_json,'$.PhysicalSubscriptionPauseDate'),
    JSON_VALUE(r.record_json,'$.PhysicalSubscriptionReStartDate')
  )) AS effective_date
FROM gold.subscription_contract sc
JOIN bronze.raw_record r
  ON r.source_system=sc.source_system
 AND CONCAT(r.source_record_id,':sub')=sc.source_record_id
WHERE NOT EXISTS (
  SELECT 1 FROM gold.subscription_status_history h
  WHERE h.subscription_contract_id=sc.subscription_contract_id
);
`);

    await pool.request().batch(`
INSERT INTO gold.subscription_shipment(subscription_contract_id,source_system,source_record_id,shipment_month,quantity,shipstation_counter)
SELECT
  sc.subscription_contract_id,
  sc.source_system,
  CONCAT(sc.source_record_id,':shipment') AS source_record_id,
  COALESCE(JSON_VALUE(r.record_json,'$.NextBoxMonth'),JSON_VALUE(r.record_json,'$.Next Box Month')),
  TRY_CONVERT(int, COALESCE(JSON_VALUE(r.record_json,'$.SubscriptionBoxQuantity'),JSON_VALUE(r.record_json,'$."Subscription Box Quantity"'))),
  TRY_CONVERT(int, COALESCE(JSON_VALUE(r.record_json,'$.ShipstationCounter'),JSON_VALUE(r.record_json,'$."Shipstation Counter"')))
FROM gold.subscription_contract sc
JOIN bronze.raw_record r
  ON r.source_system=sc.source_system
 AND CONCAT(r.source_record_id,':sub')=sc.source_record_id
WHERE NOT EXISTS (
  SELECT 1 FROM gold.subscription_shipment sh
  WHERE sh.subscription_contract_id=sc.subscription_contract_id
);
`);

    await pool.request().batch(`
INSERT INTO gold.pledge_commitment(person_id,source_system,source_record_id,commitment_type,amount_committed,cadence,status,committed_at)
SELECT
  cw.canonical_id,
  t.source_system,
  CONCAT(t.source_record_id,':pledge') AS source_record_id,
  'recurring_commitment',
  t.amount,
  'monthly',
  COALESCE(t.status,'active'),
  t.transaction_ts
FROM silver.transaction_source t
LEFT JOIN gold.crosswalk cw
  ON cw.canonical_type='person'
 AND cw.source_system=t.source_system
 AND cw.source_record_id=t.person_ref
WHERE (
  LOWER(t.file_path) LIKE '%recurring%'
  OR LOWER(t.file_path) LIKE '%payplan%'
  OR LOWER(t.file_path) LIKE '%subscription%'
)
AND NOT EXISTS (
  SELECT 1 FROM gold.pledge_commitment p
  WHERE p.source_system=t.source_system
    AND p.source_record_id=CONCAT(t.source_record_id,':pledge')
);
`);

    await pool.request().batch(`
INSERT INTO gold.refund_chargeback(person_id,source_system,source_record_id,reason,amount,occurred_at,status)
SELECT
  cw.canonical_id,
  t.source_system,
  CONCAT(t.source_record_id,':refund') AS source_record_id,
  'status_indicates_refund_or_chargeback',
  t.amount,
  t.transaction_ts,
  t.status
FROM silver.transaction_source t
LEFT JOIN gold.crosswalk cw
  ON cw.canonical_type='person'
 AND cw.source_system=t.source_system
 AND cw.source_record_id=t.person_ref
WHERE (LOWER(COALESCE(t.status,'')) LIKE '%refund%'
   OR LOWER(COALESCE(t.status,'')) LIKE '%chargeback%')
AND NOT EXISTS (
  SELECT 1 FROM gold.refund_chargeback r
  WHERE r.source_system=t.source_system
    AND r.source_record_id=CONCAT(t.source_record_id,':refund')
);
`);

    await pool.request().batch(`
INSERT INTO gold.appeal(source_system,source_record_id,appeal_name,appeal_type,campaign_name)
SELECT DISTINCT
  r.source_system,
  CONCAT(r.source_record_id,':appeal') AS source_record_id,
  COALESCE(JSON_VALUE(r.record_json,'$.CampaignName'),'General Appeal') AS appeal_name,
  'fundraising',
  JSON_VALUE(r.record_json,'$.CampaignName')
FROM bronze.raw_record r
WHERE JSON_VALUE(r.record_json,'$.CampaignName') IS NOT NULL
AND NOT EXISTS (
  SELECT 1 FROM gold.appeal a
  WHERE a.source_system=r.source_system
    AND a.source_record_id=CONCAT(r.source_record_id,':appeal')
);
`);

    await pool.request().batch(`
INSERT INTO gold.designation(source_system,source_record_id,designation_name,designation_type)
SELECT DISTINCT
  r.source_system,
  CONCAT(r.source_record_id,':designation') AS source_record_id,
  COALESCE(
    JSON_VALUE(r.record_json,'$.Wherewouldyouliketodirectyourdonation'),
    JSON_VALUE(r.record_json,'$."Where would you like to direct your donation?"'),
    JSON_VALUE(r.record_json,'$.DonationGift')
  ) AS designation_name,
  'donation_intent'
FROM bronze.raw_record r
WHERE COALESCE(
  JSON_VALUE(r.record_json,'$.Wherewouldyouliketodirectyourdonation'),
  JSON_VALUE(r.record_json,'$."Where would you like to direct your donation?"'),
  JSON_VALUE(r.record_json,'$.DonationGift')
) IS NOT NULL
AND NOT EXISTS (
  SELECT 1 FROM gold.designation d
  WHERE d.source_system=r.source_system
    AND d.source_record_id=CONCAT(r.source_record_id,':designation')
);
`);

    await pool.request().batch(`
INSERT INTO gold.support_case(person_id,source_system,source_record_id,case_subject,case_status,opened_at,closed_at)
SELECT
  cw.canonical_id,
  e.source_system,
  CONCAT(e.source_record_id,':support') AS source_record_id,
  COALESCE(e.subject,e.engagement_type,'Support Case'),
  'open',
  e.occurred_at,
  NULL
FROM silver.engagement_source e
LEFT JOIN gold.crosswalk cw
  ON cw.canonical_type='person'
 AND cw.source_system=e.source_system
 AND cw.source_record_id=e.person_ref
WHERE LOWER(COALESCE(e.engagement_type,'')) LIKE '%support%'
   OR LOWER(COALESCE(e.engagement_type,'')) LIKE '%help%'
   OR LOWER(COALESCE(e.subject,'')) LIKE '%support%'
AND NOT EXISTS (
  SELECT 1 FROM gold.support_case s
  WHERE s.source_system=e.source_system
    AND s.source_record_id=CONCAT(e.source_record_id,':support')
);
`);

    const counts = await pool.request().query(`
SELECT
  (SELECT COUNT(1) FROM gold.event) AS event_count,
  (SELECT COUNT(1) FROM gold.ticket_sale) AS ticket_sale_count,
  (SELECT COUNT(1) FROM gold.subscription_contract) AS subscription_count,
  (SELECT COUNT(1) FROM gold.pledge_commitment) AS pledge_count,
  (SELECT COUNT(1) FROM gold.refund_chargeback) AS refund_count,
  (SELECT COUNT(1) FROM gold.appeal) AS appeal_count,
  (SELECT COUNT(1) FROM gold.designation) AS designation_count,
  (SELECT COUNT(1) FROM gold.support_case) AS support_case_count,
  (SELECT COUNT(1) FROM meta.source_to_canonical_rule) AS rule_count;
`);

    console.log('OK: source mappings applied', counts.recordset[0]);
  });
}

main().catch((err) => {
  console.error('ERROR:', err.message);
  process.exit(1);
});
