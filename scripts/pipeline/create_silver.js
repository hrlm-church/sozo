/**
 * Create Silver Layer Schema — sozov2
 *
 * Creates the `silver` schema and all typed tables.
 * Idempotent: drops and recreates tables.
 *
 * Usage: node scripts/pipeline/create_silver.js
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

const TABLES = [
  // ── CORE ──────────────────────────────────────────────
  {
    name: 'silver.contact',
    ddl: `CREATE TABLE silver.contact (
      contact_id    INT IDENTITY(1,1) PRIMARY KEY,
      source_system VARCHAR(20)  NOT NULL,
      source_id     VARCHAR(100) NOT NULL,
      first_name    NVARCHAR(200),
      last_name     NVARCHAR(200),
      middle_name   NVARCHAR(100),
      suffix        NVARCHAR(50),
      title         NVARCHAR(100),
      organization_name NVARCHAR(500),
      email_primary NVARCHAR(500),
      email_2       NVARCHAR(500),
      email_3       NVARCHAR(500),
      phone_primary NVARCHAR(100),
      phone_2       NVARCHAR(100),
      address_line1 NVARCHAR(500),
      address_line2 NVARCHAR(500),
      city          NVARCHAR(200),
      state         NVARCHAR(100),
      postal_code   NVARCHAR(20),
      country       NVARCHAR(100),
      date_of_birth DATE,
      gender        NVARCHAR(20),
      spouse_name   NVARCHAR(200),
      household_name NVARCHAR(500),
      company_id    VARCHAR(100),
      keap_number   VARCHAR(100),
      dd_number     VARCHAR(100),
      gb_external_id VARCHAR(100),
      accepts_marketing BIT,
      lifecycle_stage NVARCHAR(100),
      created_at    DATETIME2,
      updated_at    DATETIME2,
      UNIQUE (source_system, source_id)
    )`
  },
  {
    name: 'silver.donation',
    ddl: `CREATE TABLE silver.donation (
      donation_id     INT IDENTITY(1,1) PRIMARY KEY,
      source_system   VARCHAR(20) NOT NULL,
      source_id       VARCHAR(100),
      contact_source_id VARCHAR(100),
      donated_at      DATE,
      amount          DECIMAL(12,2),
      currency        VARCHAR(10) DEFAULT 'USD',
      fund_code       VARCHAR(100),
      project_code    VARCHAR(100),
      source_code     VARCHAR(100),
      payment_type    VARCHAR(50),
      campaign_name   NVARCHAR(500),
      description     NVARCHAR(1000),
      short_comment   NVARCHAR(1000),
      is_anonymous    BIT DEFAULT 0,
      is_deductible   BIT DEFAULT 1
    )`
  },
  {
    name: 'silver.subscription',
    ddl: `CREATE TABLE silver.subscription (
      subscription_id INT IDENTITY(1,1) PRIMARY KEY,
      keap_id         INT,
      contact_keap_id INT,
      start_date      DATE,
      end_date        DATE,
      last_bill_date  DATE,
      next_bill_date  DATE,
      billing_amount  DECIMAL(12,2),
      billing_cycle   VARCHAR(50),
      frequency       INT,
      status          VARCHAR(50),
      reason_stopped  NVARCHAR(500),
      auto_charge     BIT,
      product_id      INT,
      created_at      DATETIME2,
      updated_at      DATETIME2
    )`
  },

  // ── ENGAGEMENT ────────────────────────────────────────
  {
    name: 'silver.note',
    ddl: `CREATE TABLE silver.note (
      note_id           INT IDENTITY(1,1) PRIMARY KEY,
      source_system     VARCHAR(20) NOT NULL,
      source_id         VARCHAR(100),
      contact_source_id VARCHAR(100),
      note_type         NVARCHAR(200),
      subject           NVARCHAR(500),
      content           NVARCHAR(MAX),
      created_at        DATETIME2,
      created_by        NVARCHAR(200)
    )`
  },
  {
    name: 'silver.communication',
    ddl: `CREATE TABLE silver.communication (
      comm_id           INT IDENTITY(1,1) PRIMARY KEY,
      source_system     VARCHAR(20) NOT NULL,
      contact_source_id VARCHAR(100),
      comm_date         DATE,
      comm_type         NVARCHAR(200),
      direction         NVARCHAR(20),
      subject           NVARCHAR(500),
      content           NVARCHAR(MAX),
      source_code       NVARCHAR(200),
      from_email        NVARCHAR(500),
      to_email          NVARCHAR(500)
    )`
  },

  // ── COMMERCE (Keap) ──────────────────────────────────
  {
    name: 'silver.invoice',
    ddl: `CREATE TABLE silver.invoice (
      invoice_id    INT IDENTITY(1,1) PRIMARY KEY,
      keap_id       INT,
      contact_keap_id INT,
      job_id        INT,
      created_at    DATETIME2,
      due_date      DATE,
      total         DECIMAL(12,2),
      total_due     DECIMAL(12,2),
      total_paid    DECIMAL(12,2),
      pay_status    VARCHAR(50),
      credit_status VARCHAR(50),
      refund_status VARCHAR(50),
      invoice_type  VARCHAR(50),
      description   NVARCHAR(1000),
      promo_code    NVARCHAR(200),
      updated_at    DATETIME2
    )`
  },
  {
    name: 'silver.payment',
    ddl: `CREATE TABLE silver.payment (
      payment_id      INT IDENTITY(1,1) PRIMARY KEY,
      keap_id         INT,
      contact_keap_id INT,
      invoice_keap_id INT,
      pay_date        DATETIME2,
      amount          DECIMAL(12,2),
      pay_type        VARCHAR(50),
      pay_note        NVARCHAR(1000),
      collection_method VARCHAR(50),
      payment_subtype VARCHAR(50),
      created_at      DATETIME2,
      updated_at      DATETIME2
    )`
  },
  {
    name: 'silver.[order]',
    ddl: `CREATE TABLE silver.[order] (
      order_id        INT IDENTITY(1,1) PRIMARY KEY,
      keap_id         INT,
      contact_keap_id INT,
      title           NVARCHAR(500),
      created_at      DATETIME2,
      start_date      DATE,
      due_date        DATE,
      order_type      VARCHAR(50),
      order_status    VARCHAR(50),
      source          NVARCHAR(200),
      promo_code      NVARCHAR(200),
      coupon_code     NVARCHAR(200),
      updated_at      DATETIME2
    )`
  },
  {
    name: 'silver.order_item',
    ddl: `CREATE TABLE silver.order_item (
      item_id         INT IDENTITY(1,1) PRIMARY KEY,
      keap_id         INT,
      order_keap_id   INT,
      product_keap_id INT,
      item_name       NVARCHAR(500),
      qty             INT,
      cost_per_unit   DECIMAL(12,2),
      price_per_unit  DECIMAL(12,2),
      item_type       VARCHAR(50),
      created_at      DATETIME2
    )`
  },

  // ── REFERENCE ─────────────────────────────────────────
  {
    name: 'silver.product',
    ddl: `CREATE TABLE silver.product (
      product_id  INT IDENTITY(1,1) PRIMARY KEY,
      keap_id     INT,
      name        NVARCHAR(500),
      short_desc  NVARCHAR(1000),
      description NVARCHAR(MAX),
      price       DECIMAL(12,2),
      cost        DECIMAL(12,2),
      sku         NVARCHAR(100),
      status      VARCHAR(50),
      is_digital  BIT,
      shippable   BIT,
      taxable     BIT,
      created_at  DATETIME2,
      updated_at  DATETIME2
    )`
  },
  {
    name: 'silver.tag',
    ddl: `CREATE TABLE silver.tag (
      tag_id             INT IDENTITY(1,1) PRIMARY KEY,
      keap_id            INT,
      group_name         NVARCHAR(500),
      group_description  NVARCHAR(2000),
      category_name      NVARCHAR(500),
      category_description NVARCHAR(2000),
      created_at         DATETIME2,
      updated_at         DATETIME2
    )`
  },
  {
    name: 'silver.company',
    ddl: `CREATE TABLE silver.company (
      company_id    INT IDENTITY(1,1) PRIMARY KEY,
      keap_id       INT,
      name          NVARCHAR(500),
      email         NVARCHAR(500),
      phone         NVARCHAR(100),
      fax           NVARCHAR(100),
      address_line1 NVARCHAR(500),
      address_line2 NVARCHAR(500),
      city          NVARCHAR(200),
      state         NVARCHAR(100),
      postal_code   NVARCHAR(20),
      country       NVARCHAR(100),
      notes         NVARCHAR(MAX),
      created_at    DATETIME2,
      updated_at    DATETIME2
    )`
  },
  {
    name: 'silver.stripe_customer',
    ddl: `CREATE TABLE silver.stripe_customer (
      id            INT IDENTITY(1,1) PRIMARY KEY,
      stripe_id     VARCHAR(100),
      email         NVARCHAR(500),
      name          NVARCHAR(500),
      phone         NVARCHAR(100),
      old_id        NVARCHAR(200),
      total_spend   DECIMAL(12,2),
      payment_count INT,
      created_at    DATETIME2
    )`
  },

  // ── TAG ASSIGNMENTS (Keap ContactGroupAssign) ────────
  {
    name: 'silver.contact_tag',
    ddl: `CREATE TABLE silver.contact_tag (
      contact_tag_id    INT IDENTITY(1,1) PRIMARY KEY,
      tag_keap_id       INT NOT NULL,
      contact_keap_id   INT NOT NULL,
      contact_source_id VARCHAR(20),
      date_applied      DATETIME2,
      CONSTRAINT uq_contact_tag UNIQUE (tag_keap_id, contact_keap_id)
    )`
  },

  // ── NORMALIZED DD DETAILS ─────────────────────────────
  {
    name: 'silver.contact_email',
    ddl: `CREATE TABLE silver.contact_email (
      email_id          INT IDENTITY(1,1) PRIMARY KEY,
      source_system     VARCHAR(20) NOT NULL,
      contact_source_id VARCHAR(100),
      email_address     NVARCHAR(500),
      email_type        VARCHAR(50),
      is_primary        BIT,
      is_active         BIT
    )`
  },
  {
    name: 'silver.contact_phone',
    ddl: `CREATE TABLE silver.contact_phone (
      phone_id          INT IDENTITY(1,1) PRIMARY KEY,
      source_system     VARCHAR(20) NOT NULL,
      contact_source_id VARCHAR(100),
      phone_number      NVARCHAR(100),
      phone_type        VARCHAR(50),
      area_code         VARCHAR(10),
      is_primary        BIT,
      is_active         BIT
    )`
  },
  {
    name: 'silver.contact_address',
    ddl: `CREATE TABLE silver.contact_address (
      address_id        INT IDENTITY(1,1) PRIMARY KEY,
      source_system     VARCHAR(20) NOT NULL,
      contact_source_id VARCHAR(100),
      address_line1     NVARCHAR(500),
      address_line2     NVARCHAR(500),
      city              NVARCHAR(200),
      state             NVARCHAR(100),
      postal_code       NVARCHAR(20),
      country           NVARCHAR(100),
      is_active         BIT
    )`
  },

  // ── NEW SOURCE TABLES ───────────────────────────────────
  {
    name: 'silver.stripe_charge',
    ddl: `CREATE TABLE silver.stripe_charge (
      charge_id        INT IDENTITY(1,1) PRIMARY KEY,
      stripe_charge_id VARCHAR(100),
      customer_id      VARCHAR(100),
      customer_email   NVARCHAR(500),
      customer_name    NVARCHAR(500),
      amount           DECIMAL(12,2),
      amount_refunded  DECIMAL(12,2),
      currency         VARCHAR(10),
      status           VARCHAR(50),
      description      NVARCHAR(2000),
      fee              DECIMAL(12,2),
      created_at       DATETIME2,
      card_brand       VARCHAR(50),
      card_last4       VARCHAR(10),
      card_funding     VARCHAR(20),
      statement_desc   NVARCHAR(200),
      refunded_at      DATETIME2,
      disputed_amount  DECIMAL(12,2),
      meta_source      NVARCHAR(200),
      meta_from_app    NVARCHAR(200),
      meta_order_id    NVARCHAR(200),
      meta_order_key   NVARCHAR(200),
      meta_site_url    NVARCHAR(500),
      checkout_summary NVARCHAR(2000),
      source_file      VARCHAR(50)
    )`
  },
  {
    name: 'silver.woo_order',
    ddl: `CREATE TABLE silver.woo_order (
      woo_order_id   INT IDENTITY(1,1) PRIMARY KEY,
      order_number   VARCHAR(50),
      customer_name  NVARCHAR(500),
      customer_email NVARCHAR(500),
      order_date     DATETIME2,
      revenue        DECIMAL(12,2),
      net_sales      DECIMAL(12,2),
      status         VARCHAR(50),
      product_name   NVARCHAR(1000),
      items_sold     INT,
      coupon         NVARCHAR(200),
      customer_type  VARCHAR(50),
      attribution    NVARCHAR(500),
      city           NVARCHAR(200),
      region         NVARCHAR(100),
      postal_code    NVARCHAR(20)
    )`
  },
  {
    name: 'silver.event_ticket',
    ddl: `CREATE TABLE silver.event_ticket (
      ticket_id       INT IDENTITY(1,1) PRIMARY KEY,
      event_name      NVARCHAR(500),
      attendee_first  NVARCHAR(200),
      attendee_last   NVARCHAR(200),
      attendee_name   NVARCHAR(500),
      attendee_email  NVARCHAR(500),
      buyer_first     NVARCHAR(200),
      buyer_last      NVARCHAR(200),
      buyer_name      NVARCHAR(500),
      buyer_email     NVARCHAR(500),
      payment_date    DATETIME2,
      order_number    VARCHAR(50),
      payment_gateway VARCHAR(50),
      order_status    VARCHAR(50),
      order_total     DECIMAL(12,2),
      ticket_total    DECIMAL(12,2),
      ticket_type     NVARCHAR(500),
      ticket_code     VARCHAR(100),
      checked_in      BIT,
      price           DECIMAL(12,2),
      city            NVARCHAR(200),
      state           NVARCHAR(100),
      postal_code     NVARCHAR(20),
      country         NVARCHAR(100),
      phone           NVARCHAR(100),
      coupon_code     NVARCHAR(200)
    )`
  },
  {
    name: 'silver.subbly_subscription',
    ddl: `CREATE TABLE silver.subbly_subscription (
      sub_id              INT IDENTITY(1,1) PRIMARY KEY,
      subbly_sub_id       INT,
      customer_id         INT,
      customer_name       NVARCHAR(500),
      customer_email      NVARCHAR(500),
      product_name        NVARCHAR(500),
      status              VARCHAR(50),
      past_due            BIT,
      renewal_date        DATE,
      date_created        DATETIME2,
      date_cancelled      DATETIME2,
      cancellation_reason NVARCHAR(1000),
      cancel_feedback     NVARCHAR(2000),
      shipping_method     NVARCHAR(200),
      shipping_price      DECIMAL(12,2),
      currency_code       VARCHAR(10),
      address_line1       NVARCHAR(500),
      city                NVARCHAR(200),
      state               NVARCHAR(100),
      postal_code         NVARCHAR(20),
      country             NVARCHAR(100),
      phone               NVARCHAR(100),
      girl_name           NVARCHAR(200),
      girl_birthday       NVARCHAR(100),
      orders_count        INT,
      paused              BIT,
      discount            NVARCHAR(200)
    )`
  },
  {
    name: 'silver.shopify_order',
    ddl: `CREATE TABLE silver.shopify_order (
      shopify_order_id   INT IDENTITY(1,1) PRIMARY KEY,
      order_name         VARCHAR(50),
      customer_email     NVARCHAR(500),
      financial_status   VARCHAR(50),
      paid_at            DATETIME2,
      fulfillment_status VARCHAR(50),
      currency           VARCHAR(10),
      subtotal           DECIMAL(12,2),
      shipping           DECIMAL(12,2),
      taxes              DECIMAL(12,2),
      total              DECIMAL(12,2),
      discount_code      NVARCHAR(200),
      discount_amount    DECIMAL(12,2),
      line_item_name     NVARCHAR(1000),
      line_item_price    DECIMAL(12,2),
      line_item_qty      INT,
      vendor             NVARCHAR(200),
      billing_city       NVARCHAR(200),
      billing_state      NVARCHAR(100),
      billing_zip        NVARCHAR(20),
      shipping_city      NVARCHAR(200),
      shipping_state     NVARCHAR(100),
      shipping_zip       NVARCHAR(20),
      tags               NVARCHAR(2000),
      source             NVARCHAR(200),
      risk_level         VARCHAR(20),
      created_at         DATETIME2
    )`
  },
  {
    name: 'silver.generic_tag',
    ddl: `CREATE TABLE silver.generic_tag (
      generic_tag_id    INT IDENTITY(1,1) PRIMARY KEY,
      source_system     VARCHAR(20) NOT NULL,
      contact_source_id VARCHAR(200),
      tag_value         NVARCHAR(500) NOT NULL,
      tag_category      NVARCHAR(200),
      applied_at        DATETIME2
    )`
  },
];

async function main() {
  loadEnv();
  const newOnly = process.argv.includes('--new-only');
  const pool = await sql.connect({
    server: process.env.SOZO_SQL_HOST, database: 'sozov2',
    user: process.env.SOZO_SQL_USER, password: process.env.SOZO_SQL_PASSWORD,
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 60000
  });

  // Create silver schema
  console.log('Creating silver schema...');
  await pool.request().query(`
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
      EXEC('CREATE SCHEMA silver')
  `);

  // Create each table
  for (const t of TABLES) {
    const shortName = t.name.replace('silver.', '').replace(/[\[\]]/g, '');
    console.log(`  ${t.name}...`);
    try {
      if (newOnly) {
        // Only create if it doesn't exist — preserve existing data
        const exists = await pool.request().query(
          `SELECT OBJECT_ID('${t.name}', 'U') AS oid`
        );
        if (exists.recordset[0].oid) {
          console.log(`    SKIP (already exists, --new-only)`);
          continue;
        }
      } else {
        await pool.request().query(
          `IF OBJECT_ID('${t.name}', 'U') IS NOT NULL DROP TABLE ${t.name}`
        );
      }
      await pool.request().query(t.ddl);
      console.log(`    OK`);
    } catch (err) {
      console.error(`    FAIL: ${err.message}`);
    }
  }

  // Create indexes for common lookups
  console.log('\nCreating indexes...');
  const indexes = [
    'CREATE INDEX ix_contact_source ON silver.contact (source_system, source_id)',
    'CREATE INDEX ix_contact_email ON silver.contact (email_primary)',
    'CREATE INDEX ix_contact_name ON silver.contact (last_name, first_name)',
    'CREATE INDEX ix_donation_contact ON silver.donation (source_system, contact_source_id)',
    'CREATE INDEX ix_donation_date ON silver.donation (donated_at)',
    'CREATE INDEX ix_donation_amount ON silver.donation (amount DESC)',
    'CREATE INDEX ix_note_contact ON silver.note (source_system, contact_source_id)',
    'CREATE INDEX ix_comm_contact ON silver.communication (source_system, contact_source_id)',
    'CREATE INDEX ix_invoice_contact ON silver.invoice (contact_keap_id)',
    'CREATE INDEX ix_payment_contact ON silver.payment (contact_keap_id)',
    'CREATE INDEX ix_order_contact ON silver.[order] (contact_keap_id)',
    'CREATE INDEX ix_sub_contact ON silver.subscription (contact_keap_id)',
    'CREATE INDEX ix_stripe_email ON silver.stripe_customer (email)',
    'CREATE INDEX ix_ct_contact ON silver.contact_tag (contact_keap_id)',
    'CREATE INDEX ix_ct_tag ON silver.contact_tag (tag_keap_id)',
    'CREATE INDEX ix_ct_source ON silver.contact_tag (contact_source_id)',
    'CREATE INDEX ix_cemail_contact ON silver.contact_email (source_system, contact_source_id)',
    'CREATE INDEX ix_cphone_contact ON silver.contact_phone (source_system, contact_source_id)',
    'CREATE INDEX ix_caddr_contact ON silver.contact_address (source_system, contact_source_id)',
    // New source indexes
    'CREATE INDEX ix_stripe_charge_email ON silver.stripe_charge (customer_email)',
    'CREATE INDEX ix_stripe_charge_date ON silver.stripe_charge (created_at)',
    'CREATE INDEX ix_stripe_charge_status ON silver.stripe_charge (status)',
    'CREATE INDEX ix_woo_order_email ON silver.woo_order (customer_email)',
    'CREATE INDEX ix_woo_order_date ON silver.woo_order (order_date)',
    'CREATE INDEX ix_event_ticket_buyer ON silver.event_ticket (buyer_email)',
    'CREATE INDEX ix_event_ticket_attendee ON silver.event_ticket (attendee_email)',
    'CREATE INDEX ix_subbly_sub_email ON silver.subbly_subscription (customer_email)',
    'CREATE INDEX ix_shopify_order_email ON silver.shopify_order (customer_email)',
    'CREATE INDEX ix_generic_tag_source ON silver.generic_tag (source_system, contact_source_id)',
    'CREATE INDEX ix_generic_tag_value ON silver.generic_tag (tag_value)',
  ];
  for (const idx of indexes) {
    try {
      await pool.request().query(idx);
      console.log(`  OK: ${idx.split(' ON ')[0]}`);
    } catch (err) {
      console.error(`  FAIL: ${err.message.substring(0, 80)}`);
    }
  }

  await pool.close();
  console.log(`\nDone. Created ${TABLES.length} tables.`);
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
