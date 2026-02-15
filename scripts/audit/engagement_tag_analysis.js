const sql = require("mssql");
const fs = require("fs");
const path = require("path");

const envPath = path.join(__dirname, "..", "..", ".env.local");
const envContent = fs.readFileSync(envPath, "utf8");
const env = {};
for (const line of envContent.split(String.fromCharCode(10))) {
  const m = line.match(/^([^#=]+)=(.*)$/);
  if (m) env[m[1].trim()] = m[2].trim();
}

const config = {
  server: env.SOZO_SQL_HOST,
  database: "sozov2",
  user: env.SOZO_SQL_USER,
  password: env.SOZO_SQL_PASSWORD,
  options: { encrypt: true, trustServerCertificate: false },
  requestTimeout: 300000,
  connectionTimeout: 30000,
};

function printTable(label, rows) {
  console.log("\n" + "=".repeat(90));
  console.log("  " + label);
  console.log("=".repeat(90));
  if (!rows || rows.length === 0) { console.log("  (no rows)"); return; }
  console.table(rows);
  console.log("  -> " + rows.length + " rows\n");
}

async function run(pool, label, q) {
  const t0 = Date.now();
  console.log("Running: " + label + "...");
  try {
    const r = await pool.request().query(q);
    const sec = ((Date.now()-t0)/1000).toFixed(1);
    if (r.recordset && r.recordset.length > 0) {
      console.log("  done in " + sec + "s");
      printTable(label, r.recordset);
    } else {
      console.log("  done in " + sec + "s (" + (r.rowsAffected||[0])[0] + " rows affected)");
    }
  } catch(e) { console.error("  FAILED (" + ((Date.now()-t0)/1000).toFixed(0) + "s): " + e.message); }
}

async function main() {
  let pool;
  try {
    pool = await sql.connect(config);
    console.log("Connected to sozov2.\n");

    // Stage 1: Create temp tables for tag aggregations to avoid repeated scans of 3M row table
    console.log("--- STAGE 1: Pre-aggregating tag_detail (3M rows) into temp tables ---\n");

    // Temp: person-level tag summary
    await run(pool, "Create #person_tags",
      "SELECT person_id, COUNT(DISTINCT tag_group) AS groups, COUNT(*) AS total_tags INTO #person_tags FROM serving.tag_detail GROUP BY person_id"
    );

    // Temp: tag_group x person_id (distinct) with tag_group NOT NULL
    await run(pool, "Create #tag_group_person",
      "SELECT tag_group, person_id INTO #tag_group_person FROM serving.tag_detail WHERE tag_group IS NOT NULL GROUP BY tag_group, person_id"
    );

    // Temp: top tags
    await run(pool, "Create #top_tags",
      "SELECT TOP 30 tag_value, tag_group, COUNT(DISTINCT person_id) people INTO #top_tags FROM serving.tag_detail WHERE tag_value IS NOT NULL GROUP BY tag_value, tag_group ORDER BY COUNT(DISTINCT person_id) DESC"
    );

    // Temp: person program flags for query 8
    await run(pool, "Create #person_programs",
      "SELECT person_id, MAX(CASE WHEN tag_group='True Girl' THEN 1 ELSE 0 END) AS tg, MAX(CASE WHEN tag_group='B2BB' THEN 1 ELSE 0 END) AS b2bb, MAX(CASE WHEN tag_group='Donor Assignment' THEN 1 ELSE 0 END) AS donor, MAX(CASE WHEN tag_group='Customer Tags' THEN 1 ELSE 0 END) AS customer, MAX(CASE WHEN tag_group='Nurture Tags' THEN 1 ELSE 0 END) AS nurture INTO #person_programs FROM serving.tag_detail WHERE tag_group IS NOT NULL GROUP BY person_id"
    );

    console.log("\n--- STAGE 2: Running analytical queries ---\n");

    // Q1: Tag group distribution with donor overlap (using pre-agg temp)
    await run(pool, "1. TAG GROUP DISTRIBUTION WITH DONOR OVERLAP",
      "SELECT t.tag_group, COUNT(DISTINCT t.person_id) tagged_people, COUNT(DISTINCT d.person_id) are_donors, CAST(COUNT(DISTINCT d.person_id)*100.0/NULLIF(COUNT(DISTINCT t.person_id),0) AS DECIMAL(5,1)) donor_pct, CAST(ISNULL(SUM(d.total_given),0) AS INT) total_giving FROM #tag_group_person t LEFT JOIN serving.donor_summary d ON d.person_id=t.person_id GROUP BY t.tag_group ORDER BY COUNT(DISTINCT t.person_id) DESC"
    );

    // Q2: Top 30 most common tags
    await run(pool, "2. TOP 30 MOST COMMON TAGS",
      "SELECT tag_value, tag_group, people FROM #top_tags ORDER BY people DESC"
    );

    // Q3: Multi-tag engagement depth
    await run(pool, "3. MULTI-TAG ENGAGEMENT DEPTH",
      "SELECT CASE WHEN groups=1 THEN '1-Single Program' WHEN groups=2 THEN '2-Two Programs' WHEN groups=3 THEN '3-Three Programs' WHEN groups>=4 THEN '4-Multi (4+)' END AS depth, COUNT(*) people, CAST(AVG(total_tags) AS INT) avg_tags FROM #person_tags GROUP BY CASE WHEN groups=1 THEN '1-Single Program' WHEN groups=2 THEN '2-Two Programs' WHEN groups=3 THEN '3-Three Programs' WHEN groups>=4 THEN '4-Multi (4+)' END ORDER BY 1"
    );

    // Q4: Engaged non-donors
    await run(pool, "4. ENGAGED NON-DONORS (CONVERSION TARGETS)",
      "SELECT CASE WHEN t.groups>=4 THEN 'Super Engaged (4+ programs)' WHEN t.groups=3 THEN 'Highly Engaged (3 programs)' WHEN t.groups=2 THEN 'Moderately Engaged (2 programs)' ELSE 'Lightly Engaged (1 program)' END AS engagement_level, COUNT(*) people, CAST(AVG(t.total_tags) AS INT) avg_tags FROM #person_tags t WHERE t.person_id NOT IN (SELECT person_id FROM serving.donor_summary) GROUP BY CASE WHEN t.groups>=4 THEN 'Super Engaged (4+ programs)' WHEN t.groups=3 THEN 'Highly Engaged (3 programs)' WHEN t.groups=2 THEN 'Moderately Engaged (2 programs)' ELSE 'Lightly Engaged (1 program)' END ORDER BY 1"
    );

    // Q5: Communication channel effectiveness
    await run(pool, "5. COMMUNICATION CHANNEL EFFECTIVENESS",
      "SELECT channel, direction, COUNT(*) comms, COUNT(DISTINCT person_id) people FROM serving.communication_detail GROUP BY channel, direction ORDER BY COUNT(*) DESC"
    );

    // Q6: Lifecycle stage x engagement
    await run(pool, "6. LIFECYCLE STAGE x ENGAGEMENT",
      "SELECT p.lifecycle_stage, COUNT(*) people, CAST(AVG(CAST(p.tag_count AS FLOAT)) AS INT) avg_tags, CAST(AVG(CAST(p.note_count AS FLOAT)) AS INT) avg_notes, CAST(AVG(CAST(p.comm_count AS FLOAT)) AS INT) avg_comms, CAST(AVG(CAST(p.order_count AS FLOAT)) AS INT) avg_orders, CAST(AVG(p.lifetime_giving) AS INT) avg_giving FROM serving.person_360 p WHERE p.display_name<>'Unknown' GROUP BY p.lifecycle_stage ORDER BY COUNT(*) DESC"
    );

    // Q7: Top engagement, zero giving
    await run(pool, "7. HIGHEST ENGAGEMENT + ZERO GIVING (BEST CONVERSION TARGETS)",
      "SELECT TOP 20 display_name, tag_count, note_count, comm_count, order_count, total_spent, lifecycle_stage FROM serving.person_360 WHERE lifetime_giving=0 AND display_name<>'Unknown' ORDER BY tag_count + note_count + comm_count DESC"
    );

    // Q8: Program affinity segments
    await run(pool, "8. TAG-BASED PROGRAM AFFINITY SEGMENTS",
      "SELECT CASE WHEN tg=1 AND b2bb=1 THEN 'TrueGirl + B2BB' WHEN tg=1 AND donor=1 THEN 'TrueGirl + Donor' WHEN tg=1 THEN 'TrueGirl Only' WHEN b2bb=1 THEN 'B2BB Only' WHEN donor=1 THEN 'Donor Only' WHEN nurture=1 THEN 'Nurture Only' WHEN customer=1 THEN 'Customer Only' ELSE 'Other' END AS segment, COUNT(*) people FROM #person_programs GROUP BY CASE WHEN tg=1 AND b2bb=1 THEN 'TrueGirl + B2BB' WHEN tg=1 AND donor=1 THEN 'TrueGirl + Donor' WHEN tg=1 THEN 'TrueGirl Only' WHEN b2bb=1 THEN 'B2BB Only' WHEN donor=1 THEN 'Donor Only' WHEN nurture=1 THEN 'Nurture Only' WHEN customer=1 THEN 'Customer Only' ELSE 'Other' END ORDER BY COUNT(*) DESC"
    );

    // Q9: Wealth screening gap analysis
    await run(pool, "9. WEALTH SCREENING - CAPACITY GAP ANALYSIS",
      "SELECT w.capacity_label, COUNT(*) screened, SUM(CASE WHEN d.person_id IS NOT NULL THEN 1 ELSE 0 END) are_donors, CAST(AVG(w.giving_capacity) AS INT) avg_capacity, CAST(AVG(ISNULL(d.total_given,0)) AS INT) avg_actual, CAST(AVG(w.giving_capacity) - AVG(ISNULL(d.total_given,0)) AS INT) avg_gap, CAST(SUM(w.giving_capacity - ISNULL(d.total_given,0)) AS BIGINT) total_gap FROM serving.wealth_screening w LEFT JOIN serving.donor_summary d ON d.person_id=w.person_id GROUP BY w.capacity_label ORDER BY AVG(w.giving_capacity) DESC"
    );

    console.log("\nAll 9 queries completed.");
  } catch (err) {
    console.error("ERROR:", err.message);
  } finally {
    if (pool) await pool.close();
  }
}

main();