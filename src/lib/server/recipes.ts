/**
 * Recipe Matcher — detects common query patterns and returns exact SQL + widget configs.
 *
 * When a recipe matches, it gets injected at the END of the system prompt
 * with mandatory language so the AI model uses the exact queries instead of improvising.
 */

export interface Recipe {
  id: string;
  /** Regex patterns to match against user message (case-insensitive) */
  patterns: RegExp[];
  /** Extract dynamic values from the message (e.g., "top 50" → N=50) */
  extractParams?: (msg: string) => Record<string, string>;
  /** The mandatory instruction block injected into the system prompt */
  getInstruction: (params: Record<string, string>) => string;
}

/** Extract a number from "top N" patterns, default to 50 */
function extractTopN(msg: string): Record<string, string> {
  const match = msg.match(/top\s+(\d+)/i);
  return { N: match?.[1] ?? "50" };
}

const RECIPES: Recipe[] = [
  {
    id: "360-view-top-donors",
    patterns: [
      /360\s*(view|degree|look)/i,
      /top\s+\d+\s+donors/i,
      /top\s+donors.*detail/i,
      /donor.*360/i,
      /full\s+view.*donors/i,
      /nice\s+view.*donors/i,
    ],
    extractParams: extractTopN,
    getInstruction: ({ N }) => `
## ⚠️ MANDATORY RECIPE — YOU MUST FOLLOW THIS EXACTLY ⚠️
A recipe matched for "360 view of top donors". You MUST use these EXACT SQL queries and widgets in this EXACT order. Do NOT modify the SQL. Do NOT improvise your own queries. Copy-paste these queries word-for-word into query_data calls.

**Step 1 — query_data** (group stats for stat_grid):
\`\`\`sql
SELECT COUNT(*) AS total, SUM(CAST(total_given AS DECIMAL(12,2))) AS giving, CAST(ROUND(AVG(CAST(total_given AS DECIMAL(12,2))),2) AS DECIMAL(12,2)) AS avg, SUM(CASE WHEN lifecycle_stage='Active' THEN 1 ELSE 0 END) AS active, SUM(CASE WHEN lifecycle_stage IN ('Cooling','Lapsed') THEN 1 ELSE 0 END) AS at_risk FROM (SELECT TOP (${N}) total_given, lifecycle_stage FROM serving.donor_summary WHERE display_name<>'Unknown' ORDER BY total_given DESC) x
\`\`\`
→ show_widget: **stat_grid** with stats=[{label:"Donors",value:total},{label:"Total Given",value:giving,unit:"$"},{label:"Avg Given",value:avg,unit:"$"},{label:"Active",value:active},{label:"At Risk",value:at_risk}]

**Step 2 — query_data** (lifecycle for donut_chart):
\`\`\`sql
SELECT lifecycle_stage AS [Stage], COUNT(*) AS [Donors] FROM (SELECT TOP (${N}) lifecycle_stage FROM serving.donor_summary WHERE display_name<>'Unknown' ORDER BY total_given DESC) x GROUP BY lifecycle_stage
\`\`\`
→ show_widget: **donut_chart** with categoryKey='Stage', valueKeys=['Donors']

**Step 3 — query_data** (full 360 table):
\`\`\`sql
WITH t AS (SELECT TOP (${N}) ds.person_id, ds.display_name AS [Donor], CAST(ds.total_given AS DECIMAL(12,2)) AS [Total Given], ds.donation_count AS [Gifts], CAST(ROUND(ds.avg_gift,2) AS DECIMAL(12,2)) AS [Avg Gift], ds.last_gift_date AS [Last Gift], DATEDIFF(DAY,ds.last_gift_date,GETDATE()) AS [Days Silent], ds.lifecycle_stage AS [Stage] FROM serving.donor_summary ds WHERE ds.display_name<>'Unknown' ORDER BY ds.total_given DESC), comm AS (SELECT od.person_id, COUNT(*) AS [Orders], SUM(CAST(od.total_amount AS DECIMAL(12,2))) AS [Commerce $] FROM serving.order_detail od WHERE od.person_id IN (SELECT person_id FROM t) GROUP BY od.person_id), evt AS (SELECT ed.person_id, COUNT(*) AS [Tickets] FROM serving.event_detail ed WHERE ed.person_id IN (SELECT person_id FROM t) GROUP BY ed.person_id), sub AS (SELECT sd.person_id, MAX(CASE WHEN sd.source_system='subbly' AND sd.subscription_status='Active' THEN 'Yes' ELSE 'No' END) AS [Active Sub] FROM serving.subscription_detail sd WHERE sd.person_id IN (SELECT person_id FROM t) GROUP BY sd.person_id), w AS (SELECT ws.person_id, ws.capacity_label AS [Capacity] FROM serving.wealth_screening ws WHERE ws.person_id IN (SELECT person_id FROM t)) SELECT t.[Donor], t.[Total Given], t.[Gifts], t.[Avg Gift], t.[Last Gift], t.[Days Silent], t.[Stage], ISNULL(comm.[Orders],0) AS [Orders], ISNULL(comm.[Commerce $],0) AS [Commerce $], ISNULL(evt.[Tickets],0) AS [Tickets], ISNULL(sub.[Active Sub],'No') AS [Active Sub], w.[Capacity] FROM t LEFT JOIN comm ON comm.person_id=t.person_id LEFT JOIN evt ON evt.person_id=t.person_id LEFT JOIN sub ON sub.person_id=t.person_id LEFT JOIN w ON w.person_id=t.person_id ORDER BY t.[Total Given] DESC
\`\`\`
→ show_widget: **table** with title="Top ${N} Donors — 360 View"

**Step 4 — show_widget**: **text** — Strategic analysis: who's at risk (Cooling/Lapsed with high giving + high days silent), who's undertapped (low giving vs wealth capacity), who needs a call this week. Name specific people with dollar amounts. 4-5 bullet points.

REMINDER: Use these EXACT queries. Do not modify column names, JOINs, or sort order.`,
  },

  {
    id: "capacity-vs-giving",
    patterns: [
      /capacity\s*(vs|versus|v\.?s?\.?)\s*giving/i,
      /wealth\s*(gap|screening)/i,
      /untapped\s*capacity/i,
      /giving.*capacity/i,
      /screened.*donor/i,
      /donor.*screened/i,
      /capacity.*dashboard/i,
    ],
    getInstruction: () => `
## ⚠️ MANDATORY RECIPE — YOU MUST FOLLOW THIS EXACTLY ⚠️
A recipe matched for "Capacity vs Giving dashboard". You MUST use these EXACT SQL queries and widgets in this EXACT order. Do NOT modify the SQL. Do NOT improvise your own queries.

CRITICAL: giving_capacity is an ANNUAL estimate. All comparisons MUST use annualized giving (total_given / years active), NEVER raw lifetime total_given.

**Step 1 — query_data** (KPI stats for stat_grid):
\`\`\`sql
SELECT COUNT(*) AS screened, CAST(SUM(ws.giving_capacity - ISNULL(ds.total_given / NULLIF(CEILING(DATEDIFF(MONTH,ds.first_gift_date,GETDATE())/12.0),0),0)) AS DECIMAL(14,0)) AS unrealized_annual, CAST(ROUND(AVG(CASE WHEN ws.giving_capacity>0 THEN ISNULL(ds.total_given / NULLIF(CEILING(DATEDIFF(MONTH,ds.first_gift_date,GETDATE())/12.0),0),0)/ws.giving_capacity*100 ELSE 0 END),1) AS DECIMAL(5,1)) AS avg_util, SUM(CASE WHEN ws.giving_capacity>0 AND ISNULL(ds.total_given / NULLIF(CEILING(DATEDIFF(MONTH,ds.first_gift_date,GETDATE())/12.0),0),0) < ws.giving_capacity*0.1 THEN 1 ELSE 0 END) AS below_10pct FROM serving.wealth_screening ws LEFT JOIN serving.donor_summary ds ON ds.person_id=ws.person_id WHERE ws.display_name<>'Unknown'
\`\`\`
→ show_widget: **stat_grid** with stats=[{label:"Screened Contacts",value:screened},{label:"Unrealized Annual Capacity",value:unrealized_annual,unit:"$"},{label:"Avg Annual Utilization",value:avg_util+"%"},{label:"Below 10% Capacity",value:below_10pct}]

**Step 2 — query_data** (distribution for bar_chart):
\`\`\`sql
SELECT CASE WHEN ann_util < 1 THEN '0-1%' WHEN ann_util < 5 THEN '1-5%' WHEN ann_util < 10 THEN '5-10%' WHEN ann_util < 25 THEN '10-25%' WHEN ann_util < 50 THEN '25-50%' WHEN ann_util < 100 THEN '50-99%' ELSE '100%+' END AS [Utilization Range], COUNT(*) AS [Contacts] FROM (SELECT ISNULL(ds.total_given / NULLIF(CEILING(DATEDIFF(MONTH,ds.first_gift_date,GETDATE())/12.0),0),0) / NULLIF(ws.giving_capacity,0)*100 AS ann_util FROM serving.wealth_screening ws LEFT JOIN serving.donor_summary ds ON ds.person_id=ws.person_id WHERE ws.display_name<>'Unknown' AND ws.giving_capacity>0) x GROUP BY CASE WHEN ann_util < 1 THEN '0-1%' WHEN ann_util < 5 THEN '1-5%' WHEN ann_util < 10 THEN '5-10%' WHEN ann_util < 25 THEN '10-25%' WHEN ann_util < 50 THEN '25-50%' WHEN ann_util < 100 THEN '50-99%' ELSE '100%+' END ORDER BY MIN(ann_util)
\`\`\`
→ show_widget: **bar_chart** with categoryKey='Utilization Range', valueKeys=['Contacts'], title="Annual Giving-to-Capacity Distribution"

**Step 3 — query_data** (full table sorted by gap):
\`\`\`sql
SELECT ws.display_name AS [Contact], ws.capacity_label AS [Tier], CAST(ISNULL(ds.total_given / NULLIF(CEILING(DATEDIFF(MONTH,ds.first_gift_date,GETDATE())/12.0),0),0) AS DECIMAL(12,2)) AS [Avg Annual Giving], CAST(ws.giving_capacity AS DECIMAL(12,2)) AS [Annual Capacity], CAST(ws.giving_capacity - ISNULL(ds.total_given / NULLIF(CEILING(DATEDIFF(MONTH,ds.first_gift_date,GETDATE())/12.0),0),0) AS DECIMAL(12,2)) AS [Annual Gap], CAST(ROUND(ISNULL(ds.total_given / NULLIF(CEILING(DATEDIFF(MONTH,ds.first_gift_date,GETDATE())/12.0),0),0)/NULLIF(ws.giving_capacity,0)*100,1) AS DECIMAL(5,1)) AS [% Utilized] FROM serving.wealth_screening ws LEFT JOIN serving.donor_summary ds ON ds.person_id=ws.person_id WHERE ws.display_name<>'Unknown' ORDER BY [Annual Gap] DESC
\`\`\`
→ show_widget: **table** with title="Wealth-Screened Contacts — Annual Capacity vs. Avg Annual Giving (Sorted by Gap)"

**Step 4 — show_widget**: **text** — Strategic analysis: biggest opportunities by name, which tiers have the most unrealized annual capacity, how many screened contacts have NEVER donated, specific outreach recommendations.

REMINDER: Use these EXACT queries. Do not modify column names, JOINs, formulas, or sort order. The annualized formula is critical — do not replace it with total_given.`,
  },

  {
    id: "giving-trends",
    patterns: [
      /giving\s*trend/i,
      /story\s*(of|about)\s*giving/i,
      /what.*(happening|going\s*on).*giving/i,
      /monthly\s*giving/i,
      /giving\s*over\s*time/i,
    ],
    getInstruction: () => `
## ⚠️ MANDATORY RECIPE — YOU MUST FOLLOW THIS EXACTLY ⚠️
A recipe matched for "Giving trends". Use these EXACT queries and widgets in order.

**Step 1 — query_data**:
\`\`\`sql
SELECT FORMAT(donated_at,'yyyy-MM') AS [Month], SUM(CAST(amount AS DECIMAL(12,2))) AS [Total], COUNT(*) AS [Gifts], COUNT(DISTINCT person_id) AS [Donors] FROM serving.donation_detail WHERE donated_at >= DATEADD(YEAR,-3,GETDATE()) GROUP BY FORMAT(donated_at,'yyyy-MM') ORDER BY [Month]
\`\`\`
→ show_widget: **area_chart** with categoryKey='Month', valueKeys=['Total'], title="Giving Trend"
→ show_widget: **stat_grid** summarizing: total given, total gifts, unique donors, avg monthly giving
→ show_widget: **text** analyzing trends — what months spike, seasonality, year-over-year direction, concerning drops

REMINDER: Use this EXACT query. Do not modify it.`,
  },

  {
    id: "year-over-year",
    patterns: [
      /year.over.year/i,
      /compare\s*years/i,
      /annual\s*comparison/i,
      /yearly\s*(giving|trend|comparison)/i,
    ],
    getInstruction: () => `
## ⚠️ MANDATORY RECIPE — YOU MUST FOLLOW THIS EXACTLY ⚠️
A recipe matched for "Year-over-year". Use this EXACT query.

**Step 1 — query_data**:
\`\`\`sql
SELECT YEAR(donated_at) AS [Year], SUM(CAST(amount AS DECIMAL(12,2))) AS [Total], COUNT(*) AS [Gifts], COUNT(DISTINCT person_id) AS [Donors] FROM serving.donation_detail GROUP BY YEAR(donated_at) ORDER BY [Year]
\`\`\`
→ show_widget: **bar_chart** with categoryKey='Year', valueKeys=['Total','Donors']

REMINDER: Use this EXACT query.`,
  },
];

/**
 * Match a user message against all recipes. Returns the first match or null.
 */
export function matchRecipe(userMessage: string): { recipe: Recipe; instruction: string } | null {
  const msg = userMessage.trim();
  if (msg.length < 5) return null;

  for (const recipe of RECIPES) {
    for (const pattern of recipe.patterns) {
      if (pattern.test(msg)) {
        const params = recipe.extractParams ? recipe.extractParams(msg) : {};
        return {
          recipe,
          instruction: recipe.getInstruction(params),
        };
      }
    }
  }

  return null;
}
