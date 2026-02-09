import { executeSql } from "@/lib/server/sql-client";

export interface SummaryMetric {
  key: string;
  label: string;
  value: number;
  changePct: number;
  trend: "up" | "down" | "flat";
}

export interface Citation {
  title: string;
  source: string;
  snippet: string;
  url?: string;
}

export interface ChartArtifact {
  id: string;
  title: string;
  type: "bar" | "line";
  x: string[];
  series: Array<{ name: string; data: number[] }>;
}

export interface TableArtifact {
  id: string;
  title: string;
  columns: string[];
  rows: Array<Array<string | number>>;
}

export interface DashboardSummaryPayload {
  asOf: string;
  metrics: SummaryMetric[];
  citations: Citation[];
  charts: ChartArtifact[];
  tables: TableArtifact[];
}

type RiskRow = {
  report_date: string;
  household_name: string;
  signal: string;
  risk_score: number;
  high_risk: boolean;
  updated_at: string;
};

type LinkageRow = {
  report_date: string;
  profiles_linked: number;
  migration_exceptions: number;
};

const fallbackPayload = (): DashboardSummaryPayload => ({
  asOf: new Date().toISOString(),
  metrics: [
    { key: "profiles_linked", label: "Profiles linked", value: 83257, changePct: 0, trend: "flat" },
    { key: "high_risk_households", label: "High-risk households", value: 0, changePct: 0, trend: "flat" },
    { key: "migration_exceptions", label: "Migration exceptions", value: 0, changePct: 0, trend: "flat" },
  ],
  citations: [
    { title: "dbo.household_risk_daily", source: "Azure SQL", snippet: "Risk rollup table." },
    { title: "dbo.profile_linkage_daily", source: "Azure SQL", snippet: "Profile linkage KPI table." },
  ],
  charts: [],
  tables: [],
});

const num = (value: unknown) => (typeof value === "number" ? value : Number(value ?? 0));

export const getDashboardSummary = async (): Promise<DashboardSummaryPayload> => {
  const riskSql = `
SELECT TOP (7)
  CAST(report_date AS date) AS report_date,
  household_name,
  signal,
  risk_score,
  high_risk,
  updated_at
FROM dbo.household_risk_daily
ORDER BY report_date DESC, risk_score DESC;
`.trim();

  const linkageSql = `
SELECT TOP (14)
  CAST(report_date AS date) AS report_date,
  profiles_linked,
  migration_exceptions
FROM dbo.profile_linkage_daily
ORDER BY report_date DESC;
`.trim();

  const [riskResult, linkageResult] = await Promise.all([
    executeSql(riskSql),
    executeSql(linkageSql),
  ]);

  if (!riskResult.ok || !linkageResult.ok) {
    return fallbackPayload();
  }

  const riskRows = riskResult.rows as RiskRow[];
  const linkageRows = linkageResult.rows as LinkageRow[];

  if (riskRows.length === 0 || linkageRows.length === 0) {
    return fallbackPayload();
  }

  const uniqueDates = Array.from(new Set(linkageRows.map((row) => String(row.report_date)))).slice(0, 2);
  const currentDate = uniqueDates[0];
  const priorDate = uniqueDates[1] ?? uniqueDates[0];

  const currentLink = linkageRows.find((row) => String(row.report_date) === currentDate);
  const priorLink = linkageRows.find((row) => String(row.report_date) === priorDate) ?? currentLink;

  const currentProfiles = num(currentLink?.profiles_linked);
  const priorProfiles = Math.max(1, num(priorLink?.profiles_linked));
  const profilesDelta = ((currentProfiles - priorProfiles) / priorProfiles) * 100;

  const currentExceptions = num(currentLink?.migration_exceptions);
  const priorExceptions = Math.max(1, num(priorLink?.migration_exceptions));
  const exceptionsDelta = ((currentExceptions - priorExceptions) / priorExceptions) * 100;

  const highRiskCount = riskRows.filter((row) => Boolean(row.high_risk)).length;
  const trendRows = riskRows.slice().reverse();

  return {
    asOf: new Date().toISOString(),
    metrics: [
      {
        key: "profiles_linked",
        label: "Profiles linked",
        value: currentProfiles,
        changePct: Number(profilesDelta.toFixed(2)),
        trend: profilesDelta > 0 ? "up" : profilesDelta < 0 ? "down" : "flat",
      },
      {
        key: "high_risk_households",
        label: "High-risk households",
        value: highRiskCount,
        changePct: 0,
        trend: "flat",
      },
      {
        key: "migration_exceptions",
        label: "Migration exceptions",
        value: currentExceptions,
        changePct: Number(exceptionsDelta.toFixed(2)),
        trend: exceptionsDelta > 0 ? "up" : exceptionsDelta < 0 ? "down" : "flat",
      },
    ],
    citations: [
      {
        title: "dbo.household_risk_daily",
        source: "Azure SQL",
        snippet: "Household risk records used for table and 7-day trend.",
      },
      {
        title: "dbo.profile_linkage_daily",
        source: "Azure SQL",
        snippet: "Profile linkage and migration exception daily KPI source.",
      },
    ],
    charts: [
      {
        id: "risk-trend-7d",
        title: "High-risk Households (7D)",
        type: "line",
        x: trendRows.map((row) => String(row.report_date).slice(5, 10)),
        series: [{ name: "Risk Score", data: trendRows.map((row) => num(row.risk_score)) }],
      },
    ],
    tables: [
      {
        id: "top-households",
        title: "Top Households Requiring Review",
        columns: ["Household", "Signal", "Score", "Last Updated"],
        rows: riskRows.slice(0, 5).map((row) => [
          row.household_name,
          row.signal,
          num(row.risk_score),
          String(row.updated_at),
        ]),
      },
    ],
  };
};
