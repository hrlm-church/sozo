import type { WidgetConfig } from "@/types/widget";

/**
 * Auto-pivot long-format data to wide-format when seriesKey is set.
 *
 * Input (long):  [{name:"Alice", month:"Jan", amount:100}, {name:"Bob", month:"Jan", amount:200}]
 * With seriesKey="name", categoryKey="month", valueKeys=["amount"]
 * Output (wide): [{month:"Jan", Alice:100, Bob:200}]
 *
 * Also returns the derived valueKeys (one per unique series value) and a color map.
 */

const PALETTE = [
  "#6f43ea", "#2f7ff6", "#17c6b8", "#f59e0b", "#f43f5e",
  "#8b5cf6", "#ec4899", "#14b8a6", "#f97316", "#10b981",
  "#3b82f6", "#fbbf24", "#fb7185", "#60a5fa", "#34d399",
  "#a78bfa", "#f472b6", "#2dd4bf", "#fb923c", "#6ee7b7",
];

export function pivotIfNeeded(
  data: Record<string, unknown>[],
  config: WidgetConfig,
): { data: Record<string, unknown>[]; valueKeys: string[]; colors: Record<string, string> } {
  const { seriesKey, categoryKey, valueKeys, colors: configColors } = config;

  if (!seriesKey || !categoryKey || !data.length) {
    return { data, valueKeys: valueKeys ?? [], colors: configColors ?? {} };
  }

  const valueKey = valueKeys?.[0] ?? "value";

  // Collect unique series names (preserve order of appearance)
  const seriesNames: string[] = [];
  const seen = new Set<string>();
  for (const row of data) {
    const s = String(row[seriesKey] ?? "");
    if (!seen.has(s)) { seen.add(s); seriesNames.push(s); }
  }

  // Pivot: group by categoryKey, spread seriesKey values into columns
  const buckets = new Map<string, Record<string, unknown>>();
  const categoryOrder: string[] = [];
  for (const row of data) {
    const cat = String(row[categoryKey] ?? "");
    const series = String(row[seriesKey] ?? "");
    const val = row[valueKey] ?? 0;
    if (!buckets.has(cat)) {
      buckets.set(cat, { [categoryKey]: cat });
      categoryOrder.push(cat);
    }
    buckets.get(cat)![series] = val;
  }

  // Build color map
  const colors: Record<string, string> = {};
  seriesNames.forEach((name, i) => {
    colors[name] = configColors?.[name] ?? PALETTE[i % PALETTE.length];
  });

  return {
    data: categoryOrder.map((cat) => buckets.get(cat)!),
    valueKeys: seriesNames,
    colors,
  };
}
