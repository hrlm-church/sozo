/**
 * Feature flags — gated via environment variables.
 * Set FEATURE_<NAME>=1 to enable in a given environment.
 */

function isEnabled(flag: string): boolean {
  return process.env[flag] === "1" || process.env[flag] === "true";
}

export const features = {
  briefing: () => isEnabled("FEATURE_BRIEFING"),
  actions: () => isEnabled("FEATURE_ACTIONS"),
  writeback: () => isEnabled("FEATURE_WRITEBACK"),
  alerts: () => isEnabled("FEATURE_ALERTS"),
  goals: () => isEnabled("FEATURE_GOALS"),
} as const;
