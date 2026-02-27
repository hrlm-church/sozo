/**
 * Azure Application Insights integration.
 * Auto-collects: unhandled exceptions, HTTP dependencies (SQL, OpenAI),
 * performance counters, and custom events/metrics.
 *
 * Set APPLICATIONINSIGHTS_CONNECTION_STRING env var to enable.
 * When the env var is missing, all methods are no-ops (safe for local dev).
 */

import type { TelemetryClient } from "applicationinsights";

let client: TelemetryClient | null = null;
let initialized = false;

function getClient(): TelemetryClient | null {
  if (initialized) return client;
  initialized = true;

  const connectionString = process.env.APPLICATIONINSIGHTS_CONNECTION_STRING;
  if (!connectionString) return null;

  try {
    // Dynamic import to avoid bundling in dev when not configured
    const appInsights = require("applicationinsights");
    appInsights
      .setup(connectionString)
      .setAutoCollectRequests(false) // We track manually for more control
      .setAutoCollectPerformance(true, true)
      .setAutoCollectExceptions(true)
      .setAutoCollectDependencies(true) // Tracks SQL, HTTP calls automatically
      .setAutoCollectConsole(false)
      .setUseDiskRetryCaching(true)
      .setSendLiveMetrics(false)
      .start();

    client = appInsights.defaultClient;
    client!.context.tags[client!.context.keys.cloudRole] = "sozo-app";
    return client;
  } catch (err) {
    console.warn("[telemetry] Failed to initialize Application Insights:", err);
    return null;
  }
}

/** Track an API request with timing and status */
export function trackRequest(opts: {
  name: string;
  url: string;
  duration: number;
  statusCode: number;
  success: boolean;
  requestId?: string;
  userId?: string;
}) {
  const c = getClient();
  if (!c) return;

  c.trackRequest({
    name: opts.name,
    url: opts.url,
    duration: opts.duration,
    resultCode: String(opts.statusCode),
    success: opts.success,
    properties: {
      requestId: opts.requestId ?? "",
      userId: opts.userId ?? "",
    },
  });
}

/** Track a custom event (e.g., "conversation_saved", "feedback_submitted") */
export function trackEvent(name: string, properties?: Record<string, string>, measurements?: Record<string, number>) {
  const c = getClient();
  if (!c) return;

  c.trackEvent({ name, properties, measurements });
}

/** Track a custom metric (e.g., "chat_response_time_ms", "token_usage") */
export function trackMetric(name: string, value: number) {
  const c = getClient();
  if (!c) return;

  c.trackMetric({ name, value });
}

/** Track an exception */
export function trackException(error: Error, properties?: Record<string, string>) {
  const c = getClient();
  if (!c) return;

  c.trackException({ exception: error, properties });
}

/** Flush telemetry buffer (call on shutdown) */
export function flushTelemetry(): void {
  const c = getClient();
  if (!c) return;
  c.flush();
}
