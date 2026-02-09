"use client";

import { FormEvent, useEffect, useState } from "react";
import { ChatApiResponse, UiMessage } from "@/types/chat";

interface HealthResponse {
  ok: boolean;
  timestamp: string;
  services?: Array<{
    service: string;
    status: string;
    detail: string;
    latencyMs?: number;
  }>;
}

interface SummaryResponse {
  asOf: string;
  metrics: Array<{
    key: string;
    label: string;
    value: number;
    changePct: number;
    trend: "up" | "down" | "flat";
  }>;
}

const prettyNumber = (value: number) => new Intl.NumberFormat("en-US").format(value);

export default function Home() {
  const [draft, setDraft] = useState("");
  const [messages, setMessages] = useState<UiMessage[]>([]);
  const [latestResponse, setLatestResponse] = useState<ChatApiResponse | null>(null);
  const [summary, setSummary] = useState<SummaryResponse | null>(null);
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [isSending, setIsSending] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const bootstrap = async () => {
      const [summaryResponse, healthResponse] = await Promise.allSettled([
        fetch("/api/dashboard/summary", { cache: "no-store" }),
        fetch("/api/health", { cache: "no-store" }),
      ]);

      if (summaryResponse.status === "fulfilled" && summaryResponse.value.ok) {
        setSummary((await summaryResponse.value.json()) as SummaryResponse);
      }

      if (healthResponse.status === "fulfilled") {
        const body = (await healthResponse.value.json()) as HealthResponse;
        setHealth(body);
      }
    };

    bootstrap().catch(() => {
      setError("Failed to load startup data.");
    });
  }, []);

  const onSubmit = async (event: FormEvent) => {
    event.preventDefault();
    if (!draft.trim()) {
      return;
    }

    const nextMessages: UiMessage[] = [...messages, { role: "user", content: draft.trim() }];
    setMessages(nextMessages);
    setDraft("");
    setError(null);
    setIsSending(true);

    try {
      const response = await fetch("/api/chat", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ messages: nextMessages }),
      });

      const body = (await response.json()) as ChatApiResponse | { error: string };
      if (!response.ok || !("answer" in body)) {
        throw new Error("error" in body ? body.error : "Chat request failed.");
      }

      setLatestResponse(body);
      setMessages((current) => [...current, { role: "assistant", content: body.answer }]);
    } catch (chatError) {
      setError(chatError instanceof Error ? chatError.message : "Unexpected chat error.");
    } finally {
      setIsSending(false);
    }
  };

  return (
    <main className="mx-auto max-w-6xl space-y-6 p-6">
      <header className="card-base p-6">
        <p className="text-sm font-medium uppercase tracking-[0.14em] text-slate-500">Sozo MVP</p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">Chat-first analyst workspace</h1>
        <p className="mt-2 text-sm text-slate-600">
          Backend routes: <code>/api/health</code>, <code>/api/chat</code>, <code>/api/dashboard/summary</code>
        </p>
      </header>

      <section className="grid grid-cols-1 gap-4 md:grid-cols-3">
        {summary?.metrics.map((metric) => (
          <article key={metric.key} className="card-base p-4">
            <p className="text-xs uppercase tracking-[0.1em] text-slate-500">{metric.label}</p>
            <p className="mt-2 text-2xl font-semibold">{prettyNumber(metric.value)}</p>
            <p className="mt-1 text-sm text-slate-600">{metric.changePct}% vs prior window</p>
          </article>
        ))}
      </section>

      <section className="grid grid-cols-1 gap-6 lg:grid-cols-[1.3fr_1fr]">
        <article className="card-base p-5">
          <h2 className="text-lg font-semibold">Chat</h2>

          <div className="mt-4 max-h-[420px] space-y-3 overflow-y-auto rounded-xl border border-slate-200 bg-white/70 p-3">
            {messages.length === 0 ? (
              <p className="text-sm text-slate-500">Ask for household KPIs, profile linkage status, or risk trend summaries.</p>
            ) : (
              messages.map((message, index) => (
                <div
                  key={`${message.role}-${index}`}
                  className={`rounded-xl p-3 text-sm ${
                    message.role === "user"
                      ? "ml-10 bg-slate-900 text-white"
                      : "mr-10 border border-slate-200 bg-slate-50 text-slate-800"
                  }`}
                >
                  {message.content}
                </div>
              ))
            )}
          </div>

          <form onSubmit={onSubmit} className="mt-4 flex gap-2">
            <input
              value={draft}
              onChange={(event) => setDraft(event.target.value)}
              placeholder="What changed in high-risk households this week?"
              className="w-full rounded-xl border border-slate-300 bg-white px-4 py-3 text-sm outline-none ring-slate-400 focus:ring"
            />
            <button
              type="submit"
              disabled={isSending}
              className="rounded-xl bg-slate-900 px-4 py-3 text-sm font-medium text-white disabled:opacity-50"
            >
              {isSending ? "Sending..." : "Send"}
            </button>
          </form>

          {error ? <p className="mt-3 text-sm text-red-700">{error}</p> : null}
        </article>

        <article className="card-base p-5">
          <h2 className="text-lg font-semibold">Structured output</h2>

          {!latestResponse ? (
            <p className="mt-3 text-sm text-slate-500">Citations, tables, and charts from the latest assistant response will appear here.</p>
          ) : (
            <div className="mt-3 space-y-4 text-sm">
              <div>
                <p className="font-semibold">Route</p>
                <p className="text-slate-600">
                  {latestResponse.route.tool} ({latestResponse.route.reason})
                </p>
              </div>

              <div>
                <p className="font-semibold">Citations</p>
                <ul className="mt-1 space-y-2">
                  {latestResponse.citations.map((citation) => (
                    <li key={`${citation.source}-${citation.title}`} className="rounded-lg border border-slate-200 p-2">
                      <p className="font-medium">{citation.title}</p>
                      <p className="text-slate-600">{citation.source}</p>
                      <p className="text-slate-500">{citation.snippet}</p>
                    </li>
                  ))}
                </ul>
              </div>

              <div>
                <p className="font-semibold">Charts</p>
                <pre className="mt-1 overflow-x-auto rounded-lg border border-slate-200 bg-slate-50 p-2 text-xs">
                  {JSON.stringify(latestResponse.artifacts.charts, null, 2)}
                </pre>
              </div>

              <div>
                <p className="font-semibold">Tables</p>
                <pre className="mt-1 overflow-x-auto rounded-lg border border-slate-200 bg-slate-50 p-2 text-xs">
                  {JSON.stringify(latestResponse.artifacts.tables, null, 2)}
                </pre>
              </div>
            </div>
          )}
        </article>
      </section>

      <section className="card-base p-5">
        <h2 className="text-lg font-semibold">Backend health</h2>
        {!health ? (
          <p className="mt-2 text-sm text-slate-500">Loading service checks...</p>
        ) : (
          <div className="mt-2 space-y-2 text-sm">
            {health.services?.map((service) => (
              <div key={service.service} className="rounded-lg border border-slate-200 bg-white/70 px-3 py-2">
                <p className="font-medium">
                  {service.service}: {service.status}
                </p>
                <p className="text-slate-600">{service.detail}</p>
              </div>
            ))}
          </div>
        )}
      </section>
    </main>
  );
}
