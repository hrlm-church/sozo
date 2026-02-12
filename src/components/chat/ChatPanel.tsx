"use client";

import { useChat } from "@ai-sdk/react";
import { useRef, useEffect, useState, useMemo } from "react";
import type { UIMessage } from "ai";
import { isToolUIPart, getToolName } from "ai";
import { WidgetRenderer } from "@/components/widgets/WidgetRenderer";
import { SuggestedPrompts } from "./SuggestedPrompts";
import { useDashboardStore } from "@/lib/stores/dashboard-store";
import type { Widget } from "@/types/widget";

export function ChatPanel() {
  const scrollRef = useRef<HTMLDivElement>(null);
  const addWidget = useDashboardStore((s) => s.addWidget);
  const widgets = useDashboardStore((s) => s.widgets);
  const pinnedIds = useMemo(() => new Set(widgets.map((w) => w.id)), [widgets]);
  const [input, setInput] = useState("");

  const { messages, sendMessage, status, error } = useChat();

  const isLoading = status === "submitted" || status === "streaming";

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  function getWidgets(message: UIMessage): Widget[] {
    const result: Widget[] = [];
    if (!message.parts) return result;
    for (const part of message.parts) {
      if (isToolUIPart(part)) {
        const name = getToolName(part);
        const p = part as Record<string, unknown>;
        if (name === "show_widget" && p.state === "output-available" && p.output) {
          const output = p.output as { widget?: Widget };
          if (output.widget) result.push(output.widget);
        }
      }
    }
    return result;
  }

  function getTextContent(message: UIMessage): string {
    if (!message.parts) return "";
    return message.parts
      .filter((p) => p.type === "text")
      .map((p) => (p as { type: "text"; text: string }).text)
      .join("");
  }

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || isLoading) return;
    sendMessage({ text: input.trim() });
    setInput("");
  };

  const handleSuggestedPrompt = (prompt: string) => {
    sendMessage({ text: prompt });
  };

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
        background: "#fff",
        borderRight: "1px solid var(--surface-border)",
      }}
    >
      {/* Header */}
      <div style={{ padding: "18px 24px", borderBottom: "1px solid var(--surface-border)", flexShrink: 0 }}>
        <h2 style={{
          fontSize: "1.1rem", fontWeight: 700, margin: 0, letterSpacing: "-0.02em",
          background: "var(--accent-gradient)", WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent",
        }}>
          Sozo
        </h2>
        <p style={{ fontSize: "0.72rem", color: "var(--text-muted)", margin: 0, marginTop: 2 }}>
          Ministry Intelligence
        </p>
      </div>

      {/* Messages area */}
      <div ref={scrollRef} style={{ flex: 1, overflow: "auto", padding: "20px 24px" }}>
        {messages.length === 0 ? (
          <SuggestedPrompts onSelect={handleSuggestedPrompt} />
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
            {messages.map((message) => {
              const text = getTextContent(message);
              const msgWidgets = getWidgets(message);
              return (
                <div key={message.id}>
                  {text && (
                    <div
                      style={{
                        padding: "10px 16px",
                        borderRadius: 18,
                        fontSize: "0.84rem",
                        lineHeight: 1.55,
                        ...(message.role === "user"
                          ? {
                              background: "var(--accent-gradient)",
                              color: "#fff",
                              marginLeft: 48,
                              borderBottomRightRadius: 6,
                            }
                          : {
                              background: "#f0f0f2",
                              color: "var(--text-primary)",
                              marginRight: 24,
                              borderBottomLeftRadius: 6,
                            }),
                      }}
                    >
                      {text}
                    </div>
                  )}
                  {msgWidgets.map((widget) => (
                    <div key={widget.id} style={{ marginTop: 10, height: 280 }}>
                      <WidgetRenderer
                        widget={widget}
                        onPin={() => addWidget(widget)}
                        isPinned={pinnedIds.has(widget.id)}
                      />
                    </div>
                  ))}
                </div>
              );
            })}
            {isLoading && (
              <div style={{
                padding: "10px 16px", borderRadius: 18, background: "#f0f0f2",
                color: "var(--text-muted)", fontSize: "0.84rem", marginRight: 24,
                borderBottomLeftRadius: 6,
              }}>
                Thinking...
              </div>
            )}
          </div>
        )}
      </div>

      {error && (
        <div style={{ padding: "8px 24px", fontSize: "0.75rem", color: "var(--red)", flexShrink: 0 }}>
          {error.message}
        </div>
      )}

      {/* Input area */}
      <form
        onSubmit={handleSubmit}
        style={{
          padding: "14px 24px",
          borderTop: "1px solid var(--surface-border)",
          display: "flex",
          gap: 10,
          flexShrink: 0,
        }}
      >
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="Ask anything..."
          disabled={isLoading}
          style={{
            flex: 1,
            padding: "10px 16px",
            borderRadius: "var(--r-pill)",
            border: "1px solid var(--surface-border-strong)",
            background: "var(--app-bg)",
            fontSize: "0.84rem",
            outline: "none",
            color: "var(--text-primary)",
            transition: "border-color 150ms ease",
          }}
          onFocus={(e) => { e.currentTarget.style.borderColor = "var(--accent)"; }}
          onBlur={(e) => { e.currentTarget.style.borderColor = "var(--surface-border-strong)"; }}
        />
        <button
          type="submit"
          disabled={isLoading || !input.trim()}
          className="gradient-btn"
          style={{ padding: "10px 22px", fontSize: "0.84rem" }}
        >
          Send
        </button>
      </form>
    </div>
  );
}
