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

  // Auto-scroll to bottom on new messages
  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  /** Extract widgets from tool invocations in a message */
  function getWidgets(message: UIMessage): Widget[] {
    const result: Widget[] = [];
    if (!message.parts) return result;
    for (const part of message.parts) {
      if (isToolUIPart(part)) {
        const name = getToolName(part);
        const p = part as Record<string, unknown>;
        if (name === "show_widget" && p.state === "output-available" && p.output) {
          const output = p.output as { widget?: Widget };
          if (output.widget) {
            result.push(output.widget);
          }
        }
      }
    }
    return result;
  }

  /** Get text content from a message */
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
        background: "var(--surface-strong)",
        borderRight: "1px solid var(--surface-border)",
      }}
    >
      {/* Header */}
      <div
        style={{
          padding: "16px 20px",
          borderBottom: "1px solid var(--surface-border)",
          flexShrink: 0,
        }}
      >
        <h2 style={{ fontSize: "1rem", fontWeight: 650, color: "var(--text-primary)" }}>
          Sozo Chat
        </h2>
        <p style={{ fontSize: "0.75rem", color: "var(--text-muted)", marginTop: 2 }}>
          Ask anything or build a dashboard
        </p>
      </div>

      {/* Messages area */}
      <div
        ref={scrollRef}
        style={{
          flex: 1,
          overflow: "auto",
          padding: "16px 20px",
        }}
      >
        {messages.length === 0 ? (
          <SuggestedPrompts onSelect={handleSuggestedPrompt} />
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
            {messages.map((message) => {
              const text = getTextContent(message);
              const widgets = getWidgets(message);

              return (
                <div key={message.id}>
                  {/* Text content */}
                  {text && (
                    <div
                      style={{
                        padding: "10px 14px",
                        borderRadius: "var(--r-md)",
                        fontSize: "0.85rem",
                        lineHeight: 1.5,
                        ...(message.role === "user"
                          ? {
                              background: "var(--accent-purple)",
                              color: "#fff",
                              marginLeft: 40,
                            }
                          : {
                              background: "var(--surface)",
                              color: "var(--text-primary)",
                              marginRight: 20,
                            }),
                      }}
                    >
                      {text}
                    </div>
                  )}

                  {/* Inline widgets from tool invocations */}
                  {widgets.map((widget) => (
                    <div key={widget.id} style={{ marginTop: 8, height: 280 }}>
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
              <div
                style={{
                  padding: "10px 14px",
                  borderRadius: "var(--r-md)",
                  background: "var(--surface)",
                  color: "var(--text-muted)",
                  fontSize: "0.85rem",
                  marginRight: 20,
                }}
              >
                Thinking...
              </div>
            )}
          </div>
        )}
      </div>

      {/* Error display */}
      {error && (
        <div
          style={{
            padding: "8px 20px",
            fontSize: "0.8rem",
            color: "#ef4444",
            flexShrink: 0,
          }}
        >
          {error.message}
        </div>
      )}

      {/* Input area */}
      <form
        onSubmit={handleSubmit}
        style={{
          padding: "12px 20px",
          borderTop: "1px solid var(--surface-border)",
          display: "flex",
          gap: 8,
          flexShrink: 0,
        }}
      >
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="Ask anything or say 'build me a dashboard'..."
          disabled={isLoading}
          style={{
            flex: 1,
            padding: "10px 14px",
            borderRadius: "var(--r-md)",
            border: "1px solid var(--surface-border)",
            background: "var(--surface)",
            fontSize: "0.85rem",
            outline: "none",
            color: "var(--text-primary)",
          }}
        />
        <button
          type="submit"
          disabled={isLoading || !input.trim()}
          className="gradient-btn"
          style={{ padding: "10px 20px", fontSize: "0.85rem" }}
        >
          Send
        </button>
      </form>
    </div>
  );
}
