"use client";

import { useChat } from "@ai-sdk/react";
import { useRef, useEffect, useState, useMemo, useCallback } from "react";
import type { UIMessage } from "ai";
import { isToolUIPart, getToolName } from "ai";
import { WidgetRenderer } from "@/components/widgets/WidgetRenderer";
import { SuggestedPrompts } from "./SuggestedPrompts";
import { ConversationSidebar } from "./ConversationSidebar";
import { useDashboardStore } from "@/lib/stores/dashboard-store";
import { useConversationStore } from "@/lib/stores/conversation-store";
import type { Widget } from "@/types/widget";

export function ChatPanel() {
  const scrollRef = useRef<HTMLDivElement>(null);
  const addWidget = useDashboardStore((s) => s.addWidget);
  const widgets = useDashboardStore((s) => s.widgets);
  const pinnedIds = useMemo(() => new Set(widgets.map((w) => w.id)), [widgets]);
  const [input, setInput] = useState("");
  const [sidebarOpen, setSidebarOpen] = useState(true);

  const conversationId = useConversationStore((s) => s.conversationId);
  const setConversationId = useConversationStore((s) => s.setConversationId);
  const refreshList = useConversationStore((s) => s.refreshList);

  // Generate a stable conversation ID for new chats
  const [activeConvId, setActiveConvId] = useState<string | null>(null);

  const { messages, sendMessage, status, error, setMessages } = useChat();
  const isLoading = status === "submitted" || status === "streaming";
  const prevStatusRef = useRef(status);

  // When conversation changes in sidebar, load it
  useEffect(() => {
    if (conversationId && conversationId !== activeConvId) {
      loadConversation(conversationId);
    } else if (conversationId === null && activeConvId !== null) {
      // New chat
      setMessages([]);
      setActiveConvId(null);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [conversationId]);

  const loadConversation = async (id: string) => {
    try {
      const res = await fetch(`/api/conversation/load?id=${encodeURIComponent(id)}`);
      if (!res.ok) return;
      const data = await res.json();
      const restored: UIMessage[] = (data.messages || []).map(
        (m: { id: string; role: string; content: string }) => {
          try {
            return JSON.parse(m.content);
          } catch {
            return { id: m.id, role: m.role, parts: [{ type: "text", text: m.content }] };
          }
        },
      );
      setMessages(restored);
      setActiveConvId(id);
    } catch {
      // Failed to load — stay on current chat
    }
  };

  // Auto-save when assistant finishes responding
  const saveConversation = useCallback(
    async (msgs: UIMessage[]) => {
      if (msgs.length === 0) return;
      const convId = activeConvId || crypto.randomUUID();
      if (!activeConvId) {
        setActiveConvId(convId);
        setConversationId(convId);
      }

      // Extract title from first user message
      const firstUser = msgs.find((m) => m.role === "user");
      const title = firstUser
        ? getTextContent(firstUser).slice(0, 100) || "New Chat"
        : "New Chat";

      const serialized = msgs.map((m) => ({
        id: m.id,
        role: m.role,
        content: JSON.stringify(m),
      }));

      try {
        await fetch("/api/conversation/save", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            conversationId: convId,
            title,
            messages: serialized,
          }),
        });
        refreshList();
      } catch {
        // Silent fail — conversation will be saved on next message
      }
    },
    [activeConvId, setConversationId, refreshList],
  );

  // Trigger save when streaming finishes
  useEffect(() => {
    if (
      prevStatusRef.current === "streaming" &&
      status === "ready" &&
      messages.length > 0
    ) {
      saveConversation(messages);
    }
    prevStatusRef.current = status;
  }, [status, messages, saveConversation]);

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
      .join("")
      .replace(/\{\{widget:id=[^}]+\}\}\s*/g, "")
      .trim();
  }

  function getActiveToolCalls(message: UIMessage): string[] {
    if (!message.parts) return [];
    const active: string[] = [];
    for (const part of message.parts) {
      if (isToolUIPart(part)) {
        const name = getToolName(part);
        const p = part as Record<string, unknown>;
        if (name === "query_data" && p.state !== "output-available") {
          active.push("Querying database...");
        } else if (name === "build_360" && p.state !== "output-available") {
          active.push("Building 360 profiles...");
        } else if (name === "search_data" && p.state !== "output-available") {
          active.push("Searching profiles...");
        } else if (name === "show_widget" && p.state !== "output-available") {
          active.push("Building visualization...");
        }
      }
    }
    return active;
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
    <div style={{ display: "flex", height: "100%" }}>
      {/* Conversation Sidebar */}
      {sidebarOpen && <ConversationSidebar />}

      {/* Main Chat Area */}
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          height: "100%",
          flex: 1,
          background: "#fff",
          borderRight: "1px solid var(--surface-border)",
          minWidth: 0,
        }}
      >
        {/* Header */}
        <div
          style={{
            padding: "12px 20px",
            borderBottom: "1px solid var(--surface-border)",
            flexShrink: 0,
            display: "flex",
            alignItems: "center",
            gap: 10,
          }}
        >
          <button
            onClick={() => setSidebarOpen(!sidebarOpen)}
            title={sidebarOpen ? "Hide history" : "Show history"}
            style={{
              background: "none",
              border: "none",
              cursor: "pointer",
              fontSize: "0.85rem",
              color: "var(--text-muted)",
              padding: "2px 4px",
            }}
          >
            {sidebarOpen ? "\u2630" : "\u2630"}
          </button>
          <div>
            <h2
              style={{
                fontSize: "1.05rem",
                fontWeight: 700,
                margin: 0,
                letterSpacing: "-0.02em",
                background: "var(--accent-gradient)",
                WebkitBackgroundClip: "text",
                WebkitTextFillColor: "transparent",
              }}
            >
              Sozo
            </h2>
            <p
              style={{
                fontSize: "0.68rem",
                color: "var(--text-muted)",
                margin: 0,
                marginTop: 1,
              }}
            >
              Ministry Intelligence
            </p>
          </div>
        </div>

        {/* Messages area */}
        <div ref={scrollRef} style={{ flex: 1, overflow: "auto", padding: "20px 20px" }}>
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
                    {msgWidgets.map((widget) => {
                      const tall =
                        widget.type === "stat_grid" ||
                        widget.type === "table" ||
                        widget.type === "drill_down_table";
                      return (
                        <div
                          key={widget.id}
                          style={{ marginTop: 10, minHeight: tall ? 360 : 280 }}
                        >
                          <WidgetRenderer
                            widget={widget}
                            onPin={() => addWidget(widget)}
                            isPinned={pinnedIds.has(widget.id)}
                          />
                          {!pinnedIds.has(widget.id) && (
                            <button
                              onClick={() => addWidget(widget)}
                              style={{
                                display: "block",
                                margin: "6px auto 0",
                                padding: "5px 16px",
                                fontSize: "0.72rem",
                                fontWeight: 600,
                                color: "var(--accent)",
                                background: "var(--accent-light)",
                                border: "1px solid var(--accent)",
                                borderRadius: "var(--r-pill)",
                                cursor: "pointer",
                                transition: "all 120ms ease",
                              }}
                              onMouseEnter={(e) => {
                                e.currentTarget.style.background = "var(--accent)";
                                e.currentTarget.style.color = "#fff";
                              }}
                              onMouseLeave={(e) => {
                                e.currentTarget.style.background = "var(--accent-light)";
                                e.currentTarget.style.color = "var(--accent)";
                              }}
                            >
                              + Pin to Dashboard
                            </button>
                          )}
                        </div>
                      );
                    })}
                  </div>
                );
              })}
              {isLoading &&
                (() => {
                  const lastAssistant = [...messages]
                    .reverse()
                    .find((m) => m.role === "assistant");
                  const toolActivity = lastAssistant
                    ? getActiveToolCalls(lastAssistant)
                    : [];
                  return (
                    <div
                      style={{
                        padding: "10px 16px",
                        borderRadius: 18,
                        background: "#f0f0f2",
                        color: "var(--text-muted)",
                        fontSize: "0.84rem",
                        marginRight: 24,
                        borderBottomLeftRadius: 6,
                      }}
                    >
                      {toolActivity.length > 0
                        ? toolActivity[toolActivity.length - 1]
                        : "Thinking..."}
                    </div>
                  );
                })()}
            </div>
          )}
        </div>

        {error && (
          <div
            style={{
              padding: "8px 20px",
              fontSize: "0.75rem",
              color: "var(--red)",
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
            padding: "14px 20px",
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
            onFocus={(e) => {
              e.currentTarget.style.borderColor = "var(--accent)";
            }}
            onBlur={(e) => {
              e.currentTarget.style.borderColor = "var(--surface-border-strong)";
            }}
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
    </div>
  );
}
