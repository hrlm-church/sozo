"use client";

import { useEffect } from "react";
import {
  useConversationStore,
  type SavedConversation,
} from "@/lib/stores/conversation-store";

export function ConversationSidebar() {
  const conversations = useConversationStore((s) => s.conversations);
  const conversationId = useConversationStore((s) => s.conversationId);
  const loading = useConversationStore((s) => s.loading);
  const newChat = useConversationStore((s) => s.newChat);
  const setConversationId = useConversationStore((s) => s.setConversationId);
  const refreshList = useConversationStore((s) => s.refreshList);
  const deleteConversation = useConversationStore((s) => s.deleteConversation);

  useEffect(() => {
    refreshList();
  }, [refreshList]);

  const formatDate = (dateStr: string) => {
    const d = new Date(dateStr);
    const now = new Date();
    const diffMs = now.getTime() - d.getTime();
    const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
    if (diffDays === 0) return "Today";
    if (diffDays === 1) return "Yesterday";
    if (diffDays < 7) return `${diffDays}d ago`;
    return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  };

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
        background: "var(--app-bg)",
        borderRight: "1px solid var(--surface-border)",
        width: 220,
        flexShrink: 0,
      }}
    >
      {/* New Chat Button */}
      <div style={{ padding: "12px 12px 8px" }}>
        <button
          onClick={newChat}
          style={{
            width: "100%",
            padding: "8px 12px",
            fontSize: "0.78rem",
            fontWeight: 600,
            color: "#fff",
            background: "var(--accent-gradient)",
            border: "none",
            borderRadius: 8,
            cursor: "pointer",
            transition: "opacity 150ms ease",
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.opacity = "0.9";
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.opacity = "1";
          }}
        >
          + New Chat
        </button>
      </div>

      {/* Conversation List */}
      <div style={{ flex: 1, overflow: "auto", padding: "4px 8px" }}>
        {loading && conversations.length === 0 && (
          <p
            style={{
              fontSize: "0.72rem",
              color: "var(--text-muted)",
              textAlign: "center",
              padding: 16,
            }}
          >
            Loading...
          </p>
        )}
        {!loading && conversations.length === 0 && (
          <p
            style={{
              fontSize: "0.72rem",
              color: "var(--text-muted)",
              textAlign: "center",
              padding: 16,
            }}
          >
            No conversations yet
          </p>
        )}
        {conversations.map((conv: SavedConversation) => {
          const isActive = conv.id === conversationId;
          return (
            <div
              key={conv.id}
              onClick={() => setConversationId(conv.id)}
              style={{
                padding: "8px 10px",
                marginBottom: 2,
                borderRadius: 6,
                cursor: "pointer",
                background: isActive ? "rgba(6, 147, 227, 0.12)" : "transparent",
                border: isActive
                  ? "1px solid rgba(6, 147, 227, 0.25)"
                  : "1px solid transparent",
                transition: "all 100ms ease",
              }}
              onMouseEnter={(e) => {
                if (!isActive)
                  e.currentTarget.style.background = "rgba(255, 255, 255, 0.04)";
              }}
              onMouseLeave={(e) => {
                if (!isActive)
                  e.currentTarget.style.background = "transparent";
              }}
            >
              <div
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "flex-start",
                }}
              >
                <span
                  style={{
                    fontSize: "0.75rem",
                    fontWeight: isActive ? 600 : 400,
                    color: "var(--text-primary)",
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                    flex: 1,
                    marginRight: 4,
                  }}
                >
                  {conv.title}
                </span>
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    deleteConversation(conv.id);
                  }}
                  title="Delete conversation"
                  style={{
                    background: "none",
                    border: "none",
                    padding: "0 2px",
                    cursor: "pointer",
                    fontSize: "0.7rem",
                    color: "var(--text-muted)",
                    opacity: 0.5,
                    flexShrink: 0,
                  }}
                  onMouseEnter={(e) => {
                    e.currentTarget.style.opacity = "1";
                    e.currentTarget.style.color = "#e74c3c";
                  }}
                  onMouseLeave={(e) => {
                    e.currentTarget.style.opacity = "0.5";
                    e.currentTarget.style.color = "var(--text-muted)";
                  }}
                >
                  x
                </button>
              </div>
              <span
                style={{
                  fontSize: "0.65rem",
                  color: "var(--text-muted)",
                }}
              >
                {formatDate(conv.updated_at)} &middot;{" "}
                {conv.message_count} msgs
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
