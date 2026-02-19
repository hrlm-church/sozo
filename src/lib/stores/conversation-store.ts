import { create } from "zustand";

export interface SavedConversation {
  id: string;
  title: string;
  message_count: number;
  created_at: string;
  updated_at: string;
}

interface ConversationState {
  /** Current active conversation ID (null = new unsaved chat) */
  conversationId: string | null;
  /** List of saved conversations */
  conversations: SavedConversation[];
  /** Loading state */
  loading: boolean;

  /** Start a new chat (clears current conversation) */
  newChat: () => void;
  /** Set the active conversation */
  setConversationId: (id: string) => void;
  /** Refresh conversation list from server */
  refreshList: () => Promise<void>;
  /** Delete a conversation */
  deleteConversation: (id: string) => Promise<void>;
}

export const useConversationStore = create<ConversationState>((set, get) => ({
  conversationId: null,
  conversations: [],
  loading: false,

  newChat: () => set({ conversationId: null }),

  setConversationId: (id: string) => set({ conversationId: id }),

  refreshList: async () => {
    set({ loading: true });
    try {
      const res = await fetch("/api/conversation/list");
      if (res.ok) {
        const data = await res.json();
        set({ conversations: data.conversations ?? [] });
      }
    } catch {
      // Silently fail — list will be empty
    } finally {
      set({ loading: false });
    }
  },

  deleteConversation: async (id: string) => {
    try {
      await fetch(`/api/conversation/delete?id=${encodeURIComponent(id)}`, {
        method: "DELETE",
      });
      const { conversations, conversationId } = get();
      set({
        conversations: conversations.filter((c) => c.id !== id),
        ...(conversationId === id ? { conversationId: null } : {}),
      });
    } catch {
      // Silently fail
    }
  },
}));
