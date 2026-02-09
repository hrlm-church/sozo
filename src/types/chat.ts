import type {
  ChartArtifact,
  Citation,
  TableArtifact,
} from "@/lib/server/dashboard-summary";

export interface UiMessage {
  role: "user" | "assistant";
  content: string;
}

export interface ChatApiResponse {
  answer: string;
  citations: Citation[];
  artifacts: {
    charts: ChartArtifact[];
    tables: TableArtifact[];
  };
  route: {
    tool: string;
    reason: string;
  };
  timestamp: string;
  meta?: {
    usedModel: boolean;
    model?: string;
    modelError?: string;
  };
}
