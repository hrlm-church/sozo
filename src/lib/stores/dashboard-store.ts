import { create } from "zustand";
import type { Widget, WidgetLayout } from "@/types/widget";

interface DashboardState {
  /** Dashboard name */
  name: string;
  /** Dashboard ID (set after save/load) */
  dashboardId: string | null;
  /** Pinned widgets */
  widgets: Widget[];
  /** Grid layout positions */
  layouts: WidgetLayout[];
  /** Whether the dashboard has unsaved changes */
  dirty: boolean;

  setName: (name: string) => void;
  addWidget: (widget: Widget) => void;
  removeWidget: (widgetId: string) => void;
  updateLayout: (layouts: WidgetLayout[]) => void;
  clearDashboard: () => void;
  loadDashboard: (data: {
    id: string;
    name: string;
    widgets: Widget[];
    layouts: WidgetLayout[];
  }) => void;
  markSaved: (id: string) => void;
}

/** Default layout position for a new widget */
function nextLayout(existing: WidgetLayout[], widgetId: string, widgetType: string): WidgetLayout {
  const maxY = existing.reduce((max, l) => Math.max(max, l.y + l.h), 0);

  // Widget type → default size
  const sizes: Record<string, [number, number]> = {
    kpi: [3, 3],
    bar_chart: [6, 5],
    line_chart: [6, 5],
    area_chart: [6, 5],
    donut_chart: [4, 5],
    table: [12, 6],
    funnel: [6, 5],
    stat_grid: [12, 3],
    text: [6, 4],
  };

  const [w, h] = sizes[widgetType] ?? [6, 4];

  return { i: widgetId, x: 0, y: maxY, w, h, minW: 2, minH: 2 };
}

export const useDashboardStore = create<DashboardState>((set, get) => ({
  name: "Untitled Dashboard",
  dashboardId: null,
  widgets: [],
  layouts: [],
  dirty: false,

  setName: (name) => set({ name, dirty: true }),

  addWidget: (widget) => {
    const state = get();
    if (state.widgets.find((w) => w.id === widget.id)) return;
    const layout = nextLayout(state.layouts, widget.id, widget.type);
    set({
      widgets: [...state.widgets, widget],
      layouts: [...state.layouts, layout],
      dirty: true,
    });
  },

  removeWidget: (widgetId) =>
    set((s) => ({
      widgets: s.widgets.filter((w) => w.id !== widgetId),
      layouts: s.layouts.filter((l) => l.i !== widgetId),
      dirty: true,
    })),

  updateLayout: (layouts) => set({ layouts, dirty: true }),

  clearDashboard: () =>
    set({
      name: "Untitled Dashboard",
      dashboardId: null,
      widgets: [],
      layouts: [],
      dirty: false,
    }),

  loadDashboard: (data) =>
    set({
      dashboardId: data.id,
      name: data.name,
      widgets: data.widgets,
      layouts: data.layouts,
      dirty: false,
    }),

  markSaved: (id) => set({ dashboardId: id, dirty: false }),
}));
