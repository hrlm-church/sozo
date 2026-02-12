"use client";

import { useMemo, useCallback } from "react";
import { GridLayout, useContainerWidth } from "react-grid-layout";
import type { Layout } from "react-grid-layout";
import { useDashboardStore } from "@/lib/stores/dashboard-store";
import { WidgetRenderer } from "@/components/widgets/WidgetRenderer";
import type { WidgetLayout } from "@/types/widget";

export function DashboardCanvas() {
  const widgets = useDashboardStore((s) => s.widgets);
  const layouts = useDashboardStore((s) => s.layouts);
  const removeWidget = useDashboardStore((s) => s.removeWidget);
  const updateLayout = useDashboardStore((s) => s.updateLayout);
  const { width, containerRef } = useContainerWidth();

  const widgetMap = useMemo(() => {
    const map = new Map<string, (typeof widgets)[number]>();
    for (const w of widgets) map.set(w.id, w);
    return map;
  }, [widgets]);

  const onLayoutChange = useCallback(
    (newLayout: Layout) => {
      const mapped: WidgetLayout[] = newLayout.map((l) => ({
        i: l.i, x: l.x, y: l.y, w: l.w, h: l.h, minW: l.minW, minH: l.minH,
      }));
      updateLayout(mapped);
    },
    [updateLayout],
  );

  if (widgets.length === 0) {
    return (
      <div
        ref={containerRef}
        style={{
          display: "flex", alignItems: "center", justifyContent: "center",
          height: "100%", color: "var(--text-muted)", fontSize: "0.88rem",
        }}
      >
        <div style={{ textAlign: "center" }}>
          <div style={{ fontSize: "2rem", marginBottom: 12, opacity: 0.25 }}>+</div>
          <p style={{ margin: 0, fontWeight: 500, color: "var(--text-secondary)" }}>
            Pin widgets from chat to build your dashboard
          </p>
          <p style={{ margin: "4px 0 0", fontSize: "0.78rem" }}>
            Ask Sozo a question to get started
          </p>
        </div>
      </div>
    );
  }

  return (
    <div ref={containerRef}>
      {width > 0 && (
        <GridLayout
          layout={layouts as unknown as Layout}
          width={width}
          gridConfig={{ cols: 12, rowHeight: 60, margin: [16, 16] as const }}
          dragConfig={{ handle: ".widget-drag-handle" }}
          onLayoutChange={onLayoutChange}
          autoSize
        >
          {layouts.map((l) => {
            const widget = widgetMap.get(l.i);
            if (!widget) return null;
            return (
              <div key={l.i} className="widget-drag-handle" style={{ cursor: "grab" }}>
                <WidgetRenderer widget={widget} onRemove={() => removeWidget(widget.id)} isPinned />
              </div>
            );
          })}
        </GridLayout>
      )}
    </div>
  );
}
