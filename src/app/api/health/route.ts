import { NextResponse } from "next/server";
import { getSystemHealth } from "@/lib/server/azure-health";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const payload = await getSystemHealth();
    const status = payload.ok ? 200 : 503;
    return NextResponse.json(payload, { status });
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        timestamp: new Date().toISOString(),
        error:
          error instanceof Error ? error.message : "Unexpected health check error",
      },
      { status: 500 },
    );
  }
}
