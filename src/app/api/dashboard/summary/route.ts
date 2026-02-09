import { NextResponse } from "next/server";
import { getDashboardSummary } from "@/lib/server/dashboard-summary";

export const dynamic = "force-dynamic";

export async function GET() {
  return NextResponse.json(await getDashboardSummary());
}
