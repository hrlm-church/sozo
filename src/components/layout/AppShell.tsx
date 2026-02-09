import { ReactNode } from "react";
import { NavItem } from "@/types/dashboard";
import { TopNav } from "@/components/layout/TopNav";

interface AppShellProps {
  navItems: NavItem[];
  activeNav: string;
  onNavChange: (id: string) => void;
  searchQuery: string;
  onSearchQueryChange: (value: string) => void;
  children: ReactNode;
}

export function AppShell({
  navItems,
  activeNav,
  onNavChange,
  searchQuery,
  onSearchQueryChange,
  children,
}: AppShellProps) {
  return (
    <div className="min-h-screen bg-[var(--app-bg)] px-4 py-4 md:px-8 md:py-6">
      <div className="mx-auto w-full max-w-[1480px]">
        <TopNav
          navItems={navItems}
          activeNav={activeNav}
          onNavChange={onNavChange}
          searchQuery={searchQuery}
          onSearchQueryChange={onSearchQueryChange}
        />
        <main className="mt-6">{children}</main>
      </div>
    </div>
  );
}
