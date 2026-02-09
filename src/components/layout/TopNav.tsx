import { NavItem } from "@/types/dashboard";
import { Chip } from "@/components/ui/Chip";
import { IconButton } from "@/components/ui/IconButton";
import { SearchInput } from "@/components/ui/SearchInput";

interface TopNavProps {
  navItems: NavItem[];
  activeNav: string;
  onNavChange: (id: string) => void;
  searchQuery: string;
  onSearchQueryChange: (value: string) => void;
}

export function TopNav({
  navItems,
  activeNav,
  onNavChange,
  searchQuery,
  onSearchQueryChange,
}: TopNavProps) {
  return (
    <header className="card-base sticky top-4 z-20 flex flex-wrap items-center gap-3 p-3 md:gap-4">
      <div className="inline-flex h-14 w-14 items-center justify-center rounded-full bg-[var(--surface)] text-sm font-semibold text-[var(--accent-purple)] shadow-[var(--shadow-soft)]">
        sozo
      </div>

      <nav className="order-3 flex w-full gap-2 overflow-x-auto pb-1 md:order-none md:w-auto md:flex-1 md:pb-0">
        {navItems.map((item) => (
          <Chip
            key={item.id}
            label={item.badge ? `${item.label} ${item.badge}` : item.label}
            active={item.id === activeNav}
            onClick={() => onNavChange(item.id)}
          />
        ))}
      </nav>

      <div className="order-2 min-w-[240px] flex-1 md:max-w-md">
        <SearchInput value={searchQuery} onChange={onSearchQueryChange} />
      </div>

      <div className="order-1 ml-auto flex items-center gap-2 md:order-none">
        <IconButton label="Theme" icon="☀️" active />
        <IconButton label="Night" icon="☾" />
        <IconButton label="Notifications" icon="🔔" />
        <button
          type="button"
          aria-label="Create"
          className="inline-flex size-12 items-center justify-center rounded-full bg-[var(--accent-gradient)] text-xl text-white shadow-[var(--shadow-pop)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--accent-purple)]"
        >
          +
        </button>
      </div>
    </header>
  );
}
