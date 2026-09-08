"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useState } from "react";
import { cn } from "@/lib/utils";

const links = [
  { href: "/", label: "Overview" },
  { href: "/query", label: "Query explorer" },
  { href: "/services", label: "Service map" },
  { href: "/flamegraph", label: "Flamegraph" },
  { href: "/logs", label: "Log explorer" },
  { href: "/health", label: "System health" },
];

export function Nav() {
  const pathname = usePathname();
  const [dark, setDark] = useState(true);

  useEffect(() => {
    setDark(localStorage.getItem("theme") !== "light");
  }, []);

  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
    try {
      localStorage.setItem("theme", dark ? "dark" : "light");
    } catch {
      // ignore -- a private-browsing tab that blocks storage still works, it
      // just re-flashes to the default theme on the next load
    }
  }, [dark]);

  return (
    <aside className="flex h-screen w-56 shrink-0 flex-col border-r bg-card">
      <div className="border-b p-4">
        <Link href="/" className="text-lg font-bold tracking-tight">
          TraceLens
        </Link>
        <p className="text-xs text-muted-foreground">observability, hand-built</p>
      </div>
      <nav className="flex-1 space-y-0.5 p-2">
        {links.map((l) => (
          <Link
            key={l.href}
            href={l.href}
            className={cn(
              "block rounded-md px-3 py-2 text-sm font-medium",
              pathname === l.href ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:bg-muted",
            )}
          >
            {l.label}
          </Link>
        ))}
      </nav>
      <div className="border-t p-2">
        <button
          type="button"
          onClick={() => setDark((d) => !d)}
          className="w-full rounded-md px-3 py-2 text-left text-sm text-muted-foreground hover:bg-muted"
        >
          {dark ? "Switch to light" : "Switch to dark"}
        </button>
      </div>
    </aside>
  );
}
