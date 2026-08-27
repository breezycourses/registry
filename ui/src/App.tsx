import { useCallback, useEffect, useMemo, useState } from "react";
import {
  api,
  fmtBytes,
  fmtDate,
  type GcReport,
  type Repo,
  type Stats,
  type Tag,
  type Whoami,
} from "./api";
import { Badge, Button, Card, Input, Skeleton } from "./components";

// --- tiny history-API router ------------------------------------------------

function usePath(): [string, (to: string) => void] {
  const [path, setPath] = useState(location.pathname);
  useEffect(() => {
    const onPop = () => setPath(location.pathname);
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);
  const navigate = useCallback((to: string) => {
    history.pushState(null, "", to);
    setPath(to);
  }, []);
  return [path, navigate];
}

// --- app shell ---------------------------------------------------------------

export default function App() {
  const [path, navigate] = usePath();
  const [who, setWho] = useState<Whoami | null>(null);

  useEffect(() => {
    api.whoami().then(setWho).catch(() => setWho(null));
  }, []);

  const repoName = path.startsWith("/repos/") ? decodeURIComponent(path.slice("/repos/".length)) : null;

  return (
    <div className="flex min-h-svh bg-background">
      <Sidebar path={path} navigate={navigate} who={who} />
      <div className="flex min-w-0 flex-1 flex-col">
        <MobileBar />
        <main className="mx-auto w-full max-w-4xl flex-1 px-4 py-8 sm:px-6 lg:px-10">
          {repoName ? (
            <RepoDetail name={repoName} isAdmin={who?.role === "admin"} navigate={navigate} />
          ) : path === "/repos" ? (
            <ReposPage navigate={navigate} />
          ) : (
            <OverviewPage isAdmin={who?.role === "admin"} />
          )}
        </main>
      </div>
    </div>
  );
}

function Logo() {
  return (
    <span className="flex items-center gap-2">
      <img src="/breezy-ghost-black.svg" alt="Breezy" className="h-[20px] dark:hidden" />
      <img src="/breezy-ghost-white.svg" alt="Breezy" className="hidden h-[20px] dark:block" />
      <img src="/breezy-text-black.svg" alt="breezy" className="h-3.5 dark:hidden" />
      <img src="/breezy-text-white.svg" alt="breezy" className="hidden h-3.5 dark:block" />
      <span className="mt-0.5 text-sm font-medium text-muted-foreground">registry</span>
    </span>
  );
}

function Sidebar({
  path,
  navigate,
  who,
}: {
  path: string;
  navigate: (to: string) => void;
  who: Whoami | null;
}) {
  const items: [string, string][] = [
    ["/", "Overview"],
    ["/repos", "Repositories"],
  ];
  const active = (to: string) =>
    to === "/" ? path === "/" : path === to || path.startsWith(to + "/");

  return (
    <aside className="sticky top-0 hidden h-svh w-60 flex-none flex-col border-r border-sidebar-border bg-sidebar text-sidebar-foreground md:flex">
      <div className="flex h-16 items-center px-5">
        <a
          href="/"
          onClick={(e) => {
            e.preventDefault();
            navigate("/");
          }}
          className="cursor-pointer outline-none"
        >
          <Logo />
        </a>
      </div>
      <nav className="flex flex-1 flex-col gap-1 px-3 pt-2">
        {items.map(([to, label]) => (
          <a
            key={to}
            href={to}
            onClick={(e) => {
              e.preventDefault();
              navigate(to);
            }}
            className={`cursor-pointer rounded-md px-3 py-2 text-sm font-medium transition-colors ${
              active(to)
                ? "bg-sidebar-accent text-sidebar-accent-foreground"
                : "text-muted-foreground hover:bg-sidebar-accent/50"
            }`}
          >
            {label}
          </a>
        ))}
      </nav>
      <div className="flex items-center justify-between border-t border-sidebar-border px-5 py-4">
        <span className="truncate text-sm text-muted-foreground">
          {who ? (
            <>
              {who.username ?? "anonymous"} · <span className="text-sidebar-foreground">{who.role}</span>
            </>
          ) : (
            "…"
          )}
        </span>
        <ThemeToggle />
      </div>
    </aside>
  );
}

function MobileBar() {
  return (
    <header className="sticky top-0 z-40 flex h-14 items-center justify-between border-b border-border bg-background/95 px-4 backdrop-blur-sm md:hidden">
      <Logo />
      <ThemeToggle />
    </header>
  );
}

function ThemeToggle() {
  const [dark, setDark] = useState(() => document.documentElement.classList.contains("dark"));
  const toggle = () => {
    const next = !dark;
    setDark(next);
    document.documentElement.classList.toggle("dark", next);
    try {
      localStorage.setItem("breezy-theme", next ? "dark" : "light");
    } catch {}
  };
  return (
    <Button variant="ghost" size="sm" onClick={toggle} aria-label="Toggle theme">
      {dark ? "☾" : "☀"}
    </Button>
  );
}

// --- pages -------------------------------------------------------------------

function OverviewPage({ isAdmin }: { isAdmin: boolean }) {
  const [stats, setStats] = useState<Stats | null>(null);
  const load = useCallback(() => {
    api.stats().then(setStats).catch(() => {});
  }, []);
  useEffect(load, [load]);

  return (
    <div className="flex flex-col gap-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Overview</h1>
        <p className="text-sm text-muted-foreground">Registry health at a glance.</p>
      </div>
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
        {stats
          ? (
              [
                ["Repositories", String(stats.repos)],
                ["Tags", String(stats.tags)],
                ["Blobs", String(stats.blobs)],
                ["Storage", fmtBytes(stats.storageBytes)],
                ["Mode", stats.mode],
              ] as [string, string][]
            ).map(([label, value]) => (
              <Card key={label} className="flex flex-col gap-1 p-4">
                <span className="text-xs font-medium tracking-wide text-muted-foreground uppercase">
                  {label}
                </span>
                <span className="text-xl font-semibold tabular-nums">{value}</span>
              </Card>
            ))
          : [1, 2, 3, 4, 5].map((i) => <Skeleton key={i} className="h-[74px] rounded-xl" />)}
      </div>
      {isAdmin && <GcCard onDone={load} />}
    </div>
  );
}

function ReposPage({ navigate }: { navigate: (to: string) => void }) {
  const [repos, setRepos] = useState<Repo[] | null>(null);
  const [filter, setFilter] = useState("");

  useEffect(() => {
    api.repos().then((r) => setRepos(r.repos)).catch(() => setRepos([]));
  }, []);

  const filtered = useMemo(
    () => (repos ?? []).filter((r) => r.name.toLowerCase().includes(filter.trim().toLowerCase())),
    [repos, filter],
  );

  return (
    <div className="flex flex-col gap-6">
      <div className="flex items-end justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Repositories</h1>
          <p className="text-sm text-muted-foreground">
            {repos ? `${repos.length} repositories` : "Loading…"}
          </p>
        </div>
        <div className="w-64">
          <Input placeholder="Filter…" value={filter} onChange={(e) => setFilter(e.target.value)} />
        </div>
      </div>

      {repos === null ? (
        <Skeleton className="h-48 w-full rounded-xl" />
      ) : filtered.length === 0 ? (
        <Card className="flex flex-col items-center gap-2 p-10 text-center">
          <p className="text-sm text-muted-foreground">
            {filter ? "No repositories match." : "No repositories yet. Push something:"}
          </p>
          {!filter && (
            <code className="rounded-md bg-muted px-3 py-1.5 font-mono text-sm">
              docker push {location.host}/team/app:v1
            </code>
          )}
        </Card>
      ) : (
        <Card>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-left text-xs tracking-wide text-muted-foreground uppercase">
                  <th className="px-5 py-3 font-medium">Name</th>
                  <th className="px-5 py-3 font-medium">Tags</th>
                  <th className="px-5 py-3 font-medium">Size</th>
                  <th className="px-5 py-3 font-medium">Last push</th>
                  <th className="px-5 py-3" />
                </tr>
              </thead>
              <tbody>
                {filtered.map((r) => (
                  <tr
                    key={r.name}
                    onClick={() => navigate(`/repos/${encodeURIComponent(r.name)}`)}
                    className="cursor-pointer border-b border-border/60 transition-colors last:border-0 hover:bg-accent/50"
                  >
                    <td className="px-5 py-3.5 font-mono font-semibold">{r.name}</td>
                    <td className="px-5 py-3.5 tabular-nums">{r.tags}</td>
                    <td className="px-5 py-3.5 tabular-nums">{fmtBytes(r.sizeBytes)}</td>
                    <td className="px-5 py-3.5 text-muted-foreground">
                      {r.lastPushed ? fmtDate(r.lastPushed) : "—"}
                    </td>
                    <td className="px-5 py-3.5 text-right text-muted-foreground">›</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )}
    </div>
  );
}

function RepoDetail({
  name,
  isAdmin,
  navigate,
}: {
  name: string;
  isAdmin?: boolean;
  navigate: (to: string) => void;
}) {
  const [tags, setTags] = useState<Tag[] | null>(null);

  const load = useCallback(() => {
    api.tags(name).then((d) => setTags(d.tags)).catch(() => setTags([]));
  }, [name]);
  useEffect(load, [load]);

  const removeTag = async (tag: string) => {
    if (!confirm(`Untag ${name}:${tag}? The manifest is reclaimed by the next GC.`)) return;
    await api.deleteTag(name, tag);
    load();
  };

  return (
    <div className="flex flex-col gap-6">
      <div>
        <a
          href="/repos"
          onClick={(e) => {
            e.preventDefault();
            navigate("/repos");
          }}
          className="cursor-pointer text-sm text-muted-foreground hover:text-foreground"
        >
          ← Repositories
        </a>
        <div className="mt-2 flex flex-wrap items-center justify-between gap-3">
          <h1 className="font-mono text-2xl font-bold tracking-tight">{name}</h1>
          <CopyButton text={`docker pull ${location.host}/${name}`} label="Copy pull command" />
        </div>
        <p className="text-sm text-muted-foreground">
          {tags ? `${tags.length} tag${tags.length === 1 ? "" : "s"}` : "Loading…"}
        </p>
      </div>

      {tags === null ? (
        <Skeleton className="h-48 w-full rounded-xl" />
      ) : tags.length === 0 ? (
        <Card className="p-10 text-center text-sm text-muted-foreground">No tags.</Card>
      ) : (
        <Card>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-left text-xs tracking-wide text-muted-foreground uppercase">
                  <th className="px-5 py-3 font-medium">Tag</th>
                  <th className="px-5 py-3 font-medium">Digest</th>
                  <th className="px-5 py-3 font-medium">Size</th>
                  <th className="px-5 py-3 font-medium">Pushed</th>
                  <th className="px-5 py-3" />
                </tr>
              </thead>
              <tbody>
                {tags.map((t) => (
                  <tr key={t.name} className="border-b border-border/60 last:border-0">
                    <td className="px-5 py-3.5">
                      <span className="flex items-center gap-2 font-mono font-semibold">
                        {t.name}
                        {t.isIndex && <Badge tone="primary">multi-arch</Badge>}
                      </span>
                    </td>
                    <td className="px-5 py-3.5 font-mono text-muted-foreground" title={t.digest}>
                      {t.digest.slice(7, 19)}
                    </td>
                    <td className="px-5 py-3.5 tabular-nums">{fmtBytes(t.size)}</td>
                    <td className="px-5 py-3.5 text-muted-foreground">{fmtDate(t.pushedAt)}</td>
                    <td className="px-5 py-3.5">
                      <span className="flex justify-end gap-1.5">
                        <CopyButton text={`docker pull ${location.host}/${name}:${t.name}`} label="Copy pull" />
                        {isAdmin && (
                          <Button variant="destructive" size="sm" onClick={() => removeTag(t.name)}>
                            Untag
                          </Button>
                        )}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )}
    </div>
  );
}

function CopyButton({ text, label }: { text: string; label: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <Button
      variant="outline"
      size="sm"
      onClick={() => {
        navigator.clipboard.writeText(text);
        setCopied(true);
        setTimeout(() => setCopied(false), 1200);
      }}
    >
      {copied ? "Copied" : label}
    </Button>
  );
}

function GcCard({ onDone }: { onDone: () => void }) {
  const [busy, setBusy] = useState(false);
  const [report, setReport] = useState<GcReport | null>(null);

  const run = async (dry: boolean) => {
    setBusy(true);
    try {
      const r = await api.gc(dry);
      setReport(r);
      if (!dry) onDone();
    } finally {
      setBusy(false);
    }
  };

  return (
    <Card className="flex flex-col gap-3 p-5">
      <div>
        <h2 className="text-lg font-semibold">Garbage collection</h2>
        <p className="text-sm text-muted-foreground">
          Reclaims untagged manifests and unreferenced blobs. Dry-run first to preview.
        </p>
      </div>
      <div className="flex items-center gap-2">
        <Button variant="outline" size="sm" disabled={busy} onClick={() => run(true)}>
          Dry run
        </Button>
        <Button size="sm" disabled={busy} onClick={() => run(false)}>
          Run GC
        </Button>
        {report && (
          <span className="text-sm text-muted-foreground">
            {report.dry_run ? "would delete" : "deleted"} {report.manifests_deleted} manifests,{" "}
            {report.blobs_deleted} blobs ({fmtBytes(report.bytes_freed)})
          </span>
        )}
      </div>
    </Card>
  );
}
