export interface Stats {
  repos: number;
  tags: number;
  manifests: number;
  blobs: number;
  storageBytes: number;
  mode: string;
}

export interface Repo {
  name: string;
  tags: number;
  sizeBytes: number;
  lastPushed: number | null;
}

export interface Tag {
  name: string;
  digest: string;
  mediaType: string;
  size: number;
  pushedAt: number;
  isIndex: boolean;
}

export interface Whoami {
  username: string | null;
  role: string;
}

export interface GcReport {
  dry_run: boolean;
  manifests_deleted: number;
  blobs_deleted: number;
  bytes_freed: number;
}

async function json<T>(url: string, init?: RequestInit): Promise<T> {
  const r = await fetch(url, init);
  if (!r.ok) throw new Error(`${url}: ${r.status}`);
  return r.json();
}

export const api = {
  whoami: () => json<Whoami>("/api/v1/whoami"),
  stats: () => json<Stats>("/api/v1/stats"),
  repos: () => json<{ repos: Repo[] }>("/api/v1/repos"),
  tags: (repo: string) =>
    json<{ repo: string; tags: Tag[] }>(`/api/v1/tags?repo=${encodeURIComponent(repo)}`),
  gc: (dryRun: boolean) =>
    json<GcReport>(`/api/v1/gc?dry_run=${dryRun ? "1" : "0"}`, { method: "POST" }),
  deleteTag: async (repo: string, tag: string) => {
    const r = await fetch(`/v2/${repo}/manifests/${tag}`, { method: "DELETE" });
    if (!r.ok) throw new Error(`delete failed: ${r.status}`);
  },
};

export function fmtBytes(n: number): string {
  if (n == null) return "—";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  let v = n;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(v >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
}

export function fmtDate(secs: number): string {
  if (!secs) return "—";
  return new Date(secs * 1000).toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}
