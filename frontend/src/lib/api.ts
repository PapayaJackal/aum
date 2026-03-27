import { getToken, getRefreshToken, setAuth, clearAuth } from "./auth";

const BASE = "/api";

let _refreshing: Promise<boolean> | null = null;

/** Try to refresh the access token using the stored refresh token.
 *  Returns true on success.  Concurrent callers share the same request. */
async function _tryRefresh(): Promise<boolean> {
  const rt = getRefreshToken();
  if (!rt) return false;

  try {
    const res = await fetch(`${BASE}/auth/refresh`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ refresh_token: rt }),
    });
    if (!res.ok) return false;
    const data: { access_token: string; refresh_token: string } = await res.json();
    setAuth(data.access_token, data.refresh_token);
    return true;
  } catch {
    return false;
  }
}

/** Fetch with auth header and transparent token refresh on 401. */
async function _authFetch(url: string, options: RequestInit = {}): Promise<Response> {
  const headers: Record<string, string> = {
    ...(options.headers as Record<string, string>),
  };
  const token = getToken();
  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }

  let res = await fetch(url, { ...options, headers });

  if (res.status === 401) {
    if (!_refreshing) _refreshing = _tryRefresh().finally(() => (_refreshing = null));
    const ok = await _refreshing;
    if (ok) {
      headers["Authorization"] = `Bearer ${getToken()}`;
      res = await fetch(url, { ...options, headers });
    }
    if (res.status === 401) {
      clearAuth();
      window.location.hash = "#/login";
      throw new Error("Unauthorized");
    }
  }

  return res;
}

async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  const res = await _authFetch(`${BASE}${path}`, {
    ...options,
    headers: { "Content-Type": "application/json", ...(options.headers as Record<string, string>) },
  });

  if (!res.ok) {
    const body = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(body.detail || res.statusText);
  }

  return res.json();
}

// Auth

export interface TokenResponse {
  access_token: string;
  refresh_token: string;
  token_type: string;
}

export function login(username: string, password: string): Promise<TokenResponse> {
  return request("/auth/login", {
    method: "POST",
    body: JSON.stringify({ username, password }),
  });
}

export function refreshToken(refresh_token: string): Promise<TokenResponse> {
  return request("/auth/refresh", {
    method: "POST",
    body: JSON.stringify({ refresh_token }),
  });
}

export function getProviders(): Promise<{ providers: string[] }> {
  return request("/auth/providers");
}

// Indices

export interface IndexInfo {
  name: string;
  has_embeddings: boolean;
}

export function listIndices(): Promise<{ indices: IndexInfo[] }> {
  return request("/indices");
}

// Search

export interface SearchResult {
  doc_id: string;
  display_path: string;
  display_path_highlighted: string;
  score: number;
  snippet: string;
  metadata: Record<string, string | string[]>;
  index: string;
}

export interface SearchResponse {
  results: SearchResult[];
  total: number;
  facets: Record<string, string[]> | null;
}

export function search(
  query: string,
  type: string = "text",
  limit: number = 20,
  index: string = "",
  offset: number = 0,
  filters: Record<string, string[]> = {},
): Promise<SearchResponse> {
  const params = new URLSearchParams({ q: query, type, limit: String(limit), offset: String(offset) });
  if (index) params.set("index", index);
  if (Object.keys(filters).length > 0) {
    params.set("filters", JSON.stringify(filters));
  }
  return request(`/search?${params}`);
}

// Documents

export interface AttachmentRef {
  doc_id: string;
  display_path: string;
}

export interface DocumentDetail {
  doc_id: string;
  display_path: string;
  content: string;
  metadata: Record<string, string | string[]>;
  attachments: AttachmentRef[];
  extracted_from: AttachmentRef | null;
}

export function getDocument(docId: string, index: string = ""): Promise<DocumentDetail> {
  const params = index ? `?index=${encodeURIComponent(index)}` : "";
  return request(`/documents/${encodeURIComponent(docId)}${params}`);
}

export async function downloadDocument(docId: string, index: string = ""): Promise<void> {
  const params = index ? `?index=${encodeURIComponent(index)}` : "";
  const res = await _authFetch(`${BASE}/documents/${encodeURIComponent(docId)}/download${params}`);

  if (!res.ok) {
    const body = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(body.detail || res.statusText);
  }

  const blob = await res.blob();
  const disposition = res.headers.get("Content-Disposition") || "";

  // Prefer RFC 5987 filename* (percent-encoded UTF-8), fall back to plain filename
  let filename = "download";
  const starMatch = disposition.match(/filename\*=(?:UTF-8|utf-8)''(.+?)(?:;|$)/);
  if (starMatch) {
    filename = decodeURIComponent(starMatch[1]);
  } else {
    const plainMatch = disposition.match(/filename="(.+?)"/);
    if (plainMatch) {
      filename = plainMatch[1];
    }
  }

  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

// Jobs

export interface Job {
  job_id: string;
  source_dir: string;
  status: string;
  total_files: number;
  processed: number;
  failed: number;
  created_at: string;
  finished_at: string | null;
}

export interface JobDetail extends Job {
  errors: {
    file_path: string;
    error_type: string;
    message: string;
    timestamp: string;
  }[];
}

export function listJobs(): Promise<Job[]> {
  return request("/jobs");
}

export function getJob(jobId: string): Promise<JobDetail> {
  return request(`/jobs/${encodeURIComponent(jobId)}`);
}
