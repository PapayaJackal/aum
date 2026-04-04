import { getToken, setAuth, clearAuth, isPublicMode } from "./auth";

const BASE = "/api";

/** Fetch with auth header; redirect to login on 401. */
async function _authFetch(url: string, options: RequestInit = {}): Promise<Response> {
  const headers: Record<string, string> = {
    ...(options.headers as Record<string, string>),
  };
  const token = getToken();
  if (token && !isPublicMode()) {
    headers["Authorization"] = `Bearer ${token}`;
  }

  const res = await fetch(url, { ...options, headers });

  if (res.status === 401 && !isPublicMode()) {
    clearAuth();
    window.location.hash = "#/login";
    throw new Error("Unauthorized");
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

export interface SessionTokenResponse {
  session_token: string;
  token_type: string;
}

export function login(username: string, password: string): Promise<SessionTokenResponse> {
  return request("/auth/login", {
    method: "POST",
    body: JSON.stringify({ username, password }),
  });
}

export interface ProvidersResponse {
  providers: string[];
  public_mode: boolean;
}

export function getProviders(): Promise<ProvidersResponse> {
  return request("/auth/providers");
}

// Invitations

export interface InviteValidation {
  username: string;
  valid: boolean;
}

export function validateInvite(token: string): Promise<InviteValidation> {
  return request(`/auth/invite/${encodeURIComponent(token)}`);
}

export function redeemInvite(token: string, password: string): Promise<SessionTokenResponse> {
  return request(`/auth/invite/${encodeURIComponent(token)}/redeem`, {
    method: "POST",
    body: JSON.stringify({ password }),
  });
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
  semanticRatio?: number,
  sort?: string,
): Promise<SearchResponse> {
  const params = new URLSearchParams({ q: query, type, limit: String(limit), offset: String(offset) });
  if (index) params.set("index", index);
  if (Object.keys(filters).length > 0) {
    params.set("filters", JSON.stringify(filters));
  }
  if (semanticRatio != null) {
    params.set("semantic_ratio", String(semanticRatio));
  }
  if (sort) {
    params.set("sort", sort);
  }
  return request(`/search?${params}`);
}

// Documents

export interface AttachmentRef {
  doc_id: string;
  display_path: string;
}

export interface ThreadMessage {
  doc_id: string;
  display_path: string;
  subject: string;
  sender: string;
  date: string;
  snippet: string;
}

export interface DocumentDetail {
  doc_id: string;
  display_path: string;
  content: string;
  metadata: Record<string, string | string[]>;
  attachments: AttachmentRef[];
  extracted_from: AttachmentRef | null;
  thread: ThreadMessage[];
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

// Preview

const PREVIEWABLE_TYPES = new Set([
  "image/jpeg",
  "image/png",
  "image/gif",
  "image/webp",
  "image/bmp",
  "application/pdf",
  "message/rfc822",
  "text/html",
]);

export function isPreviewable(metadata: Record<string, string | string[]>): boolean {
  const ct = metadata["content_type"];
  const contentType = (Array.isArray(ct) ? ct[0] : ct)?.split(";")[0]?.trim().toLowerCase() ?? "";
  return PREVIEWABLE_TYPES.has(contentType);
}

export function getContentType(metadata: Record<string, string | string[]>): string {
  const ct = metadata["content_type"];
  return (Array.isArray(ct) ? ct[0] : ct)?.split(";")[0]?.trim().toLowerCase() ?? "";
}

export async function fetchPreviewBlob(docId: string, index: string = ""): Promise<Blob> {
  const params = index ? `?index=${encodeURIComponent(index)}` : "";
  const res = await _authFetch(`${BASE}/documents/${encodeURIComponent(docId)}/preview${params}`);

  if (!res.ok) {
    const body = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(body.detail || res.statusText);
  }

  return res.blob();
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
