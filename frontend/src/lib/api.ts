import { getToken, clearAuth } from "./auth";

const BASE = "/api";

async function request<T>(
  path: string,
  options: RequestInit = {},
): Promise<T> {
  const token = getToken();
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(options.headers as Record<string, string>),
  };
  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }

  const res = await fetch(`${BASE}${path}`, { ...options, headers });

  if (res.status === 401) {
    clearAuth();
    window.location.hash = "#/login";
    throw new Error("Unauthorized");
  }

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

export function login(
  username: string,
  password: string,
): Promise<TokenResponse> {
  return request("/auth/login", {
    method: "POST",
    body: JSON.stringify({ username, password }),
  });
}

export function refreshToken(
  refresh_token: string,
): Promise<TokenResponse> {
  return request("/auth/refresh", {
    method: "POST",
    body: JSON.stringify({ refresh_token }),
  });
}

export function getProviders(): Promise<{ providers: string[] }> {
  return request("/auth/providers");
}

// Indices

export function listIndices(): Promise<{ indices: string[] }> {
  return request("/indices");
}

// Search

export interface SearchResult {
  doc_id: string;
  display_path: string;
  score: number;
  snippet: string;
  metadata: Record<string, string>;
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
): Promise<SearchResponse> {
  const params = new URLSearchParams({ q: query, type, limit: String(limit), offset: String(offset) });
  if (index) params.set("index", index);
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
  metadata: Record<string, string>;
  attachments: AttachmentRef[];
  extracted_from: AttachmentRef | null;
}

export function getDocument(docId: string, index: string = ""): Promise<DocumentDetail> {
  const params = index ? `?index=${encodeURIComponent(index)}` : "";
  return request(`/documents/${encodeURIComponent(docId)}${params}`);
}

export function downloadUrl(docId: string, index: string = ""): string {
  const params = index ? `?index=${encodeURIComponent(index)}` : "";
  return `/api/documents/${encodeURIComponent(docId)}/download${params}`;
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
