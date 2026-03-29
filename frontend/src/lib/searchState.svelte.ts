import type { SearchResult } from "./api";

const PREF_KEY = "aum_prefs";
const BASELINE_KEY = "aum_baseline_facets";

interface Prefs {
  pageSize: number;
  selectedIndices: string[];
  searchType: "text" | "hybrid";
  semanticRatio: number;
  indexSearchTypes: Record<string, "text" | "hybrid">;
  sortBy: string;
}

function loadPrefs(): Prefs {
  try {
    const raw = localStorage.getItem(PREF_KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      const searchType: "text" | "hybrid" = parsed.searchType === "hybrid" ? "hybrid" : "text";
      const indexSearchTypes: Record<string, "text" | "hybrid"> = {};
      for (const [k, v] of Object.entries(parsed.indexSearchTypes ?? {})) {
        if (v === "hybrid" || v === "text") indexSearchTypes[k] = v;
      }
      // Backward compat: migrate selectedIndex (string) to selectedIndices (array)
      let selectedIndices: string[] = [];
      if (Array.isArray(parsed.selectedIndices)) {
        selectedIndices = parsed.selectedIndices.filter((s: unknown) => typeof s === "string" && s);
      } else if (typeof parsed.selectedIndex === "string" && parsed.selectedIndex) {
        selectedIndices = [parsed.selectedIndex];
      }
      const semanticRatio = Math.max(0, Math.min(1, Number(parsed.semanticRatio ?? 0.5))) || 0.5;
      const sortBy = typeof parsed.sortBy === "string" ? parsed.sortBy : "relevance";
      return { pageSize: parsed.pageSize ?? 20, selectedIndices, searchType, semanticRatio, indexSearchTypes, sortBy };
    }
  } catch {}
  return {
    pageSize: 20,
    selectedIndices: [],
    searchType: "hybrid",
    semanticRatio: 0.5,
    indexSearchTypes: {},
    sortBy: "relevance",
  };
}

export function savePrefs() {
  try {
    localStorage.setItem(
      PREF_KEY,
      JSON.stringify({
        pageSize: searchState.pageSize,
        selectedIndices: searchState.selectedIndices,
        searchType: searchState.searchType,
        semanticRatio: searchState.semanticRatio,
        indexSearchTypes: searchState.indexSearchTypes,
        sortBy: searchState.sortBy,
      }),
    );
  } catch {}
}

/** Key for per-index search type: comma-joined sorted index names. */
function _indicesKey(indices: string[]): string {
  return [...indices].sort().join(",");
}

export function saveIndexSearchType(indices: string[], type: "text" | "hybrid") {
  searchState.indexSearchTypes[_indicesKey(indices)] = type;
  savePrefs();
}

export function getIndexSearchType(indices: string[]): "text" | "hybrid" | undefined {
  return searchState.indexSearchTypes[_indicesKey(indices)];
}

const _prefs = loadPrefs();

export const searchState = $state<{
  query: string;
  submittedQuery: string;
  searchType: "text" | "hybrid";
  semanticRatio: number;
  selectedIndices: string[];
  results: SearchResult[];
  total: number;
  searched: boolean;
  activeFacets: Record<string, string[]>;
  facets: Record<string, string[]>;
  baselineFacets: Record<string, string[]>;
  pageSize: number;
  currentPage: number;
  selectedDocId: string;
  selectedDocIndex: string;
  indexSearchTypes: Record<string, "text" | "hybrid">;
  sortBy: string;
}>({
  query: "",
  submittedQuery: "",
  searchType: _prefs.searchType,
  semanticRatio: _prefs.semanticRatio,
  selectedIndices: _prefs.selectedIndices,
  results: [] as SearchResult[],
  total: 0,
  searched: false,
  activeFacets: {} as Record<string, string[]>,
  facets: {} as Record<string, string[]>,
  baselineFacets: {} as Record<string, string[]>,
  pageSize: _prefs.pageSize,
  currentPage: 1,
  selectedDocId: "",
  selectedDocIndex: "",
  indexSearchTypes: _prefs.indexSearchTypes,
  sortBy: _prefs.sortBy,
});

export function getSearchQs(): string {
  const params = new URLSearchParams();
  if (searchState.submittedQuery) params.set("q", searchState.submittedQuery);
  params.set("type", searchState.searchType);
  if (searchState.searchType === "hybrid" && searchState.semanticRatio !== 0.5) {
    params.set("semanticRatio", String(searchState.semanticRatio));
  }
  if (searchState.selectedIndices.length > 0) params.set("index", searchState.selectedIndices.join(","));
  if (searchState.currentPage > 1) params.set("page", String(searchState.currentPage));
  if (searchState.pageSize !== 20) params.set("pageSize", String(searchState.pageSize));
  const af = searchState.activeFacets;
  if (Object.keys(af).length > 0) params.set("facets", JSON.stringify(af));
  if (searchState.sortBy && searchState.sortBy !== "relevance") params.set("sort", searchState.sortBy);
  if (searchState.selectedDocId) {
    params.set("doc", searchState.selectedDocId);
    if (searchState.selectedDocIndex) params.set("docIndex", searchState.selectedDocIndex);
  }
  return params.toString();
}

/** Persist baseline facets to sessionStorage, keyed by query + index. */
export function saveBaselineFacets(query: string, index: string) {
  try {
    sessionStorage.setItem(BASELINE_KEY, JSON.stringify({ query, index, facets: searchState.baselineFacets }));
  } catch {}
}

/** Restore baseline facets from sessionStorage if query + index match. */
export function restoreBaselineFacets(query: string, index: string): boolean {
  try {
    const raw = sessionStorage.getItem(BASELINE_KEY);
    if (!raw) return false;
    const cached = JSON.parse(raw);
    if (cached.query === query && cached.index === index) {
      searchState.baselineFacets = cached.facets;
      return true;
    }
  } catch {}
  return false;
}

export function clearBaselineFacets() {
  try {
    sessionStorage.removeItem(BASELINE_KEY);
  } catch {}
}
