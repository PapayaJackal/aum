import type { SearchResult } from "./api";

const PREF_KEY = "aum_prefs";

interface Prefs {
  pageSize: number;
  selectedIndex: string;
  searchType: "text" | "hybrid";
  indexSearchTypes: Record<string, "text" | "hybrid">;
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
      return { pageSize: parsed.pageSize ?? 20, selectedIndex: parsed.selectedIndex ?? "", searchType, indexSearchTypes };
    }
  } catch {}
  return { pageSize: 20, selectedIndex: "", searchType: "hybrid", indexSearchTypes: {} };
}

export function savePrefs() {
  try {
    localStorage.setItem(
      PREF_KEY,
      JSON.stringify({
        pageSize: searchState.pageSize,
        selectedIndex: searchState.selectedIndex,
        searchType: searchState.searchType,
        indexSearchTypes: searchState.indexSearchTypes,
      })
    );
  } catch {}
}

export function saveIndexSearchType(index: string, type: "text" | "hybrid") {
  searchState.indexSearchTypes[index] = type;
  savePrefs();
}

export function getIndexSearchType(index: string): "text" | "hybrid" | undefined {
  return searchState.indexSearchTypes[index];
}

const _prefs = loadPrefs();

export const searchState = $state<{
  query: string;
  searchType: "text" | "hybrid";
  selectedIndex: string;
  results: SearchResult[];
  total: number;
  searched: boolean;
  activeFacets: Record<string, string[]>;
  facets: Record<string, string[]>;
  pageSize: number;
  currentPage: number;
  selectedDocId: string;
  selectedDocIndex: string;
  indexSearchTypes: Record<string, "text" | "hybrid">;
}>({
  query: "",
  searchType: _prefs.searchType,
  selectedIndex: _prefs.selectedIndex,
  results: [] as SearchResult[],
  total: 0,
  searched: false,
  activeFacets: {} as Record<string, string[]>,
  facets: {} as Record<string, string[]>,
  pageSize: _prefs.pageSize,
  currentPage: 1,
  selectedDocId: "",
  selectedDocIndex: "",
  indexSearchTypes: _prefs.indexSearchTypes,
});

export function getSearchQs(): string {
  const params = new URLSearchParams();
  if (searchState.query) params.set("q", searchState.query);
  params.set("type", searchState.searchType);
  if (searchState.selectedIndex) params.set("index", searchState.selectedIndex);
  if (searchState.currentPage > 1) params.set("page", String(searchState.currentPage));
  if (searchState.pageSize !== 20) params.set("pageSize", String(searchState.pageSize));
  const af = searchState.activeFacets;
  if (Object.keys(af).length > 0) params.set("facets", JSON.stringify(af));
  if (searchState.selectedDocId) {
    params.set("doc", searchState.selectedDocId);
    if (searchState.selectedDocIndex) params.set("docIndex", searchState.selectedDocIndex);
  }
  return params.toString();
}
