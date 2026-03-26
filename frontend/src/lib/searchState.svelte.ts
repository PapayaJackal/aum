import type { SearchResult } from "./api";

const PREF_KEY = "aum_prefs";

function loadPrefs(): { pageSize: number; selectedIndex: string; searchType: string } {
  try {
    const raw = localStorage.getItem(PREF_KEY);
    if (raw) return { pageSize: 20, selectedIndex: "", searchType: "text", ...JSON.parse(raw) };
  } catch {}
  return { pageSize: 20, selectedIndex: "", searchType: "text" };
}

export function savePrefs() {
  try {
    localStorage.setItem(
      PREF_KEY,
      JSON.stringify({ pageSize: searchState.pageSize, selectedIndex: searchState.selectedIndex, searchType: searchState.searchType })
    );
  } catch {}
}

const _prefs = loadPrefs();

export const searchState = $state({
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
