import type { SearchResult } from "./api";

export const searchState = $state({
  query: "",
  searchType: "text",
  selectedIndex: "",
  results: [] as SearchResult[],
  total: 0,
  searched: false,
  activeFacets: {} as Record<string, string[]>,
  facets: {} as Record<string, string[]>,
  pageSize: 20,
  currentPage: 1,
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
  return params.toString();
}
