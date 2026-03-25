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
