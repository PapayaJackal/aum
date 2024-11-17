
type QueryResult = {
  offset: number;
  limit: number;
  estimatedTotalHits: number;
  processingTimeMs: number;
  query: string;
  hits: {
    id: string;
    content: string;
    metadata: Record<string, string>;
  }[];
}

type QueryResponse = QueryResult | { error: string };