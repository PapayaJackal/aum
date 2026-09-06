# Public API rate limits

The public API returns HTTP 429 when a client exceeds its request quota. Use
the Retry-After response header and exponential backoff. Do not retry a failed
write blindly; first determine whether the original request completed.

Partners that need a higher quota should contact developer support with their
expected traffic profile.
