const TOKEN_KEY = "aum_session_token";

let _publicMode = false;

export function setPublicMode(val: boolean): void {
  _publicMode = val;
}

export function isPublicMode(): boolean {
  return _publicMode;
}

export function getToken(): string | null {
  return localStorage.getItem(TOKEN_KEY);
}

export function setAuth(sessionToken: string): void {
  localStorage.setItem(TOKEN_KEY, sessionToken);
}

export function clearAuth(): void {
  localStorage.removeItem(TOKEN_KEY);
}

export function isAuthenticated(): boolean {
  return _publicMode || getToken() !== null;
}
