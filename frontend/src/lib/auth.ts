const TOKEN_KEY = "aum_access_token";
const REFRESH_KEY = "aum_refresh_token";

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

export function getRefreshToken(): string | null {
  return localStorage.getItem(REFRESH_KEY);
}

export function setAuth(accessToken: string, refreshToken: string): void {
  localStorage.setItem(TOKEN_KEY, accessToken);
  localStorage.setItem(REFRESH_KEY, refreshToken);
}

export function clearAuth(): void {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(REFRESH_KEY);
}

export function isAuthenticated(): boolean {
  return _publicMode || getToken() !== null;
}
