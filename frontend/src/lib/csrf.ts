/** Read the Django CSRF token from the `csrftoken` cookie (set by
 * `@ensure_csrf_cookie` on the SPA index and `/load_settings/`). */
export function getCsrfToken(): string {
  const match = document.cookie.match(/(?:^|;\s*)csrftoken=([^;]+)/);
  return match?.[1] ?? '';
}
