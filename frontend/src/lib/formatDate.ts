const MONTHS = [
  'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec',
] as const;

const pad = (n: number) => String(n).padStart(2, '0');

/** Django `date:"M d, Y g:i A"` in UTC (the app's render timezone). */
export function formatCreatedAt(iso: string): string {
  const d = new Date(iso);
  const hours24 = d.getUTCHours();
  const hours12 = hours24 % 12 === 0 ? 12 : hours24 % 12;
  const ampm = hours24 < 12 ? 'AM' : 'PM';
  return `${MONTHS[d.getUTCMonth()] ?? ''} ${pad(d.getUTCDate())}, ${d.getUTCFullYear()} ${hours12}:${pad(d.getUTCMinutes())} ${ampm}`;
}

/** Django `date:"Y-m-d H:i:s"` in UTC. */
export function formatScheduledTime(iso: string): string {
  const d = new Date(iso);
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
}
