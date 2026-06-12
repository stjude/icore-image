/** Literal class strings so the Tailwind content scan sees every variant. */
const STATUS_CLASSES: Record<string, string> = {
  PENDING: 'bg-yellow-100 text-yellow-800',
  RUNNING: 'bg-blue-100 text-blue-800',
  COMPLETED: 'bg-green-100 text-green-800',
  CANCELLED: 'bg-gray-100 text-gray-800',
};

const FALLBACK_CLASSES = 'bg-red-100 text-red-800';

interface Props {
  status: string;
  label: string;
}

export function StatusBadge({ status, label }: Props) {
  return (
    <span
      className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${STATUS_CLASSES[status] ?? FALLBACK_CLASSES}`}
    >
      {label}
    </span>
  );
}
