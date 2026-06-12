import { useEffect, useRef } from 'react';

/** Run `callback` immediately and then every `intervalMs` while `enabled`. */
export function usePolling(callback: () => void, intervalMs: number, enabled = true) {
  const callbackRef = useRef(callback);
  callbackRef.current = callback;

  useEffect(() => {
    if (!enabled) return;
    callbackRef.current();
    const id = setInterval(() => {
      callbackRef.current();
    }, intervalMs);
    return () => {
      clearInterval(id);
    };
  }, [intervalMs, enabled]);
}
