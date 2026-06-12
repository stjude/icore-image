import { useEffect, useState } from 'react';

import { getConstants, type Constants } from '../api/constants';

/** Resolve the shared backend constants; null until loaded. */
export function useConstants(): Constants | null {
  const [constants, setConstants] = useState<Constants | null>(null);

  useEffect(() => {
    let cancelled = false;
    getConstants()
      .then((value) => {
        if (!cancelled) setConstants(value);
      })
      .catch((error: unknown) => {
        console.error('Error loading constants:', error);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  return constants;
}
