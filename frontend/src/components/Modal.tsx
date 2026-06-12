import type { ReactNode } from 'react';

interface Props {
  children: ReactNode;
  /** Overrides the inner panel; defaults match the existing small modals. */
  panelClassName?: string;
}

/** Full-screen overlay modal matching the app's existing modal styling. */
export function Modal({ children, panelClassName }: Props) {
  return (
    <div className="fixed inset-0 bg-gray-600 bg-opacity-50 overflow-y-auto h-full w-full z-50">
      <div
        className={
          panelClassName ??
          'relative top-20 mx-auto p-5 border w-96 shadow-lg rounded-md bg-white'
        }
      >
        {children}
      </div>
    </div>
  );
}
