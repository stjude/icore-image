import type { DragEvent, InputHTMLAttributes } from 'react';

import { getPathForFile } from '../lib/electron';

interface Props extends Omit<InputHTMLAttributes<HTMLInputElement>, 'onChange' | 'value' | 'type'> {
  value: string;
  onChange: (path: string) => void;
}

/** Text input accepting Electron file drag-drop: dropping a file fills the
 * input with its full OS path (replaces the legacy global drop handler in
 * base.html, which set input.value directly). */
export function PathInput({ value, onChange, ...rest }: Props) {
  const handleDrop = (event: DragEvent<HTMLInputElement>) => {
    event.preventDefault();
    const file = event.dataTransfer.files[0];
    if (!file) return;
    void (async () => {
      const fullPath = await getPathForFile(file);
      if (fullPath !== null) {
        onChange(fullPath);
      }
    })();
  };

  return (
    <input
      type="text"
      value={value}
      onChange={(event) => {
        onChange(event.target.value);
      }}
      onDragOver={(event) => {
        event.preventDefault();
      }}
      onDrop={handleDrop}
      {...rest}
    />
  );
}
