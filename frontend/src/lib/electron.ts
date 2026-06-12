/** The surface exposed by electron/preload.js via contextBridge. Absent when
 * the app runs in a plain browser (e.g. during development or testing). */
interface ElectronAPI {
  getPathForFile: (file: File) => Promise<string> | string;
  openFolder: (folderPath: string) => void;
}

declare global {
  interface Window {
    electronAPI?: ElectronAPI;
  }
}

export function getPathForFile(file: File): Promise<string> | string | null {
  return window.electronAPI ? window.electronAPI.getPathForFile(file) : null;
}

export function openFolder(folderPath: string): void {
  window.electronAPI?.openFolder(folderPath);
}
