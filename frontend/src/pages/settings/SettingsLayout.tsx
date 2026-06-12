import { useRef, useState } from 'react';
import { Outlet, useLocation } from 'react-router';

const TABS = [
  { href: '/settings/general/', label: 'General' },
  { href: '/settings/image_qr/', label: 'Image Query' },
  { href: '/settings/image_deid/', label: 'Image Deid' },
  { href: '/settings/text_deid/', label: 'Text Deid' },
  { href: '/settings/admin/', label: 'Administrator' },
];

export type SaveHandler = () => void;

export interface SettingsOutletContext {
  /** Tabs register their save handler here; the layout's "Save Settings"
   * button invokes it (the React replacement for the legacy convention of a
   * page-global saveSettings() called by base_settings.html). */
  registerSaveHandler: (handler: SaveHandler | null) => void;
  /** Transient status text next to the Save button (legacy showSaveMessage,
   * which every tab used to re-implement with imperative DOM). */
  showSaveMessage: (message: string, isError?: boolean) => void;
}

export function SettingsLayout() {
  const { pathname } = useLocation();
  const current = pathname.endsWith('/') ? pathname : `${pathname}/`;
  const saveHandler = useRef<SaveHandler | null>(null);
  const [saveMessage, setSaveMessage] = useState<{ text: string; isError: boolean } | null>(
    null,
  );
  const messageTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const context: SettingsOutletContext = {
    registerSaveHandler: (handler) => {
      saveHandler.current = handler;
    },
    showSaveMessage: (message, isError = false) => {
      if (messageTimer.current) clearTimeout(messageTimer.current);
      setSaveMessage({ text: message, isError });
      messageTimer.current = setTimeout(() => {
        setSaveMessage(null);
      }, 3000);
    },
  };

  return (
    <>
      <div className="px-4">
        <h1 className="text-xl flex-1 mb-4">Settings</h1>
        <div className="flex border-b border-gray-300">
          {TABS.map((tab) => (
            <a
              key={tab.href}
              href={tab.href}
              className={`px-4 py-2 text-sm border-t border-l border-r border-gray-300 -mb-px bg-gray-200${current === tab.href ? ' bg-white' : ''}`}
            >
              {tab.label}
            </a>
          ))}
          <div className="flex-grow border-b border-gray-300 -mb-px"></div>
        </div>
      </div>
      <div className="flex-1 bg-white p-4 ml-4">
        <Outlet context={context} />
      </div>
      <div className="flex ml-4">
        <button
          id="saveSettingsBtn"
          className="mt-4 mb-4 px-2 py-1 bg-white shadow text-sm hover:bg-blue-100"
          onClick={() => {
            if (saveHandler.current) {
              console.log('Saving settings');
              saveHandler.current();
            } else {
              console.warn('No saveSettings function defined for this page');
            }
          }}
        >
          Save Settings
        </button>
        {saveMessage && (
          <div
            className={`settings-message mt-4 ml-4 ${saveMessage.isError ? 'text-red-500' : 'text-green-500'}`}
          >
            {saveMessage.text}
          </div>
        )}
      </div>
    </>
  );
}
