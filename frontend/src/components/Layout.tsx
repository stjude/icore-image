import { useEffect, useState } from 'react';
import { Outlet, useLocation } from 'react-router';

import { loadSettings, saveSettings } from '../api/endpoints';
import logo from '../assets/logo.png';
import { Sidebar } from './Sidebar';
import { UsecaseSetupModal } from './UsecaseSetupModal';

const NAV_LINKS = [
  { href: '/task_list', label: 'Projects' },
  { href: '/profile', label: 'Profile' },
  { href: '/settings/general', label: 'Settings' },
];

export function Layout() {
  const { pathname } = useLocation();
  const current = pathname.replace(/\/$/, '');
  const [usecase, setUsecase] = useState<string | undefined>(undefined);
  const [showUsecaseModal, setShowUsecaseModal] = useState(false);

  useEffect(() => {
    let cancelled = false;
    loadSettings()
      .then((settings) => {
        if (cancelled) return;
        if (!settings.icore_usecase) {
          setShowUsecaseModal(true);
        } else {
          setUsecase(settings.icore_usecase);
        }
      })
      .catch((error: unknown) => {
        console.error('Error checking usecase setup:', error);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // Pages that change the usecase (Profile) announce it so the sidebar
  // filtering updates immediately, like the legacy applyModuleFiltering().
  useEffect(() => {
    const handler = (event: Event) => {
      setUsecase((event as CustomEvent<string>).detail);
    };
    window.addEventListener('icore:usecase-changed', handler);
    return () => {
      window.removeEventListener('icore:usecase-changed', handler);
    };
  }, []);

  const submitUsecase = (choice: 'internal' | 'imagine') => {
    void (async () => {
      try {
        const settings = await loadSettings();
        settings.icore_usecase = choice;
        await saveSettings(settings);
        setUsecase(choice);
        setShowUsecaseModal(false);
      } catch (error) {
        console.error('Error saving usecase:', error);
      }
    })();
  };

  return (
    <>
      <header className="fixed w-full z-20 p-4 flex bg-white text-white shadow">
        <div className="flex-1 text-lg my-auto">
          <a href="/imagequery" className="flex items-center space-x-2">
            <img
              id="app-logo"
              src={logo}
              width="50px"
              height="50px"
              alt="iCore Logo"
              className="object-contain"
            />
            <span className="text-gray-700 font-semibold">iCore</span>
          </a>
        </div>
        <div className="flex-initial my-auto text-sm text-gray-700">
          {NAV_LINKS.map((link) => (
            <a
              key={link.href}
              href={link.href}
              className={`rounded mx-2 px-2 py-1 hover:bg-gray-100 ${current === link.href ? 'bg-gray-100' : ''}`}
            >
              {link.label}
            </a>
          ))}
        </div>
      </header>
      <Sidebar usecase={usecase} />
      <section className="w-full h-screen pt-24 pl-64">
        <div className="max-w-4xl mx-auto mb-10">
          <Outlet />
        </div>
      </section>
      {showUsecaseModal && <UsecaseSetupModal onSubmit={submitUsecase} />}
    </>
  );
}
