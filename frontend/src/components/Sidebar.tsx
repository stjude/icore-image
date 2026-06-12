import { useLocation } from 'react-router';

interface ModuleLink {
  href: string;
  label: string;
  /** IMAGINE-usecase modules stay visible when the sidebar is filtered. */
  imagine: boolean;
}

const MODULES: ModuleLink[] = [
  { href: '/imagequery', label: 'Image Query', imagine: false },
  { href: '/imagedeid', label: 'Image Deidentification', imagine: false },
  { href: '/textdeid', label: 'Text Deidentification', imagine: false },
  { href: '/imageexport', label: 'Transfer Data to IMAGINE', imagine: true },
  { href: '/imagedeidexport', label: 'Image Deidentification / Export', imagine: false },
  { href: '/singleclickicore', label: 'Single-Click iCore', imagine: true },
  { href: '/headerextract', label: 'Header Extraction', imagine: false },
];

interface Props {
  usecase: string | undefined;
}

export function Sidebar({ usecase }: Props) {
  const { pathname } = useLocation();
  const current = pathname.replace(/\/$/, '');
  const visible =
    usecase === 'imagine' ? MODULES.filter((m) => m.imagine) : MODULES;

  return (
    <section
      className="fixed z-10 w-64 h-screen bg-white shadow text-sm text-gray-700 pt-24"
      id="sidebar"
    >
      {visible.map((module) => (
        <a
          key={module.href}
          href={module.href}
          className={`block px-4 py-2 hover:bg-gray-100 ${current === module.href ? 'bg-gray-100 ' : ''}${module.imagine ? 'sidebar-module-imagine' : 'sidebar-module'}`}
        >
          {module.label}
        </a>
      ))}
    </section>
  );
}
