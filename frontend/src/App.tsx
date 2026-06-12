import { BrowserRouter, Route, Routes } from 'react-router';

import { Layout } from './components/Layout';
import { HeaderExtract } from './pages/HeaderExtract';
import { ImageDeid } from './pages/ImageDeid';
import { ImageDeidExport } from './pages/ImageDeidExport';
import { ImageExport } from './pages/ImageExport';
import { ImageQuery } from './pages/ImageQuery';
import { SingleClickICore } from './pages/SingleClickICore';
import { AdminSettings } from './pages/settings/AdminSettings';
import { GeneralSettings } from './pages/settings/GeneralSettings';
import { ImageDeidSettings } from './pages/settings/ImageDeidSettings';
import { ImageQuerySettings } from './pages/settings/ImageQuerySettings';
import { SettingsLayout } from './pages/settings/SettingsLayout';
import { TextDeidSettings } from './pages/settings/TextDeidSettings';
import { Profile } from './pages/Profile';
import { RootRedirect } from './pages/RootRedirect';
import { TaskList } from './pages/TaskList';
import { TaskProgress } from './pages/TaskProgress';
import { TextDeid } from './pages/TextDeid';

/* Routes are added here as each page is migrated off Django templates; until
 * then, Django's explicit URL patterns win over the SPA catch-all, so
 * unmigrated paths never reach this router. */
export function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<RootRedirect />} />
        <Route element={<Layout />}>
          <Route path="/tasks" element={<TaskList />} />
          <Route path="/task_list" element={<TaskList />} />
          <Route path="/task_progress" element={<TaskProgress />} />
          <Route path="/headerextract" element={<HeaderExtract />} />
          <Route path="/imageexport" element={<ImageExport />} />
          <Route path="/imagequery" element={<ImageQuery />} />
          <Route path="/imagedeid" element={<ImageDeid />} />
          <Route path="/imagedeidexport" element={<ImageDeidExport />} />
          <Route path="/singleclickicore" element={<SingleClickICore />} />
          <Route path="/textdeid" element={<TextDeid />} />
          <Route path="/profile" element={<Profile />} />
          <Route element={<SettingsLayout />}>
            <Route path="/settings/general" element={<GeneralSettings />} />
            <Route path="/settings/image_qr" element={<ImageQuerySettings />} />
            <Route path="/settings/image_deid" element={<ImageDeidSettings />} />
            <Route path="/settings/text_deid" element={<TextDeidSettings />} />
            <Route path="/settings/admin" element={<AdminSettings />} />
          </Route>
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
