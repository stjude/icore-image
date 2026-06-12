import { useEffect } from 'react';

import { loadSettings } from '../api/endpoints';

/** Client-side port of the old `root_redirect` view: IMAGINE installs land on
 * Single-Click iCore, everyone else on Image Query. Uses a full page
 * navigation so it works regardless of whether the target page has been
 * migrated to the SPA yet. */
export function RootRedirect() {
  useEffect(() => {
    loadSettings()
      .then((settings) => {
        const target =
          settings.icore_usecase === 'imagine' ? '/singleclickicore/' : '/imagequery/';
        window.location.replace(target);
      })
      .catch(() => {
        window.location.replace('/imagequery/');
      });
  }, []);

  return null;
}
