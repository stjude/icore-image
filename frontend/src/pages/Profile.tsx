import { useEffect, useState } from 'react';

import { loadSettings, saveSettings } from '../api/endpoints';

type Usecase = 'internal' | 'imagine' | '';

export function Profile() {
  const [usecase, setUsecase] = useState<Usecase>('');
  const [showSuccess, setShowSuccess] = useState(false);

  useEffect(() => {
    loadSettings()
      .then((settings) => {
        setUsecase(settings.icore_usecase ?? '');
      })
      .catch((error: unknown) => {
        console.error('Error loading profile:', error);
      });
  }, []);

  const handleSave = async () => {
    if (!usecase) {
      alert('Please select a use case');
      return;
    }

    try {
      // The legacy page re-fetches settings before saving so that all other
      // settings keys are preserved when posting the whole object back.
      const settings = await loadSettings();
      settings.icore_usecase = usecase;
      await saveSettings(settings);

      setShowSuccess(true);
      setTimeout(() => {
        setShowSuccess(false);
      }, 3000);

      // The legacy page calls applyModuleFiltering(usecase) to update the
      // sidebar immediately; the shared Layout listens for this event.
      window.dispatchEvent(new CustomEvent('icore:usecase-changed', { detail: usecase }));
    } catch (error) {
      console.error('Error saving profile:', error);
      alert('Error saving profile. Please try again.');
    }
  };

  return (
    <>
      <div className="px-4">
        <h1 className="text-xl flex-1">Profile</h1>
      </div>
      <div className="flex-1 bg-white p-4 ml-4 mt-4">
        <div className="text-md mb-4">iCore Use Case</div>
        <div className="text-sm text-gray-500 mb-4">
          Select how you are using iCore. This determines which modules are available.
        </div>

        <div className="mt-4">
          <div className="mb-3">
            <label className="inline-flex items-center">
              <input
                type="radio"
                name="usecase"
                value="internal"
                className="form-radio"
                id="usecase-internal"
                checked={usecase === 'internal'}
                onChange={() => setUsecase('internal')}
              />
              <span className="ml-2">Local/Internal use only</span>
            </label>
            <div className="ml-6 mt-1 text-sm text-gray-500">
              Access to all iCore modules for local deidentification projects
            </div>
          </div>
          <div className="mb-3">
            <label className="inline-flex items-center">
              <input
                type="radio"
                name="usecase"
                value="imagine"
                className="form-radio"
                id="usecase-imagine"
                checked={usecase === 'imagine'}
                onChange={() => setUsecase('imagine')}
              />
              <span className="ml-2">IMAGINE Initiative project</span>
            </label>
            <div className="ml-6 mt-1 text-sm text-gray-500">
              Access limited to Single-Click iCore and Transfer Data to IMAGINE modules for
              IMAGINE project compliance
            </div>
          </div>
        </div>

        <div className="mt-6">
          <button
            id="saveUsecaseBtn"
            className="px-4 py-2 bg-blue-500 text-white text-base font-medium rounded-md shadow-sm hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-300"
            onClick={() => void handleSave()}
          >
            Save
          </button>
        </div>

        <div
          id="successMessage"
          className={`${showSuccess ? '' : 'hidden '}mt-4 p-3 bg-green-100 text-green-700 rounded`}
        >
          Profile updated successfully!
        </div>
      </div>
    </>
  );
}
