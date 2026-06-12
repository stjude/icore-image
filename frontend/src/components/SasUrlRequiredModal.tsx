import { postJson } from '../api/client';

interface ValidateSasUrlResponse {
  valid: boolean;
  error?: string;
}

/** The export pages' on-load SAS gate: returns invalid (rather than throwing)
 * when the validation request itself fails. */
export async function validateSasUrlOnLoad(sasUrl: string): Promise<ValidateSasUrlResponse> {
  try {
    return await postJson<ValidateSasUrlResponse>('/validate_sas_url/', { sas_url: sasUrl });
  } catch (error) {
    console.error('Error validating SAS URL:', error);
    return { valid: false, error: 'Error validating SAS URL' };
  }
}

interface Props {
  onClose: () => void;
}

/** modals/sas_url_required.html — identical markup on the Image Export and
 * Image Deid+Export pages. */
export function SasUrlRequiredModal({ onClose }: Props) {
  return (
    <div
      id="sasUrlRequiredModal"
      className="fixed inset-0 bg-gray-600 bg-opacity-75 overflow-y-auto h-full w-full z-50"
    >
      <div className="relative top-20 mx-auto p-6 border w-[600px] shadow-lg rounded-md bg-white">
        <div className="mt-3">
          <div className="flex items-center justify-center mb-4">
            <div className="flex items-center justify-center h-12 w-12 rounded-full bg-red-100">
              <svg
                className="h-6 w-6 text-red-600"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth="2"
                  d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
                />
              </svg>
            </div>
          </div>
          <h3 className="text-lg leading-6 font-medium text-gray-900 text-center mb-2">
            SAS URL Required
          </h3>
          <div className="mt-2 px-7 py-3">
            <p className="text-sm text-gray-700 text-center mb-4">
              SAS URL must be configured in Admin Settings before using this module.
            </p>
            <p className="text-sm text-gray-600 text-center">
              Please contact your administrator or navigate to Admin Settings to configure the
              Azure Blob Storage SAS URL for data transfer to IMAGINE.
            </p>
          </div>
          <div className="items-center px-4 py-3 flex justify-center gap-2">
            <button
              onClick={onClose}
              className="px-6 py-2 bg-gray-300 text-gray-700 text-base font-medium rounded-md shadow-sm hover:bg-gray-400 focus:outline-none focus:ring-2 focus:ring-gray-300"
            >
              Cancel
            </button>
            <button
              onClick={() => {
                window.location.href = '/settings/admin/';
              }}
              className="px-6 py-2 bg-blue-500 text-white text-base font-medium rounded-md shadow-sm hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-300"
            >
              Go to Admin Settings
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
