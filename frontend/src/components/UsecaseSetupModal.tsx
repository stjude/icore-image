import { useState } from 'react';

interface Props {
  onSubmit: (usecase: 'internal' | 'imagine') => void;
}

export function UsecaseSetupModal({ onSubmit }: Props) {
  const [usecase, setUsecase] = useState<'internal' | 'imagine'>('internal');

  return (
    <div
      id="usecaseModal"
      className="fixed inset-0 bg-gray-600 bg-opacity-50 overflow-y-auto h-full w-full z-50"
    >
      <div className="relative top-20 mx-auto p-5 border w-96 shadow-lg rounded-md bg-white">
        <div className="mt-3">
          <h3 className="text-lg leading-6 font-medium text-gray-900 mb-4">
            How will data be used?
          </h3>
          <div className="mt-2 px-7 py-3">
            <div className="mb-3">
              <label className="inline-flex items-center">
                <input
                  type="radio"
                  name="usecase"
                  value="internal"
                  className="form-radio"
                  checked={usecase === 'internal'}
                  onChange={() => setUsecase('internal')}
                />
                <span className="ml-2">Local/Internal use only</span>
              </label>
            </div>
            <div className="mb-3">
              <label className="inline-flex items-center">
                <input
                  type="radio"
                  name="usecase"
                  value="imagine"
                  className="form-radio"
                  checked={usecase === 'imagine'}
                  onChange={() => setUsecase('imagine')}
                />
                <span className="ml-2">IMAGINE Initiative project</span>
              </label>
            </div>
          </div>
          <div className="items-center px-4 py-3">
            <button
              id="usecaseSubmitBtn"
              className="px-4 py-2 bg-blue-500 text-white text-base font-medium rounded-md w-full shadow-sm hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-300"
              onClick={() => onSubmit(usecase)}
            >
              Continue
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
