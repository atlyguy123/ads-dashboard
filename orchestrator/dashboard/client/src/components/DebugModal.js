import React, { useEffect, useState } from 'react';
import { X } from 'lucide-react';

// Placeholder for API service
const api = {
  getDebugModalData: async (id, type) => {
    console.log(`Fetching debug data for id: ${id}, type: ${type}`);
    // Simulate API call
    await new Promise(resolve => setTimeout(resolve, 500));
    return {
      lastMetaJson: { sample: 'data', id, type },
      mixpanelEventCounts: { eventA: 100, eventB: 200 },
      joinRatePercent: 75.5,
      lastEtlTimestamps: { meta: new Date().toISOString(), mixpanel: new Date().toISOString() }
    };
  }
};

export const DebugModal = ({ isOpen, onClose, data }) => {
  const [debugInfo, setDebugInfo] = useState({
    lastMetaJson: {},
    mixpanelEventCounts: {},
    joinRatePercent: 0,
    lastEtlTimestamps: {}
  });
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (isOpen && data) {
      const fetchDebugData = async () => {
        setIsLoading(true);
        setError(null);
        try {
          const result = await api.getDebugModalData(data.id, data.type);
          setDebugInfo(result);
        } catch (err) {
          console.error('Error fetching debug modal data:', err);
          setError('Failed to load debug data. Please try again later.');
        } finally {
          setIsLoading(false);
        }
      };

      fetchDebugData();
    }
  }, [isOpen, data]);

  if (!isOpen || !data) return null;

  return (
    <div className={`fixed inset-0 z-50 overflow-y-auto ${isOpen ? 'block' : 'hidden'}`}>
      <div className="flex items-end justify-center min-h-screen pt-4 px-4 pb-20 text-center sm:block sm:p-0">
        <div className="fixed inset-0 transition-opacity" aria-hidden="true">
          <div className="absolute inset-0 bg-black/50"></div>
        </div>
        <span className="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>
        <div className="inline-block align-bottom bg-white dark:bg-gray-800 rounded-lg text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-2xl sm:w-full">
          <div className="bg-white dark:bg-gray-800 px-4 pt-5 pb-4 sm:p-6 sm:pb-4">
            <div className="sm:flex sm:items-start">
              <div className="mt-3 text-center sm:mt-0 sm:ml-4 sm:text-left w-full">
                <h3 className="text-xl leading-6 font-semibold text-gray-900 dark:text-gray-100" id="modal-title">
                  Debug Information: {data.name}
                </h3>
                <button onClick={onClose} className="absolute top-4 right-4 p-1 text-gray-400 hover:text-gray-600 dark:text-gray-500 dark:hover:text-gray-300">
                  <X size={20} />
                </button>
                <div className="mt-4">
                  {isLoading ? (
                    <div className="flex justify-center items-center h-64">
                      <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500"></div>
                    </div>
                  ) : error ? (
                    <div className="text-center text-red-500 p-4">{error}</div>
                  ) : (
                    <div className="space-y-4 text-sm">
                      <div>
                        <h4 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-1">Last Raw Meta JSON Row</h4>
                        <pre className="bg-gray-100 dark:bg-gray-700 p-3 rounded text-xs overflow-x-auto max-h-60">
                          {JSON.stringify(debugInfo.lastMetaJson, null, 2)}
                        </pre>
                      </div>
                      <div>
                        <h4 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-1">Mixpanel Event Counts</h4>
                        <pre className="bg-gray-100 dark:bg-gray-700 p-3 rounded text-xs overflow-x-auto max-h-60">
                          {JSON.stringify(debugInfo.mixpanelEventCounts, null, 2)}
                        </pre>
                      </div>
                      <div>
                        <h4 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-1">Join Rate</h4>
                        <p className="text-gray-700 dark:text-gray-300">{debugInfo.joinRatePercent?.toFixed(1) || 0}% (Matched vs Total)</p>
                      </div>
                      <div>
                        <h4 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-1">Last ETL Timestamps</h4>
                        <p className="text-gray-700 dark:text-gray-300">Meta: {debugInfo.lastEtlTimestamps?.meta ? new Date(debugInfo.lastEtlTimestamps.meta).toLocaleString() : 'N/A'}</p>
                        <p className="text-gray-700 dark:text-gray-300">Mixpanel: {debugInfo.lastEtlTimestamps?.mixpanel ? new Date(debugInfo.lastEtlTimestamps.mixpanel).toLocaleString() : 'N/A'}</p>
                      </div>
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>
          <div className="bg-gray-50 dark:bg-gray-700 px-4 py-3 sm:px-6 sm:flex sm:flex-row-reverse">
            <button
              type="button"
              onClick={onClose}
              className="mt-3 w-full inline-flex justify-center rounded-md border border-gray-300 dark:border-gray-600 shadow-sm px-4 py-2 bg-white dark:bg-gray-800 text-base font-medium text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 sm:mt-0 sm:ml-3 sm:w-auto sm:text-sm"
            >
              Close
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}; 