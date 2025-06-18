import React from 'react';
import { AlertCircle, CheckCircle, X } from 'lucide-react';

const AlertMessages = ({ error, success, onClearError, onClearSuccess }) => {
  return (
    <>
      {/* Error Alert */}
      {error && (
        <div className="mb-6 bg-red-50 dark:bg-red-900/50 border border-red-200 dark:border-red-800 rounded-lg p-4">
          <div className="flex items-center">
            <AlertCircle className="h-5 w-5 text-red-400 mr-3" />
            <div className="flex-1">
              <p className="text-red-800 dark:text-red-200">{error}</p>
            </div>
            <button
              onClick={onClearError}
              className="text-red-400 hover:text-red-600"
            >
              <X className="h-5 w-5" />
            </button>
          </div>
        </div>
      )}

      {/* Success Alert */}
      {success && (
        <div className="mb-6 bg-green-50 dark:bg-green-900/50 border border-green-200 dark:border-green-800 rounded-lg p-4">
          <div className="flex items-center">
            <CheckCircle className="h-5 w-5 text-green-400 mr-3" />
            <div className="flex-1">
              <p className="text-green-800 dark:text-green-200">{success}</p>
            </div>
            <button
              onClick={onClearSuccess}
              className="text-green-400 hover:text-green-600"
            >
              <X className="h-5 w-5" />
            </button>
          </div>
        </div>
      )}
    </>
  );
};

export default AlertMessages; 