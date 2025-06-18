import React from 'react';

const JobManager = ({ activeJobs, cancelJob }) => {
  if (activeJobs.length === 0) return null;

  const getStatusColor = (status) => {
    switch (status) {
      case 'running':
        return 'bg-blue-100 text-blue-800';
      case 'completed':
        return 'bg-green-100 text-green-800';
      case 'completed_with_errors':
        return 'bg-yellow-100 text-yellow-800';
      case 'completed_unknown':
        return 'bg-purple-100 text-purple-800';
      case 'failed':
        return 'bg-red-100 text-red-800';
      case 'cancelled':
        return 'bg-gray-100 text-gray-800';
      default:
        return 'bg-gray-100 text-gray-800';
    }
  };

  const getStatusText = (status) => {
    switch (status) {
      case 'completed_unknown':
        return 'Status Unknown';
      case 'completed_with_errors':
        return 'Completed (with errors)';
      default:
        return status;
    }
  };

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
      <h2 className="text-xl font-semibold mb-4">Collection Jobs</h2>
      <div className="space-y-3">
        {activeJobs.map((job) => (
          <div key={job.job_id} className="border rounded p-3">
            <div className="flex justify-between items-start mb-2">
              <div>
                <div className="font-medium">{job.start_date} to {job.end_date}</div>
                <div className="text-sm text-gray-600">
                  Fields: {job.fields}
                  {job.breakdowns && ` | Breakdowns: ${job.breakdowns}`}
                </div>
              </div>
              <div className="flex items-center space-x-2">
                <span className={`px-2 py-1 rounded text-xs ${getStatusColor(job.status)}`}>
                  {getStatusText(job.status)}
                </span>
                {job.status === 'running' && (
                  <button
                    onClick={() => cancelJob(job.job_id)}
                    className="text-red-600 hover:text-red-800 text-xs"
                  >
                    Cancel
                  </button>
                )}
              </div>
            </div>
            
            {job.status === 'running' && (
              <div>
                <div className="w-full bg-gray-200 rounded-full h-2">
                  <div 
                    className="bg-blue-600 h-2 rounded-full" 
                    style={{ width: `${job.progress_percentage}%` }}
                  ></div>
                </div>
                <div className="text-xs text-gray-600 mt-1">
                  {job.current_date && `Processing: ${job.current_date}`}
                  {` (${Math.round(job.progress_percentage)}%)`}
                </div>
              </div>
            )}
            
            {job.status === 'completed_unknown' && job.message && (
              <div className="mt-2 text-xs text-purple-600 bg-purple-50 p-2 rounded">
                <strong>Note:</strong> {job.message}
              </div>
            )}
            
            {job.errors && job.errors.length > 0 && (
              <div className="mt-2 text-xs text-red-600">
                {job.errors.length} error(s) occurred
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
};

export default JobManager; 