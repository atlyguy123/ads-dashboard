import React from 'react';

const DataCoverageDisplay = ({ dataCoverage, missingDates, configurations }) => {
  if (!dataCoverage) return null;

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
      <h2 className="text-xl font-semibold mb-4">Data Coverage Summary</h2>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="bg-blue-100 dark:bg-blue-900 p-3 rounded">
          <div className="text-2xl font-bold">{dataCoverage.total_days}</div>
          <div className="text-sm text-gray-600">Days Stored</div>
        </div>
        <div className="bg-green-100 dark:bg-green-900 p-3 rounded">
          <div className="text-2xl font-bold">{dataCoverage.total_records}</div>
          <div className="text-sm text-gray-600">Total Records</div>
        </div>
        <div className="bg-orange-100 dark:bg-orange-900 p-3 rounded">
          <div className="text-2xl font-bold">{missingDates.length}</div>
          <div className="text-sm text-gray-600">Missing Days</div>
        </div>
        <div className="bg-purple-100 dark:bg-purple-900 p-3 rounded">
          <div className="text-2xl font-bold">{configurations.length}</div>
          <div className="text-sm text-gray-600">Configurations</div>
        </div>
      </div>
      
      {dataCoverage.earliest_date && (
        <div className="mt-4 text-sm text-gray-600">
          Date Range: {dataCoverage.earliest_date} to {dataCoverage.latest_date}
        </div>
      )}
    </div>
  );
};

export default DataCoverageDisplay; 