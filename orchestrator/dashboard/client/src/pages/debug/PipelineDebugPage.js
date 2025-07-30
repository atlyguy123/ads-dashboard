import React from 'react';
import { Link } from 'react-router-dom';

const PipelineDebugPage = () => {
  const debugTools = [
    {
      id: 'conversion-rates',
      title: 'Conversion Rates Debug',
      description: 'Debug and analyze conversion rate calculations from the pipeline',
      icon: '🔍',
      path: '/debug/conversion-rates',
      status: 'ready'
    },
    {
      id: 'price-bucket',
      title: 'Price Bucket Debug',
      description: 'Debug and analyze price bucket assignments from the pipeline',
      icon: '💰',
      path: '/debug/price-bucket',
      status: 'ready'
    },
    {
      id: 'value-estimation',
      title: 'Value Estimation Debug',
      description: 'Debug and analyze value estimation calculations from the pipeline',
      icon: '📊',
      path: '/debug/value-estimation',
      status: 'ready'
    },
    {
      id: 'id-name-mapping',
      title: 'ID-Name Mapping Debug',
      description: 'Debug and validate canonical ID-to-name mappings from Meta pipeline',
      icon: '🏷️',
      path: '/debug/id-name-mapping',
      status: 'in-development'
    },
    {
      id: 'hierarchy-mapping',
      title: 'Hierarchy Mapping Debug',
      description: 'Debug campaign → adset → ad hierarchy relationships and confidence scores',
      icon: '🏗️',
      path: '/debug/hierarchy-mapping',
      status: 'in-development'
    },
    {
      id: 'daily-metrics',
      title: 'Daily Metrics Debug',
      description: 'Debug pre-computed daily metrics with user deduplication and revenue calculations',
      icon: '📈',
      path: '/debug/daily-metrics',
      status: 'in-development'
    }
  ];

  return (
    <div className="p-6 max-w-7xl mx-auto">
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md p-6">
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-2">
            Pipeline Debug Tools
          </h1>
          <p className="text-gray-600 dark:text-gray-300">
            Debug and analyze different stages of the processing pipeline.
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {debugTools.map((tool) => (
            <Link
              key={tool.id}
              to={tool.path}
              className="bg-gray-50 dark:bg-gray-700 rounded-lg p-6 hover:bg-gray-100 dark:hover:bg-gray-600 transition-colors duration-200 group"
            >
              <div className="flex items-center mb-4">
                <div className="text-3xl mr-3">{tool.icon}</div>
                <div>
                  <h3 className="text-lg font-semibold text-gray-900 dark:text-white group-hover:text-blue-600 dark:group-hover:text-blue-400">
                    {tool.title}
                  </h3>
                  <span className={`inline-block px-2 py-1 text-xs rounded-full ${
                    tool.status === 'ready' 
                      ? 'bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-400'
                      : 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/20 dark:text-yellow-400'
                  }`}>
                    {tool.status === 'ready' ? 'Ready' : 'In Development'}
                  </span>
                </div>
              </div>
              <p className="text-gray-600 dark:text-gray-300 text-sm">
                {tool.description}
              </p>
              <div className="mt-4 flex items-center text-blue-600 dark:text-blue-400 text-sm">
                <span>Open Debug Tool</span>
                <svg className="w-4 h-4 ml-1 group-hover:translate-x-1 transition-transform duration-200" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                </svg>
              </div>
            </Link>
          ))}
        </div>

        <div className="mt-8 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
          <h3 className="text-blue-800 dark:text-blue-200 font-medium mb-2">📝 Debug Tools Overview</h3>
          <ul className="text-blue-600 dark:text-blue-300 text-sm space-y-1">
            <li>• <strong>Conversion Rates:</strong> Analyze how conversion rates are calculated and assigned to user cohorts</li>
            <li>• <strong>Price Bucket:</strong> Debug price bucket assignments and validate data integrity</li>
            <li>• <strong>Value Estimation:</strong> Examine value estimation calculations and timeline data</li>
            <li>• <strong>ID-Name Mapping:</strong> Validate canonical name assignments and frequency analysis for all advertising IDs</li>
            <li>• <strong>Hierarchy Mapping:</strong> Debug campaign → adset → ad relationships and confidence scoring</li>
            <li>• <strong>Daily Metrics:</strong> Analyze pre-computed metrics with user deduplication and revenue calculations</li>
          </ul>
        </div>
      </div>
    </div>
  );
};

export default PipelineDebugPage; 