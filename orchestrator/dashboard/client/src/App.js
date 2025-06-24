import React, { useState } from 'react';
import { Routes, Route, Link } from 'react-router-dom';
import { Settings, ChevronDown } from 'lucide-react';
import './App.css';
import { Dashboard } from './pages/Dashboard';
import { MixpanelDebugPage } from './pages/MixpanelDebugPage';
import { MetaDebugger } from './pages/MetaDebugger';
import CohortAnalyzerPage from './pages/CohortAnalyzerPage';
import CohortAnalyzerV3RefactoredPage from './pages/CohortAnalyzerV3RefactoredPage';
import CohortPipelineDebugPage from './pages/CohortPipelineDebugPage';
import ConversionProbabilityPage from './pages/ConversionProbabilityPageRefactored';
import PricingManagementPage from './pages/PricingManagementPage';
import DataPipelinePage from './pages/DataPipelinePage';
import DataPipelineDebugPage from './pages/DataPipelineDebugPage';

// Debug pages
import PipelineDebugPage from './pages/debug/PipelineDebugPage';
import ConversionRatesDebugPage from './pages/debug/ConversionRatesDebugPage';
import PriceBucketDebugPage from './pages/debug/PriceBucketDebugPage';
import ValueEstimationDebugPage from './pages/debug/ValueEstimationDebugPage';

function App() {
  const [isDebugDropdownOpen, setIsDebugDropdownOpen] = useState(false);

  const debugItems = [
    { path: '/data-pipeline', label: 'Data Pipeline', icon: '🔧' },
    { path: '/debug/conversion-rates', label: 'Conversion Rates Debug', icon: '📈' },
    { path: '/debug/price-bucket', label: 'Price Bucket Debug', icon: '💰' },
    { path: '/debug/value-estimation', label: 'Value Estimation Debug', icon: '📊' },
    { path: '/debug', label: 'Mixpanel Debugger', icon: '🔍' },
    { path: '/meta-debug', label: 'Meta Debugger', icon: '🎯' },
  ];

  return (
    <div className="min-h-screen bg-gray-100 dark:bg-gray-900 text-gray-800 dark:text-gray-200">
      <nav className="bg-white dark:bg-gray-800 shadow-md p-4">
        <div className="container mx-auto flex justify-between items-center">
          <Link to="/" className="text-xl font-bold hover:text-blue-500 transition-colors">
            Ads Dashboard
          </Link>
          <div className="flex items-center space-x-4">
            {/* Debug Dropdown */}
            <div className="relative">
              <button
                onClick={() => setIsDebugDropdownOpen(!isDebugDropdownOpen)}
                className="flex items-center space-x-1 px-3 py-2 text-gray-600 dark:text-gray-300 hover:text-blue-500 transition-colors rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700"
                aria-label="Debug Tools"
              >
                <Settings className="h-4 w-4" />
                <ChevronDown className={`h-3 w-3 transition-transform ${isDebugDropdownOpen ? 'rotate-180' : ''}`} />
              </button>
              
              {isDebugDropdownOpen && (
                <div className="absolute right-0 mt-2 w-56 bg-white dark:bg-gray-800 rounded-lg shadow-lg border border-gray-200 dark:border-gray-700 z-50">
                  <div className="py-1">
                    <div className="px-3 py-2 text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide border-b border-gray-200 dark:border-gray-700">
                      Debug Tools
                    </div>
                    {debugItems.map((item) => (
                      <Link
                        key={item.path}
                        to={item.path}
                        onClick={() => setIsDebugDropdownOpen(false)}
                        className="flex items-center space-x-2 px-3 py-2 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700 hover:text-blue-500 transition-colors"
                      >
                        <span className="text-base">{item.icon}</span>
                        <span>{item.label}</span>
                      </Link>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </nav>
      
      {/* Click outside to close dropdown */}
      {isDebugDropdownOpen && (
        <div 
          className="fixed inset-0 z-40" 
          onClick={() => setIsDebugDropdownOpen(false)}
        />
      )}
      
      <div className="w-full">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/cohort-analyzer" element={<CohortAnalyzerPage />} />
          <Route path="/cohort-analyzer-v3" element={<CohortAnalyzerV3RefactoredPage />} />
          <Route path="/conversion-probability" element={<ConversionProbabilityPage />} />
          <Route path="/pricing-management" element={<PricingManagementPage />} />
          <Route path="/cohort-pipeline" element={<CohortPipelineDebugPage />} />
          <Route path="/data-pipeline" element={<DataPipelinePage />} />
          <Route path="/data-pipeline/debug" element={<DataPipelineDebugPage />} />
          
          {/* Pipeline Debug Routes */}
          <Route path="/pipeline-debug" element={<PipelineDebugPage />} />
          <Route path="/debug/conversion-rates" element={<ConversionRatesDebugPage />} />
          <Route path="/debug/price-bucket" element={<PriceBucketDebugPage />} />
          <Route path="/debug/value-estimation" element={<ValueEstimationDebugPage />} />
          
          <Route path="/debug" element={<MixpanelDebugPage />} />
          <Route path="/meta-debug" element={<MetaDebugger />} />
        </Routes>
      </div>
    </div>
  );
}

export default App; 