import React from 'react';
import { Routes, Route, Link } from 'react-router-dom';
import './App.css';
import { Dashboard } from './pages/Dashboard';
import { MixpanelDebugPage } from './pages/MixpanelDebugPage';
import { MetaDebugger } from './pages/MetaDebugger';
import CohortAnalyzerPage from './pages/CohortAnalyzerPage';
import CohortAnalyzerV3RefactoredPage from './pages/CohortAnalyzerV3RefactoredPage';
import CohortPipelineDebugPage from './pages/CohortPipelineDebugPage';
import ConversionProbabilityPage from './pages/ConversionProbabilityPageRefactored';
import PricingManagementPage from './pages/PricingManagementPage';

function App() {
  return (
    <div className="min-h-screen bg-gray-100 dark:bg-gray-900 text-gray-800 dark:text-gray-200">
      <nav className="bg-white dark:bg-gray-800 shadow-md p-4">
        <div className="container mx-auto flex justify-between items-center">
          <div className="text-xl font-bold">Ads Dashboard</div>
          <div className="space-x-4">
            <Link to="/" className="hover:text-blue-500">Dashboard</Link>
            <Link to="/cohort-analyzer" className="hover:text-blue-500">Cohort Analyzer</Link>
            <Link to="/cohort-analyzer-v3" className="hover:text-blue-500 text-purple-600 dark:text-purple-400">Cohort V3</Link>
            <Link to="/conversion-probability" className="hover:text-blue-500">Conversion Probability</Link>
            <Link to="/pricing-management" className="hover:text-blue-500">Pricing Management</Link>
            <Link to="/cohort-pipeline" className="hover:text-blue-500">Cohort Pipeline</Link>
            <Link to="/debug" className="hover:text-blue-500">Mixpanel Debugger</Link>
            <Link to="/meta-debug" className="hover:text-blue-500">Meta Debugger</Link>
          </div>
        </div>
      </nav>
      
      <div className="w-full">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/cohort-analyzer" element={<CohortAnalyzerPage />} />
          <Route path="/cohort-analyzer-v3" element={<CohortAnalyzerV3RefactoredPage />} />
          <Route path="/conversion-probability" element={<ConversionProbabilityPage />} />
          <Route path="/pricing-management" element={<PricingManagementPage />} />
          <Route path="/cohort-pipeline" element={<CohortPipelineDebugPage />} />
          <Route path="/debug" element={<MixpanelDebugPage />} />
          <Route path="/meta-debug" element={<MetaDebugger />} />
        </Routes>
      </div>
    </div>
  );
}

export default App; 