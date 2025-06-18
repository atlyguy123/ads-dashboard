import React, { useState, useEffect } from 'react';
import LiveApiTester from './LiveApiTester';
import HistoricalDataManager from './HistoricalDataManager';
import ActionMapper from './ActionMapper';
import { DEFAULT_FIELDS } from './utils/metaConstants';
import { compactObject } from './utils/metaApiUtils';

export const MetaDebugger = () => {
  // Mode state
  const [activeTab, setActiveTab] = useState('live'); // 'live', 'historical', 'mapping'
  
  // Form input state
  const [startDateInput, setStartDateInput] = useState('');
  const [endDateInput, setEndDateInput] = useState('');
  const [incrementInput, setIncrementInput] = useState('1');
  
  // Fields selection state
  const [selectedFields, setSelectedFields] = useState({});
  const [selectedBreakdowns, setSelectedBreakdowns] = useState({});
  
  // Shared error state
  const [error, setError] = useState(null);
  
  // Initialize fields with defaults
  useEffect(() => {
    // Try to load from localStorage first
    const savedFields = localStorage.getItem('metaDebugger_selectedFields');
    const savedBreakdowns = localStorage.getItem('metaDebugger_selectedBreakdowns');
    
    if (savedFields) {
      setSelectedFields(JSON.parse(savedFields));
    } else {
      // Otherwise use defaults
      const defaults = {};
      DEFAULT_FIELDS.split(',').forEach(field => {
        defaults[field] = true;
      });
      setSelectedFields(defaults);
    }
    
    if (savedBreakdowns) {
      setSelectedBreakdowns(JSON.parse(savedBreakdowns));
    }
  }, []);
  
  // Load other saved values from localStorage on initial render
  useEffect(() => {
    const savedStartDate = localStorage.getItem('metaDebugger_startDate');
    const savedEndDate = localStorage.getItem('metaDebugger_endDate');
    const savedIncrement = localStorage.getItem('metaDebugger_increment');
    
    if (savedStartDate) setStartDateInput(savedStartDate);
    if (savedEndDate) setEndDateInput(savedEndDate);
    if (savedIncrement) setIncrementInput(savedIncrement);
  }, []);
  
  // Save values to localStorage whenever they change
  useEffect(() => {
    localStorage.setItem('metaDebugger_startDate', startDateInput);
    localStorage.setItem('metaDebugger_endDate', endDateInput);
    localStorage.setItem('metaDebugger_increment', incrementInput);
    localStorage.setItem('metaDebugger_selectedFields', JSON.stringify(compactObject(selectedFields)));
    localStorage.setItem('metaDebugger_selectedBreakdowns', JSON.stringify(compactObject(selectedBreakdowns)));
  }, [startDateInput, endDateInput, incrementInput, selectedFields, selectedBreakdowns]);

  return (
    <div className="flex">
      <div className="flex-1 p-6 max-w-5xl">
        <h1 className="text-2xl font-bold mb-6">Meta API Debugger</h1>
        
        {/* Mode Toggle */}
        <div className="bg-gray-100 dark:bg-gray-800 rounded-lg p-4 mb-6">
          <div className="flex space-x-4">
            <button
              onClick={() => setActiveTab('live')}
              className={`px-4 py-2 rounded ${activeTab === 'live' 
                ? 'bg-blue-600 text-white' 
                : 'bg-gray-200 text-gray-700'}`}
            >
              Live API Testing
            </button>
            <button
              onClick={() => setActiveTab('historical')}
              className={`px-4 py-2 rounded ${activeTab === 'historical' 
                ? 'bg-green-600 text-white' 
                : 'bg-gray-200 text-gray-700'}`}
            >
              Historical Data Collection
            </button>
            <button
              onClick={() => setActiveTab('mapping')}
              className={`px-4 py-2 rounded ${activeTab === 'mapping' 
                ? 'bg-purple-600 text-white' 
                : 'bg-gray-200 text-gray-700'}`}
            >
              Action Mapping
            </button>
          </div>
        </div>

        {/* Render appropriate mode component */}
        {activeTab === 'live' && (
          <LiveApiTester
            startDateInput={startDateInput}
            setStartDateInput={setStartDateInput}
            endDateInput={endDateInput}
            setEndDateInput={setEndDateInput}
            incrementInput={incrementInput}
            setIncrementInput={setIncrementInput}
            selectedFields={selectedFields}
            setSelectedFields={setSelectedFields}
            selectedBreakdowns={selectedBreakdowns}
            setSelectedBreakdowns={setSelectedBreakdowns}
            error={error}
            setError={setError}
          />
        )}
        {activeTab === 'historical' && (
          <HistoricalDataManager
            startDateInput={startDateInput}
            setStartDateInput={setStartDateInput}
            endDateInput={endDateInput}
            setEndDateInput={setEndDateInput}
            selectedFields={selectedFields}
            setSelectedFields={setSelectedFields}
            selectedBreakdowns={selectedBreakdowns}
            setSelectedBreakdowns={setSelectedBreakdowns}
            error={error}
            setError={setError}
          />
        )}
        {activeTab === 'mapping' && (
          <ActionMapper
            selectedFields={selectedFields}
            setSelectedFields={setSelectedFields}
            selectedBreakdowns={selectedBreakdowns}
            setSelectedBreakdowns={setSelectedBreakdowns}
            error={error}
            setError={setError}
          />
        )}

        {/* Error display (shared between modes) */}
        {error && (
          <div className="mt-4 p-3 bg-red-100 dark:bg-red-800 text-red-800 dark:text-red-100 rounded">
            Error: {error}
          </div>
        )}
      </div>
    </div>
  );
}; 