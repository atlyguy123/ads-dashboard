import React, { useState, useRef, useEffect } from 'react';
import { ChevronDown, ChevronUp, Check } from 'lucide-react';

const MultiSelectDropdown = ({ 
  label, 
  options = [], 
  selectedValues = [], 
  onChange, 
  placeholder = "Select options...",
  className = ""
}) => {
  const [isOpen, setIsOpen] = useState(false);
  const dropdownRef = useRef(null);

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setIsOpen(false);
      }
    };

    if (isOpen) {
      document.addEventListener('mousedown', handleClickOutside);
      return () => {
        document.removeEventListener('mousedown', handleClickOutside);
      };
    }
  }, [isOpen]);

  const handleToggle = () => {
    setIsOpen(!isOpen);
  };

  const handleOptionToggle = (value) => {
    const newSelectedValues = selectedValues.includes(value)
      ? selectedValues.filter(v => v !== value)
      : [...selectedValues, value];
    onChange(newSelectedValues);
  };

  const handleSelectAll = () => {
    if (selectedValues.length === options.length) {
      onChange([]);
    } else {
      onChange([...options]);
    }
  };

  const getDisplayText = () => {
    if (selectedValues.length === 0) {
      return placeholder;
    }
    if (selectedValues.length === 1) {
      return selectedValues[0];
    }
    if (selectedValues.length === options.length) {
      return "All selected";
    }
    return `${selectedValues.length} selected`;
  };

  return (
    <div className={`relative ${className}`} ref={dropdownRef}>
      {label && (
        <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
          {label}
        </label>
      )}
      
      {/* Dropdown Button */}
      <button
        type="button"
        onClick={handleToggle}
        className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent flex items-center justify-between"
      >
        <span className={selectedValues.length === 0 ? "text-gray-500 dark:text-gray-400" : ""}>
          {getDisplayText()}
        </span>
        {isOpen ? (
          <ChevronUp className="h-4 w-4 text-gray-400" />
        ) : (
          <ChevronDown className="h-4 w-4 text-gray-400" />
        )}
      </button>

      {/* Dropdown Menu */}
      {isOpen && (
        <div className="absolute z-50 w-full mt-1 bg-white dark:bg-gray-700 border border-gray-300 dark:border-gray-600 rounded-md shadow-lg max-h-60 overflow-y-auto">
          {/* Select All Button */}
          {options.length > 1 && (
            <div className="p-2 border-b border-gray-200 dark:border-gray-600">
              <button
                type="button"
                onClick={handleSelectAll}
                className="w-full text-left px-2 py-1 text-sm text-blue-600 dark:text-blue-400 hover:bg-gray-100 dark:hover:bg-gray-600 rounded"
              >
                {selectedValues.length === options.length ? 'Deselect All' : 'Select All'}
              </button>
            </div>
          )}
          
          {/* Options */}
          <div className="p-2 space-y-1">
            {options.map((option) => (
              <label
                key={option}
                className="flex items-center space-x-2 px-2 py-1 hover:bg-gray-100 dark:hover:bg-gray-600 rounded cursor-pointer"
              >
                <div className="relative">
                  <input
                    type="checkbox"
                    checked={selectedValues.includes(option)}
                    onChange={() => handleOptionToggle(option)}
                    className="sr-only"
                  />
                  <div className={`w-4 h-4 border-2 rounded flex items-center justify-center ${
                    selectedValues.includes(option)
                      ? 'bg-blue-600 border-blue-600'
                      : 'border-gray-300 dark:border-gray-500 bg-white dark:bg-gray-700'
                  }`}>
                    {selectedValues.includes(option) && (
                      <Check className="h-3 w-3 text-white" />
                    )}
                  </div>
                </div>
                <span className="text-sm text-gray-900 dark:text-gray-100">
                  {option}
                </span>
              </label>
            ))}
          </div>
          
          {/* No options message */}
          {options.length === 0 && (
            <div className="p-4 text-center text-sm text-gray-500 dark:text-gray-400">
              No options available
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default MultiSelectDropdown; 