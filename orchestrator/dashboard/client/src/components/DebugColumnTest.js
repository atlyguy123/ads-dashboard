import React, { useEffect, useState } from 'react';
import { dashboardApi } from '../services/dashboardApi';

// Debug component to test estimated_revenue_adjusted visibility
const DebugColumnTest = () => {
  const [testData, setTestData] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    console.log('🔥 DEBUG COMPONENT - Starting column test...');
    
    const testApiCall = async () => {
      try {
        const params = {
          start_date: '2025-06-10',
          end_date: '2025-06-10',
          breakdown: 'all',
          group_by: 'campaign'
        };
        
        console.log('🔥 DEBUG COMPONENT - Calling API with params:', params);
        const response = await dashboardApi.getAnalyticsData(params);
        console.log('🔥 DEBUG COMPONENT - API Response:', response);
        
        if (response.success && response.data) {
          setTestData(response.data);
          
          const firstRecord = response.data[0];
          console.log('🔥 DEBUG COMPONENT - First record analysis:');
          console.log('  - Total fields:', Object.keys(firstRecord).length);
          console.log('  - Has estimated_revenue_adjusted:', 'estimated_revenue_adjusted' in firstRecord);
          console.log('  - estimated_revenue_adjusted value:', firstRecord.estimated_revenue_adjusted);
          console.log('  - All revenue fields:', Object.keys(firstRecord).filter(k => k.includes('revenue')));
        } else {
          setError('API call failed or no data');
        }
      } catch (err) {
        console.error('🔥 DEBUG COMPONENT - Error:', err);
        setError(err.message);
      }
    };
    
    testApiCall();
  }, []);

  return (
    <div style={{ 
      position: 'fixed', 
      top: '10px', 
      right: '10px', 
      background: 'white', 
      border: '2px solid red', 
      padding: '20px',
      zIndex: 9999,
      maxWidth: '400px'
    }}>
      <h3 style={{ color: 'red' }}>🔥 DEBUG: estimated_revenue_adjusted Test</h3>
      
      {error && (
        <div style={{ color: 'red' }}>
          <strong>Error:</strong> {error}
        </div>
      )}
      
      {testData && (
        <div>
          <p><strong>Records:</strong> {testData.length}</p>
          {testData.length > 0 && (
            <div>
              <p><strong>First Record Fields:</strong></p>
              <ul style={{ fontSize: '12px' }}>
                {Object.keys(testData[0])
                  .filter(key => key.includes('revenue'))
                  .map(key => (
                    <li key={key} style={{ 
                      color: key === 'estimated_revenue_adjusted' ? 'green' : 'black',
                      fontWeight: key === 'estimated_revenue_adjusted' ? 'bold' : 'normal'
                    }}>
                      {key}: {testData[0][key]}
                    </li>
                  ))}
              </ul>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default DebugColumnTest; 