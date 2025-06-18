import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// Get current directory
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Parse command line arguments
const args = process.argv.slice(2);
const outputFile = args.find(arg => arg.startsWith('--output='))?.split('=')[1] || 'dashboard_debug.md';

// Target directories for dashboard debugging
const targetPaths = [
  './orchestrator/app.py',
  './orchestrator/dashboard/api',
  './orchestrator/dashboard/services',
  './orchestrator/dashboard/client/src/pages',
  './orchestrator/dashboard/client/src/services',
  './orchestrator/dashboard/client/src/components/dashboard',
  './orchestrator/dashboard/client/src/App.js',
  './database',
  './utils'
];

// Critical individual files for dashboard debugging
const criticalFiles = [
  'orchestrator/app.py',
  'orchestrator/dashboard/api/dashboard_routes.py',
  'orchestrator/dashboard/services/analytics_query_service.py',
  'orchestrator/dashboard/services/dashboard_service.py',
  'orchestrator/dashboard/client/src/App.js',
  'orchestrator/dashboard/client/src/pages/Dashboard.js',
  'orchestrator/dashboard/client/src/services/dashboardApi.js',
  'orchestrator/dashboard/client/src/services/api.js',
  'orchestrator/dashboard/client/src/components/dashboard/DashboardGrid.js',
  'orchestrator/dashboard/client/src/components/dashboard/AnalyticsPipelineControls.jsx',
  'orchestrator/dashboard/client/src/components/dashboard/RefreshPipelineControls.jsx',
  'orchestrator/dashboard/client/package.json',
  'database/schema.sql',
  'utils/database_utils.py',
  'requirements.txt',
  'start_orchestrator.sh'
];

// Project info
const projectName = "Dashboard Debug Analysis";
const projectDescription = "Critical files for debugging the dashboard MixPanel data issue where dashboard shows zeros instead of retrieving MixPanel data correctly.";

// File patterns to include
const includeFilePatterns = [
  /.*\.py$/i,     // Python files
  /.*\.js$/i,     // JavaScript files
  /.*\.jsx$/i,    // JSX files
  /.*\.sql$/i,    // SQL files
  /.*\.json$/i,   // JSON files
  /.*\.sh$/i,     // Shell scripts
  /.*\.txt$/i     // Text files like requirements.txt
];

// Files to explicitly ignore
const ignorePatterns = [
  /node_modules\/.*/i,
  /venv\/.*/i,
  /\.git\/.*/i,
  /__pycache__\/.*/i,
  /.*\.pyc$/i,
  /build\/.*/i,
  /dist\/.*/i,
  /static\/.*/i,
  /.*\.db$/i,
  /.*\.db-shm$/i,
  /.*\.db-wal$/i
];

// Get a simplified file tree for critical directories
function getFileTree() {
  let result = 'Critical Dashboard Files:\n';
  
  // Group files by category
  const categories = {
    'Backend (Python)': [],
    'Frontend (React)': [],
    'Database & Utils': [],
    'Configuration': []
  };
  
  criticalFiles.forEach(filePath => {
    if (fs.existsSync(filePath)) {
      if (filePath.includes('.py') || filePath.includes('orchestrator/app.py')) {
        categories['Backend (Python)'].push(filePath);
      } else if (filePath.includes('client/src') || filePath.includes('.js') || filePath.includes('.jsx')) {
        categories['Frontend (React)'].push(filePath);
      } else if (filePath.includes('database') || filePath.includes('utils')) {
        categories['Database & Utils'].push(filePath);
      } else {
        categories['Configuration'].push(filePath);
      }
    }
  });
  
  Object.entries(categories).forEach(([category, files]) => {
    if (files.length > 0) {
      result += `\n${category}:\n`;
      files.forEach((file, index) => {
        const isLast = index === files.length - 1;
        result += `${isLast ? '└── ' : '├── '}${file}\n`;
      });
    }
  });
  
  return result;
}

// Get file content
function getFileContent(filePath) {
  try {
    const content = fs.readFileSync(filePath, 'utf8');
    const extension = path.extname(filePath).substring(1);
    
    // Truncate very large files
    if (content.length > 50000) {
      const truncated = content.substring(0, 50000);
      return `\`\`\`${extension}\n${truncated}\n\n... [File truncated - showing first 50,000 characters]\n\`\`\``;
    }
    
    return `\`\`\`${extension}\n${content}\n\`\`\``;
  } catch (error) {
    console.log(`Error reading ${filePath}: ${error.message}`);
    return null;
  }
}

// Process critical files
function processCriticalFiles() {
  const result = {};
  
  criticalFiles.forEach(filePath => {
    if (fs.existsSync(filePath)) {
      const content = getFileContent(filePath);
      if (content) {
        result[filePath] = content;
      }
    } else {
      console.log(`Warning: Critical file not found: ${filePath}`);
    }
  });
  
  return result;
}

// Generate prompt
function generatePrompt() {
  console.log('Generating file tree...');
  const fileTree = getFileTree();
  console.log('File tree generated');
  
  console.log('Processing critical files...');
  const files = processCriticalFiles();
  console.log(`Processing ${Object.keys(files).length} files completed`);
  
  let prompt = `# ${projectName}\n\n`;
  prompt += `## Project Description\n\n${projectDescription}\n\n`;
  prompt += `## Problem Summary\n\nThe dashboard is completely non-functional. While metadata imports properly, MixPanel data is completely ignored, showing zeros for all MixPanel-related metrics instead of retrieving data correctly. The issue involves:\n\n`;
  prompt += `- Schema misalignment in analytics_query_service.py\n`;
  prompt += `- Incorrect SQL queries using non-existent columns\n`;
  prompt += `- Wrong attribution data sources\n`;
  prompt += `- Multi-level aggregation issues\n\n`;
  prompt += `## File Structure\n\`\`\`\n${fileTree}\`\`\`\n\n`;
  
  // Organize files by category for better analysis
  const backendFiles = {};
  const frontendFiles = {};
  const databaseFiles = {};
  const configFiles = {};
  
  Object.entries(files).forEach(([filePath, content]) => {
    if (filePath.includes('.py') && !filePath.includes('database')) {
      backendFiles[filePath] = content;
    } else if (filePath.includes('client/src') || filePath.includes('.js') || filePath.includes('.jsx')) {
      frontendFiles[filePath] = content;
    } else if (filePath.includes('database') || filePath.includes('utils') || filePath.includes('.sql')) {
      databaseFiles[filePath] = content;
    } else {
      configFiles[filePath] = content;
    }
  });
  
  // Helper function to add files to output
  function addFilesToOutput(filesObj, title) {
    if (Object.keys(filesObj).length === 0) return '';
    
    let output = `## ${title}\n\n`;
    
    // Sort entries
    const sortedEntries = Object.entries(filesObj).sort((a, b) => a[0].localeCompare(b[0]));
    
    sortedEntries.forEach(([filePath, content]) => {
      output += `### ${filePath}\n${content}\n\n`;
    });
    
    return output;
  }
  
  // Add files organized by category
  prompt += addFilesToOutput(backendFiles, 'Backend Files (Python)');
  prompt += addFilesToOutput(frontendFiles, 'Frontend Files (React/JavaScript)');
  prompt += addFilesToOutput(databaseFiles, 'Database & Utility Files');
  prompt += addFilesToOutput(configFiles, 'Configuration Files');
  
  prompt += `## Key Issues Identified\n\n`;
  prompt += `1. **Schema Misalignment**: analytics_query_service.py uses incorrect column names and table relationships\n`;
  prompt += `2. **Attribution Data**: Trying to use empty attribution fields in events table instead of mixpanel_user table\n`;
  prompt += `3. **Multi-Level Aggregation**: Campaign/adset level queries fail to map to constituent ads\n`;
  prompt += `4. **SQL NULL Handling**: Missing COALESCE() for SUM() aggregations\n\n`;
  
  prompt += `## Analysis Instructions\n\n`;
  prompt += `Please analyze these files to understand the complete data flow from frontend Dashboard.js → dashboardApi.js → dashboard_routes.py → analytics_query_service.py and identify any remaining issues with MixPanel data retrieval.\n\n`;
  prompt += `Focus on:\n`;
  prompt += `- Database schema alignment between queries and actual table structure\n`;
  prompt += `- Attribution data flow from mixpanel_user to aggregated metrics\n`;
  prompt += `- API response structure and frontend data handling\n`;
  prompt += `- Error handling and debugging capabilities\n`;
  
  return prompt;
}

// Main
console.log(`Starting to generate dashboard debug documentation...`);
const prompt = generatePrompt();
fs.writeFileSync(outputFile, prompt);
console.log(`Debug documentation generated and saved to ${outputFile}`); 