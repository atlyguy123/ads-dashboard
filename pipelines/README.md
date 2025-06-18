# Pipeline Orchestrator

This folder contains pipeline configurations and scripts for the file-first pipeline orchestrator system.

## How It Works

The orchestrator follows a **file-first pattern** where pipelines are defined through YAML configuration files and executed via Python scripts. The system automatically discovers pipelines by scanning this directory for `.yaml` files.

### Key Features

- **Individual Step Execution**: Run any pipeline step independently, not just full pipeline runs
- **Real-time Status Updates**: WebSocket-based live updates during execution
- **Persistent Status Tracking**: Step completion and testing status saved to `.status.json` files
- **Dependency Management**: Steps can depend on other steps completing first
- **Web UI**: Browser-based interface for pipeline management and execution

## File Structure

Each pipeline follows this structure:

```
pipelines/
├── pipeline_name.yaml           # Pipeline configuration
├── pipeline_name/              # Pipeline scripts folder
│   ├── step1_script.py         # Python script for step1
│   ├── step2_script.py         # Python script for step2
│   └── ...
├── .pipeline_name.status.json  # Auto-generated status file
└── README.md                   # This file
```

## Setting Up a New Pipeline

### 1. Create the YAML Configuration

Create a new `.yaml` file in this directory with your pipeline definition:

```yaml
# my_pipeline.yaml
name: "My Pipeline"
description: "Description of what this pipeline does"
steps:
  - name: "setup"
    script: "setup.py"
    description: "Initialize the pipeline environment"
    
  - name: "process_data"
    script: "process.py" 
    description: "Process the input data"
    depends_on: ["setup"]  # This step waits for 'setup' to complete
    
  - name: "generate_report"
    script: "report.py"
    description: "Generate final report"
    depends_on: ["process_data"]
```

### 2. Create the Scripts Folder

Create a folder with the same name as your YAML file (without extension):

```bash
mkdir my_pipeline
```

### 3. Write Your Python Scripts

Create Python scripts for each step in the pipeline folder. Each script should:

- Be self-contained and executable
- Handle its own error logging
- Exit with code 0 for success, non-zero for failure
- Write any outputs to appropriate locations

Example script (`my_pipeline/setup.py`):

```python
#!/usr/bin/env python3
"""
Setup step for My Pipeline
"""
import os
import sys

def main():
    try:
        print("Starting setup...")
        
        # Your setup logic here
        os.makedirs("output", exist_ok=True)
        
        print("Setup completed successfully")
        return 0
        
    except Exception as e:
        print(f"Setup failed: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())
```

### 4. Test Your Pipeline

1. Start the orchestrator: `python orchestrator/app.py`
2. Open http://localhost:5001/pipelines in your browser
3. Your new pipeline should appear in the list
4. Test individual steps first, then run the full pipeline

## YAML Configuration Reference

### Required Fields

- `name`: Human-readable pipeline name
- `description`: Brief description of the pipeline's purpose
- `steps`: List of pipeline steps

### Step Configuration

Each step in the `steps` array supports:

- `name`: Unique step identifier (required)
- `script`: Python script filename (required)  
- `description`: Human-readable step description (required)
- `depends_on`: List of step names that must complete first (optional)

### Example with Dependencies

```yaml
name: "Data Processing Pipeline"
description: "Fetch, process, and analyze data"
steps:
  - name: "fetch_data"
    script: "fetch.py"
    description: "Download data from external API"
    
  - name: "clean_data" 
    script: "clean.py"
    description: "Clean and validate the data"
    depends_on: ["fetch_data"]
    
  - name: "analyze_data"
    script: "analyze.py" 
    description: "Perform data analysis"
    depends_on: ["clean_data"]
    
  - name: "generate_report"
    script: "report.py"
    description: "Create analysis report"
    depends_on: ["analyze_data"]
```

## Execution Modes

### Individual Step Execution

- Run any step independently via the web UI
- Useful for testing and debugging individual components
- Status tracked independently for each step

### Full Pipeline Execution  

- Executes all steps in dependency order
- Stops on first failure unless configured otherwise
- Provides real-time progress updates

### Step Testing

- Mark steps as "tested" to track validation status
- Helps manage pipeline development and maintenance
- Visual indicators in the web UI

## Status Tracking

The system automatically creates `.{pipeline_name}.status.json` files to track:

- Step completion status
- Step testing status  
- Execution timestamps
- Error information

Example status file:
```json
{
  "fetch_data": {
    "completed": true,
    "tested": true,
    "last_run": "2024-01-15T10:30:00Z"
  },
  "process_data": {
    "completed": false,
    "tested": false,
    "last_run": null
  }
}
```

## Best Practices

### Script Development

1. **Make scripts idempotent**: Safe to run multiple times
2. **Handle errors gracefully**: Provide clear error messages
3. **Use proper exit codes**: 0 for success, non-zero for failure
4. **Log appropriately**: Use print() for normal output, stderr for errors
5. **Document dependencies**: Note any required packages or setup

### Pipeline Design

1. **Break down complex tasks**: Smaller steps are easier to debug and test
2. **Use meaningful names**: Both for pipelines and steps
3. **Define clear dependencies**: Ensure proper execution order
4. **Test incrementally**: Validate each step before building the full pipeline
5. **Document purpose**: Clear descriptions help with maintenance

### Directory Organization

1. **Keep scripts simple**: Focus each script on a single responsibility
2. **Use consistent naming**: Follow a clear naming convention
3. **Group related pipelines**: Consider subfolder organization for large projects
4. **Version control**: Track both YAML configs and Python scripts

## Example Pipelines

This directory includes example pipelines to demonstrate the pattern:

- `example_pipeline/pipeline.yaml`: Basic 3-step data processing pipeline
- `my_first_pipeline/pipeline.yaml`: Simple setup → analyze → report workflow
- `mixpanel_pipeline/pipeline.yaml`: Complete Mixpanel data processing and analysis pipeline

Study these examples to understand the configuration format and script structure.

## Setting Up the Mixpanel Database Pipeline

The `mixpanel_pipeline` demonstrates a comprehensive data processing workflow that includes database migrations, data validation, and user analysis. This pipeline processes Mixpanel analytics data and assigns economic tiers to users.

### Prerequisites

1. Ensure the database directory exists: `mkdir -p database`
2. The pipeline expects a Mixpanel database at `database/mixpanel_data.db`
3. Python packages: `sqlite3`, `json`, `datetime` (built-in modules)

### Pipeline Steps

The Mixpanel pipeline consists of 6 sequential steps:

1. **Download/Update Data** (`01_download_update_data.py`)
   - Checks existing data coverage and downloads missing data
   - Ensures the last 90 days of data are present
   - Creates data cache in `data_cache/mixpanel/`

2. **Ingest Data** (`02_ingest_data.py`)  
   - Performs database schema migrations (adds new columns and tables)
   - Ingests downloaded data without duplicates
   - Sets all users as `valid_user=TRUE` by default

3. **Set ABI Attribution** (`03_set_abi_attribution.py`)
   - Analyzes users for advertising attribution data
   - Sets `has_abi_attribution=TRUE` for users with ABI campaign/adset/ad data

4. **Check Broken Users** (`04_check_broken_users.py`)
   - Identifies users with missing or invalid data
   - Sets `valid_user=FALSE` for users that should be excluded from analysis

5. **Count User Events** (`05_count_user_events.py`)
   - Analyzes user event patterns and activity levels
   - Populates the `fact_user_products` table with user-product relationships
   - Validates user lifecycle patterns

6. **Assign Economic Tier** (`06_assign_economic_tier.py`)
   - Assigns economic tiers (premium/standard/basic/free) based on user behavior
   - Uses revenue, engagement, and product usage as classification criteria

### Database Schema Changes

The pipeline automatically handles these schema migrations:

**New columns added to `fact_mixpanel_user`:**
- `valid_user BOOLEAN DEFAULT TRUE` - Flag for user validity
- `economic_tier TEXT` - Economic classification

**New table created: `fact_user_products`:**
```sql
CREATE TABLE fact_user_products (
    user_product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    distinct_id TEXT NOT NULL REFERENCES fact_mixpanel_user(distinct_id),
    product_id TEXT NOT NULL,
    valid_lifecycle BOOLEAN DEFAULT FALSE,
    UNIQUE (distinct_id, product_id)
);
```

### Running the Pipeline

1. Start the orchestrator: `python orchestrator/app.py`
2. Open http://localhost:5001/pipelines
3. Find "mixpanel_pipeline" in the list
4. Test individual steps first to verify each component works
5. Run the full pipeline once all steps are validated

### Expected Outputs

- Updated database schema with new columns and tables
- User validation flags based on data quality
- ABI attribution flags for advertising analysis  
- Economic tier assignments for user segmentation
- Detailed verification reports at each step

This pipeline serves as a complete example of:
- Database schema migrations in a pipeline
- Multi-step data validation and enrichment
- User segmentation and classification
- Comprehensive error handling and verification

## Troubleshooting

### Pipeline Not Appearing

- Check YAML syntax with a validator
- Ensure the `.yaml` file is in the pipelines directory  
- Restart the orchestrator to refresh pipeline discovery

### Step Execution Failures

- Check script permissions (should be executable)
- Verify script paths in YAML configuration
- Review error output in the web UI or orchestrator logs
- Test scripts manually: `python pipelines/pipeline_name/script_name.py`

### Dependency Issues

- Ensure dependency step names match exactly (case-sensitive)
- Check for circular dependencies 
- Verify dependent steps complete successfully first

## Web Interface

Access the pipeline orchestrator at: http://localhost:5001/pipelines

Features:
- View all discovered pipelines
- Execute individual steps or full pipelines  
- Monitor real-time execution progress
- Mark steps as tested
- View step completion status

---

For questions or issues, check the orchestrator logs or review the example pipelines for reference patterns. 