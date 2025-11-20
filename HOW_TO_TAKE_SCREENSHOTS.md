# How to Take Screenshots for Presentation

This guide explains how to capture screenshots of the backend data ingestion and pipeline processes for your presentation.

## 1. Data Ingestion Screenshots

### GitHub Archive Data Ingestion
```bash
# Run the ingestion script
python src/ingest/download_gharchive.py --date 2024-01-01

# Take screenshots of:
- Terminal output showing file downloads
- Progress bars and file counts
- Data directory structure (ls -lh data/raw/github/)
- File sizes and counts
```

### StackOverflow Data Ingestion
```bash
# Run the ingestion script
python src/ingest/download_stackoverflow.py

# Take screenshots of:
- Download progress
- Survey file listings
- Data processing output
```

### Kaggle Data Ingestion
```bash
# Run the ingestion script
python src/ingest/comprehensive_data_processor.py

# Take screenshots of:
- CSV file downloads
- Data processing logs
- Record counts
```

### BLS Data Ingestion
```bash
# Run the ingestion script
python src/ingest/download_bls.py

# Take screenshots of:
- API calls and responses
- Data file creation
```

## 2. Apache Spark ETL Pipeline Screenshots

### Running Spark ETL
```bash
# Start Spark session and run ETL
python src/spark/etl_pipeline.py

# Take screenshots of:
- Spark session initialization
- Job execution progress
- Task completion status
- Data processing metrics
- Output file creation
```

### Spark Web UI (if available)
- Open Spark Web UI (usually http://localhost:4040)
- Screenshot:
  - Jobs tab showing completed jobs
  - Stages tab with execution details
  - Storage tab showing cached data
  - Environment tab showing configuration

## 3. Apache Airflow Screenshots

### Access Airflow Web UI
```bash
# Start Airflow (if not running)
airflow webserver --port 8080

# Open browser: http://localhost:8080
```

### Screenshots to Take:
1. **DAG Graph View**: 
   - Show the complete workflow
   - Highlight data flow from ingestion to gold layer
   
2. **Tree View**:
   - Show task execution history
   - Success/failure status
   
3. **Task Instance Details**:
   - Logs showing data processing
   - Execution times
   - Data volumes processed

4. **DAG Code View**:
   - Show the pipeline definition

## 4. Delta Lake Screenshots

### Delta Lake Operations
```bash
# Run Delta Lake operations
python -c "from delta import DeltaTable; ..."

# Take screenshots of:
- Delta table creation
- Version history
- Data statistics
```

## 5. MLflow Screenshots

### Access MLflow UI
```bash
# Start MLflow UI
mlflow ui --port 5000

# Open browser: http://localhost:5000
```

### Screenshots to Take:
1. **Experiments List**: Show all ML experiments
2. **Experiment Details**: 
   - Parameters used
   - Metrics (MSE, R2, etc.)
   - Model artifacts
3. **Model Comparison**: Compare different model runs
4. **Model Registry**: If models are registered

## 6. Data Lake Structure Screenshots

### Directory Structure
```bash
# Show data lake structure
tree -L 3 data/ -h
# or
du -sh data/*/
```

### Screenshots to Take:
- Raw layer directory structure
- Bronze layer with parquet files
- Silver layer unified datasets
- Gold layer ML-ready data
- File sizes and counts

## 7. Terminal/Command Line Screenshots

### Useful Commands to Run and Screenshot:
```bash
# Data volume summary
du -sh data/raw/* data/bronze/* data/silver/* data/gold/*

# File counts
find data/raw -type f | wc -l
find data/bronze -type f | wc -l

# Show sample data
head -20 data/bronze/job_postings/part-00000*.parquet | less

# Spark job execution
python src/spark/etl_pipeline.py 2>&1 | tee etl_output.log
```

## 8. Code Screenshots

### Key Files to Screenshot:
- `src/spark/etl_pipeline.py` - Main ETL pipeline code
- `dags/job_market_airflow_dag.py` - Airflow DAG definition
- `src/ingest/comprehensive_data_processor.py` - Data ingestion
- `src/etl/bronze_to_silver.py` - Silver layer processing
- `src/etl/silver_to_gold.py` - Gold layer processing

## 9. Data Processing Logs

### Run with Verbose Output
```bash
# Run ETL with detailed logging
python src/spark/etl_pipeline.py --verbose

# Screenshot:
- Processing statistics
- Record counts at each stage
- Error handling (if any)
- Completion messages
```

## 10. Quick Screenshot Guide

### On macOS:
- **Full Screen**: `Cmd + Shift + 3`
- **Selected Area**: `Cmd + Shift + 4`
- **Window**: `Cmd + Shift + 4`, then press `Space`

### On Linux:
- Use `gnome-screenshot` or `scrot`
- Or use `Print Screen` key

### On Windows:
- `Windows + Shift + S` for Snipping Tool
- Or `Print Screen` key

## Recommended Screenshot Sequence for Presentation

1. **Data Sources Overview** (5-10 screenshots)
   - GitHub Archive download
   - StackOverflow survey download
   - Kaggle data download
   - BLS data download
   - Data volume summary

2. **ETL Pipeline Execution** (5-8 screenshots)
   - Spark session start
   - Bronze layer processing
   - Silver layer processing
   - Gold layer processing
   - Job completion summary

3. **Airflow Orchestration** (3-5 screenshots)
   - DAG graph view
   - Task execution tree
   - Successful run completion

4. **Data Lake Structure** (3-4 screenshots)
   - Raw layer structure
   - Bronze layer files
   - Silver layer unified data
   - Gold layer ML features

5. **MLflow Tracking** (2-3 screenshots)
   - Experiment list
   - Model metrics
   - Model artifacts

## Tips for Good Screenshots

1. **Clear Terminal**: Use a clean terminal with readable font
2. **Highlight Important Info**: Use terminal highlighting or annotations
3. **Show Progress**: Capture progress bars and completion messages
4. **Include Context**: Show file paths, timestamps, and data volumes
5. **Consistent Format**: Use same terminal size and font for consistency
6. **Remove Sensitive Data**: Blur any API keys or credentials

## Example Screenshot Workflow

```bash
# 1. Start with clean terminal
clear

# 2. Show data directory structure
tree -L 2 data/ -h

# 3. Run ingestion (take screenshot during execution)
python src/ingest/download_gharchive.py --date 2024-01-01

# 4. Show results
ls -lh data/raw/github/ | head -20

# 5. Run ETL pipeline
python src/spark/etl_pipeline.py

# 6. Show processed data
ls -lh data/bronze/ data/silver/ data/gold/

# 7. Open Airflow UI and screenshot DAG
# 8. Open MLflow UI and screenshot experiments
```

Save all screenshots in a dedicated folder for easy access during your presentation!

