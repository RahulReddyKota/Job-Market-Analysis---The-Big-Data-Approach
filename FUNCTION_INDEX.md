# Function Index - Quick Navigation Guide

## 1. src/spark/etl_pipeline.py

### Class: JobMarketSparkETL
- **Line 22**: `__init__()` - Initialize Spark ETL pipeline
- **Line 26**: `_create_spark_session()` - Create Spark session with Delta Lake support
- **Line 59**: `setup_delta_lake()` - Initialize Delta Lake tables
- **Line 76**: `process_github_data(batch_size=50)` - Process GitHub Archive data
- **Line 151**: `process_stackoverflow_data()` - Process StackOverflow survey data
- **Line 190**: `process_kaggle_jobs_data()` - Process Kaggle job data
- **Line 228**: `process_bls_data()` - Process BLS employment data
- **Line 271**: `create_silver_layer()` - Create unified Silver layer
- **Line 565**: `create_gold_layer()` - Create ML-ready Gold layer
- **Line 596**: `create_gold_postings_aggregates()` - Create job postings aggregates
- **Line 643**: `create_gold_skills_time_series()` - Create skills time-series data
- **Line 690**: `create_gold_stackoverflow_aggregates()` - Create StackOverflow aggregates
- **Line 719**: `create_gold_company_aggregates()` - Create company aggregates
- **Line 760**: `create_gold_github_aggregates()` - Create GitHub aggregates
- **Line 836**: `create_gold_github_language_monthly_and_linkage()` - Create language trends
- **Line 929**: `create_gold_bls_health_index()` - Create BLS health index
- **Line 952**: `run_full_pipeline()` - Run complete ETL pipeline

### Main Function
- **Line 987**: `main()` - Entry point

---

## 2. src/ingest/comprehensive_data_processor.py

### Class: ComprehensiveDataProcessor
- **Line 25**: `__init__()` - Initialize data processor
- **Line 35**: `copy_kaggle_data()` - Copy Kaggle datasets to raw directory
- **Line 90**: `get_data_summary()` - Get comprehensive data summary
- **Line 135**: `process_stackoverflow_data()` - Process StackOverflow surveys
- **Line 168**: `process_kaggle_job_data()` - Process Kaggle job datasets
- **Line 231**: `create_unified_salary_dataset()` - Create unified salary dataset
- **Line 283**: `run_full_pipeline()` - Run complete processing pipeline

### Main Function
- **Line 311**: `main()` - Entry point

---

## 3. dags/job_market_airflow_dag.py

### Airflow Task Functions
- **Line 35**: `check_data_availability()` - Check if data sources are available
- **Line 61**: `run_spark_etl()` - Run Spark ETL pipeline
- **Line 88**: `run_ml_training()` - Run ML model training
- **Line 115**: `validate_data_quality()` - Validate data quality
  - **Line 124**: `count_layer()` - Helper: Count layer datasets
- **Line 151**: `generate_insights()` - Generate insights and reports
- **Line 207**: `generate_market_report()` - Generate markdown report
  - **Line 215**: `safe_read_parquet()` - Helper: Safe parquet reading
  - **Line 226**: `pick()` - Helper: Pick column from dataframe

---

## 4. src/ml/salary_prediction_model.py

### Class: SalaryPredictionModel
- **Line 29**: `__init__()` - Initialize model
- **Line 38**: `load_data()` - Load salary data for training
- **Line 59**: `prepare_features()` - Prepare features for ML model
- **Line 113**: `train_model()` - Train XGBoost model
- **Line 187**: `save_model()` - Save trained model
- **Line 212**: `load_model()` - Load saved model
- **Line 228**: `predict_salary()` - Predict salary from features

### Main Function
- **Line 256**: `main()` - Entry point for training

---

## 5. src/ml/skill_forecasting.py

- **Line 15**: `load_monthly()` - Load monthly skill demand data
- **Line 29**: `forecast_skill()` - Forecast skill demand using time-series
- **Line 68**: `main()` - Entry point

---

## 6. smart_career_api.py

### Data Loading Functions
- **Line 93**: `load_skills_data()` - Load skills data from parquet
- **Line 102**: `get_role_skills()` - Get skills for a target role

### Recommendation Functions
- **Line 123**: `calculate_skill_priority()` - Calculate skill priority score
- **Line 172**: `get_personalized_recommendations()` - Get personalized career recommendations

### FastAPI Endpoints
- **Line 256**: `@app.on_event("startup")` - Startup event handler
- **Line 262**: `health()` - Health check endpoint
- **Line 266**: `career_recommendations()` - Career recommendations endpoint
- **Line 270**: `predict_salary()` - Salary prediction endpoint

---

## 7. src/api/app.py

### Model Management
- **Line 13**: `get_model()` - Get or load salary prediction model

### FastAPI Endpoints
- **Line 47**: `health()` - Health check endpoint
- **Line 51**: `predict_salary()` - ML salary prediction endpoint
- **Line 137**: `forecast_skills()` - Skill demand forecasting endpoint
- **Line 163**: `career_recommendations()` - Career recommendations endpoint

---

## 8. optimized_streamlit.py

### Utility Functions
- **Line 11**: `console_log()` - Console logging utility
- **Line 25**: `call_career_api()` - Call career recommendations API
- **Line 46**: `call_ml_salary_prediction()` - Call ML salary prediction API
- **Line 69**: `call_skill_forecast()` - Call skill forecasting API
- **Line 90**: `safe_numeric_salary()` - Safe numeric conversion for salaries
- **Line 117**: `convert_from_usd()` - Convert USD to target currency
- **Line 124**: `get_currency_symbol()` - Get currency symbol
- **Line 128**: `convert_to_yearly_usd()` - Convert salary to yearly USD
- **Line 154**: `normalize_salaries_to_yearly_usd()` - Normalize all salaries
- **Line 258**: `is_valid_job_title()` - Validate job title
- **Line 359**: `filter_valid_job_titles()` - Filter valid job titles

### Data Loading Functions
- **Line 370**: `load_skills_demand()` - Load skills demand data
- **Line 382**: `load_unified_salaries()` - Load unified salary data
- **Line 413**: `load_job_postings()` - Load job postings data
- **Line 457**: `load_market_summary()` - Load market summary data

### Main UI Functions
- **Line 495**: `main()` - Main Streamlit app entry point
- **Line 529**: `show_career_planner()` - Career Planner tab
- **Line 575**: `display_recommendations()` - Display recommendations
- **Line 669**: `show_market_overview()` - Market Overview tab
- **Line 783**: `show_skill_analysis()` - Skill Analysis tab
- **Line 929**: `show_salary_intelligence()` - Salary Intelligence tab
- **Line 1180**: `show_realtime_trends()` - Real-time Trends tab
- **Line 1325**: `show_about()` - About tab

---

## Quick Jump Commands (VS Code / Cursor)

Press `Cmd+G` (Mac) or `Ctrl+G` (Windows/Linux) and enter line number to jump directly.

Or use `Cmd+P` (Mac) or `Ctrl+P` (Windows/Linux) and type `:line_number` to jump to a specific line.

