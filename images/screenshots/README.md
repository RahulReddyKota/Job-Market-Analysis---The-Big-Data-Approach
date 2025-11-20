# Screenshots Directory

This directory is for storing screenshots of data ingestion processes and ETL pipelines to display in the Job Market Analysis Tool.

## How to Add Screenshots

1. **Take Screenshots** of your:
   - Data ingestion processes (GitHub, StackOverflow, Kaggle, BLS)
   - ETL pipeline execution (Spark, Delta Lake)
   - Apache Airflow DAGs and workflows
   - Apache Spark processing jobs
   - Any other relevant pipeline visualizations

2. **Save Screenshots** in this directory (`images/screenshots/`)

3. **Use Descriptive Filenames**:
   - `data_ingestion_github.png` - GitHub data ingestion
   - `data_ingestion_stackoverflow.png` - StackOverflow data ingestion
   - `etl_pipeline_spark.png` - Spark ETL pipeline
   - `airflow_dag.png` - Airflow DAG visualization
   - `spark_processing.png` - Spark job execution
   - `delta_lake_storage.png` - Delta Lake storage
   - `mlflow_tracking.png` - MLflow experiment tracking

4. **Supported Formats**: PNG, JPG, JPEG, GIF

5. **Automatic Categorization**: The app will automatically categorize screenshots based on filename:
   - Files with "ingest" or "data" → Data Ingestion section
   - Files with "pipeline" or "etl" → ETL Pipeline section
   - Files with "spark" → Apache Spark section
   - Files with "airflow" → Apache Airflow section
   - Others → Additional Screenshots section

## Viewing Screenshots

After adding screenshots, refresh the "About" tab in the Streamlit app to see them displayed automatically in organized sections.

