#!/usr/bin/env python3
"""
Apache Airflow DAG for Job Market Analysis Pipeline
Orchestrates the complete big data processing workflow
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable

# Default arguments
default_args = {
    'owner': 'job-market-analysis',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'job_market_analysis_pipeline',
    default_args=default_args,
    description='Complete job market data processing pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['big-data', 'job-market', 'etl', 'ml']
)

def check_data_availability():
    """Check if all data sources are available"""
    import os
    from pathlib import Path
    
    data_sources = {
        'github': 'data/raw/github',
        'stackoverflow': 'data/raw/stackoverflow',
        'kaggle': 'data/raw/kaggle',
        'bls': 'data/raw/bls'
    }
    
    available_sources = []
    for source, path in data_sources.items():
        if Path(path).exists() and any(Path(path).iterdir()):
            available_sources.append(source)
            print(f"{source.upper()} data available")
        else:
            print(f"{source.upper()} data not available")
    
    if not available_sources:
        raise Exception("No data sources available!")
    
    print(f"Available data sources: {', '.join(available_sources)}")
    return available_sources

def run_spark_etl():
    """Run Spark ETL pipeline"""
    import subprocess
    import sys
    from pathlib import Path
    
    # Add project root to Python path
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root))
    
    try:
        # Run Spark ETL pipeline
        result = subprocess.run([
            'python', 'src/spark/etl_pipeline.py'
        ], cwd=project_root, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"ERROR: Spark ETL failed: {result.stderr}")
            raise Exception(f"Spark ETL failed: {result.stderr}")
        
        print("Spark ETL pipeline completed successfully")
        print(result.stdout)
        
    except Exception as e:
        print(f"ERROR: Error running Spark ETL: {e}")
        raise

def run_ml_training():
    """Run ML model training"""
    import subprocess
    import sys
    from pathlib import Path
    
    # Add project root to Python path
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root))
    
    try:
        # Run ML training
        result = subprocess.run([
            'python', 'src/ml/salary_prediction_model.py'
        ], cwd=project_root, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"ERROR: ML training failed: {result.stderr}")
            raise Exception(f"ML training failed: {result.stderr}")
        
        print("ML model training completed successfully")
        print(result.stdout)
        
    except Exception as e:
        print(f"ERROR: Error running ML training: {e}")
        raise

def validate_data_quality():
    """Validate data quality after processing"""
    import pandas as pd
    from pathlib import Path
    import os
    
    print("Validating data quality...")
    
    # Helper to count either Parquet mirrors or Delta directories
    def count_layer(human_name: str, parquet_dir: str, delta_dir: str) -> int:
        parquet_path = Path(parquet_dir)
        delta_path = Path(delta_dir)
        parquet_count = 0
        delta_exists = False
        if parquet_path.exists():
            # Count top-level parquet datasets or folders
            parquet_count = len(list(parquet_path.glob("*.parquet")))
            # Also treat subfolders as datasets (Spark writes folders)
            parquet_count += len([p for p in parquet_path.iterdir() if p.is_dir()])
        if delta_path.exists():
            # Delta tables are directories; count immediate children
            delta_exists = any(delta_path.iterdir())
        if parquet_count > 0 or delta_exists:
            print(f"{human_name} layer present (parquet: {parquet_count}, delta: {int(delta_exists)})")
            return parquet_count
        else:
            print(f"ERROR: {human_name} layer not found")
            raise Exception(f"{human_name} layer not found")

    # Validate layers
    count_layer("Bronze", "data/bronze", "data/delta/bronze")
    count_layer("Silver", "data/silver", "data/delta/silver")
    count_layer("Gold",   "data/gold",   "data/delta/gold")
    
    print("Data quality validation passed")

def generate_insights():
    """Generate insights and reports"""
    import pandas as pd
    from pathlib import Path
    import json
    import os
    
    print("Generating insights and reports...")
    
    # Load unified salary data
    # Prefer Parquet mirror; if missing, try folder; else try Delta via pyspark
    parquet_file = Path("data/silver/unified_salaries.parquet")
    parquet_folder = Path("data/silver/unified_salaries")
    delta_folder = Path("data/delta/silver/unified_salaries")
    df = None
    if parquet_file.exists():
        df = pd.read_parquet(parquet_file)
    elif parquet_folder.exists():
        # Read all parquet parts in folder
        try:
            df = pd.read_parquet(str(parquet_folder))
        except Exception:
            pass
    elif delta_folder.exists():
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName("airflow-insights").getOrCreate()
            sdf = spark.read.format("delta").load(str(delta_folder))
            df = sdf.toPandas()
            spark.stop()
        except Exception:
            df = None
    if df is not None and len(df) > 0:
        
        insights = {
            'total_records': len(df),
            'avg_salary': float(df['salary_amount'].mean()) if 'salary_amount' in df.columns else 0,
            'median_salary': float(df['salary_amount'].median()) if 'salary_amount' in df.columns else 0,
            'currency_distribution': df['currency'].value_counts().to_dict() if 'currency' in df.columns else {},
            'period_distribution': df['period'].value_counts().to_dict() if 'period' in df.columns else {}
        }
        
        # Save insights
        insights_file = Path("reports/daily_insights.json")
        insights_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(insights_file, 'w') as f:
            json.dump(insights, f, indent=2)
        
        print(f"Insights saved to {insights_file}")
        print(f"Total records: {insights['total_records']}")
        print(f"Average salary: ${insights['avg_salary']:,.2f}")
        
    else:
        print("ERROR: Salary data not found for insights generation")

def generate_market_report():
    """Generate markdown report summarizing trends and salaries"""
    import pandas as pd
    from pathlib import Path

    report_path = Path('reports/job_market_analysis.md')
    report_path.parent.mkdir(parents=True, exist_ok=True)

    def safe_read_parquet(p: Path):
        try:
            return pd.read_parquet(p) if p.exists() else None
        except Exception:
            return None

    posts = safe_read_parquet(Path('data/bronze/kaggle/job_postings'))
    salaries = safe_read_parquet(Path('data/silver/unified_salaries'))

    lines = ['# Job Market Analysis', '', 'Generated (scheduled) by Airflow.', '']

    def pick(df, cols):
        for c in cols:
            if c in df.columns:
                return c
        return None

    if posts is not None and len(posts) > 0:
        title_col = pick(posts, ['title','job_title','position','role','posting_title'])
        skills_col = pick(posts, ['skills','key_skills','skills_mentioned','tags','tech_stack'])
        loc_col = pick(posts, ['location','city','state','country','job_location'])
        date_col = pick(posts, ['posted_date','date_posted','posting_date','created_at','created','post_date'])
        base = posts.copy()
        if date_col:
            base[date_col] = pd.to_datetime(base[date_col], errors='coerce')
            cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=30)
            recent = base[base[date_col] >= cutoff]
            if len(recent) > 100:
                base = recent
        lines.append('## Trending Job Titles (last 30 days if available)')
        if title_col:
            top_titles = base[title_col].astype(str).str.lower().str.strip().value_counts().head(15)
            for t,c in top_titles.items():
                lines.append(f'- {t}: {c} postings')
        else:
            lines.append('- No job title column found')
        lines.append('')
        lines.append('## Most Demanded Skills')
        if skills_col:
            ser = base[skills_col].dropna().astype(str).str.lower().str.replace('/',',').str.replace(';',',')
            top_sk = ser.str.split(',').explode().str.strip()
            top_sk = top_sk[top_sk.str.len()>0].value_counts().head(25)
            for s,c in top_sk.items():
                lines.append(f'- {s}: {c}')
        else:
            lines.append('- No skills column found')
        lines.append('')
        lines.append('## Hot Locations (by posting volume)')
        if loc_col:
            top_loc = base[loc_col].astype(str).str.strip().str.lower().value_counts().head(15)
            for l,c in top_loc.items():
                lines.append(f'- {l}: {c} postings')
        else:
            lines.append('- No location column found')
        lines.append('')
    else:
        lines += ['## Trending Job Titles / Skills / Locations', '- Job postings dataset not found', '']

    lines.append('## Salary Summary (Silver)')
    if salaries is not None and len(salaries) > 0 and 'salary_amount' in salaries.columns:
        salaries = salaries.copy(); salaries['salary_amount'] = pd.to_numeric(salaries['salary_amount'], errors='coerce')
        sal = salaries['salary_amount'].dropna()
        lines.append(f'- Records: {len(salaries):,}')
        if len(sal)>0:
            med = sal.median(); q1 = sal.quantile(0.25); q3 = sal.quantile(0.75)
            lines.append(f'- Median salary: ${med:,.0f}')
            lines.append(f'- IQR: ${q1:,.0f} – ${q3:,.0f}')
            if 'currency' in salaries.columns:
                cur_med = salaries.dropna(subset=['salary_amount']).groupby('currency')['salary_amount'].median().sort_values(ascending=False).head(10)
                lines.append('- Median by currency:')
                for cur,val in cur_med.items():
                    lines.append(f'  - {cur}: ${val:,.0f}')
            if 'period' in salaries.columns:
                per_med = salaries.dropna(subset=['salary_amount']).groupby('period')['salary_amount'].median().sort_values(ascending=False)
                lines.append('- Median by period:')
                for per,val in per_med.items():
                    lines.append(f'  - {per}: ${val:,.0f}')
        else:
            lines.append('- No numeric salaries after coercion')
    else:
        lines.append('- Salary dataset not found or missing salary_amount')

    Path('reports').mkdir(exist_ok=True)
    Path('reports/job_market_analysis.md').write_text('\n'.join(lines))
    print('Report updated')

# Define tasks
check_data_task = PythonOperator(
    task_id='check_data_availability',
    python_callable=check_data_availability,
    dag=dag
)

spark_etl_task = PythonOperator(
    task_id='run_spark_etl',
    python_callable=run_spark_etl,
    dag=dag
)

ml_training_task = PythonOperator(
    task_id='run_ml_training',
    python_callable=run_ml_training,
    dag=dag
)

validate_quality_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag
)

generate_insights_task = PythonOperator(
    task_id='generate_insights',
    python_callable=generate_insights,
    dag=dag
)

market_report_task = PythonOperator(
    task_id='generate_market_report',
    python_callable=generate_market_report,
    dag=dag
)

# Start API server task
start_api_task = BashOperator(
    task_id='start_api_server',
    bash_command='cd /Users/manikantasirumalla/Desktop/job-market-analysis && source .venv/bin/activate && make api',
    dag=dag
)

# Define task dependencies
check_data_task >> spark_etl_task >> validate_quality_task >> ml_training_task >> generate_insights_task >> market_report_task >> start_api_task

# Optional: Add data source sensors
github_sensor = FileSensor(
    task_id='wait_for_github_data',
    filepath='data/raw/github/',
    fs_conn_id='fs_default',
    poke_interval=60,
    timeout=300,
    dag=dag
)

stackoverflow_sensor = FileSensor(
    task_id='wait_for_stackoverflow_data',
    filepath='data/raw/stackoverflow/',
    fs_conn_id='fs_default',
    poke_interval=60,
    timeout=300,
    dag=dag
)

# Alternative workflow with sensors
# github_sensor >> stackoverflow_sensor >> check_data_task >> spark_etl_task >> validate_quality_task >> ml_training_task >> generate_insights_task
