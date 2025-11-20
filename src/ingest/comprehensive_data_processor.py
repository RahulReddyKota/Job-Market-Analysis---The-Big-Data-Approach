#!/usr/bin/env python3
"""
Comprehensive Data Processor for Job Market Analysis
Processes all available datasets: StackOverflow, BLS, GitHub Archive, and Kaggle datasets
"""

import shutil
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.common.paths import RAW_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR
from src.common.logs import get_logger

logger = get_logger("ingest.comprehensive")

class ComprehensiveDataProcessor:
    """Process all job market datasets into a unified structure"""
    
    def __init__(self):
        self.raw_dir = RAW_DIR
        self.bronze_dir = BRONZE_DIR
        self.silver_dir = SILVER_DIR
        self.gold_dir = GOLD_DIR
        
        # Create all necessary directories
        for dir_path in [self.raw_dir, self.bronze_dir, self.silver_dir, self.gold_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    def copy_kaggle_data(self):
        """Copy all Kaggle datasets to raw directory"""
        logger.info("Copying Kaggle datasets...")
        
        source_dir = Path("/Users/manikantasirumalla/Desktop/untitled folder/Kaggle")
        target_dir = self.raw_dir / "kaggle"
        target_dir.mkdir(parents=True, exist_ok=True)
        
        # Main datasets
        main_files = [
            "DataAnalyst.csv",
            "dice_com-job_us_sample.csv", 
            "job_descriptions.csv",
            "linkdin_Job_data.xlsx - Sheet1.csv"
        ]
        
        copied_files = []
        for file_name in main_files:
            source_file = source_dir / file_name
            if source_file.exists():
                target_file = target_dir / file_name
                shutil.copy2(source_file, target_file)
                copied_files.append(file_name)
                logger.info(f"Copied {file_name}")
        
        # Archive-2 datasets (structured job data)
        archive2_dir = source_dir / "archive-2"
        if archive2_dir.exists():
            archive2_target = target_dir / "archive-2"
            archive2_target.mkdir(exist_ok=True)
            
            # Copy all CSV files from archive-2
            for csv_file in archive2_dir.rglob("*.csv"):
                rel_path = csv_file.relative_to(archive2_dir)
                target_file = archive2_target / rel_path
                target_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(csv_file, target_file)
                copied_files.append(f"archive-2/{rel_path}")
                logger.info(f"Copied archive-2/{rel_path}")
        
        # Archive-3 datasets (Data Science jobs)
        archive3_dir = source_dir / "archive-3"
        if archive3_dir.exists():
            archive3_target = target_dir / "archive-3"
            archive3_target.mkdir(exist_ok=True)
            
            for csv_file in archive3_dir.glob("*.csv"):
                target_file = archive3_target / csv_file.name
                shutil.copy2(csv_file, target_file)
                copied_files.append(f"archive-3/{csv_file.name}")
                logger.info(f"Copied archive-3/{csv_file.name}")
        
        logger.info(f"Kaggle data copy complete. {len(copied_files)} files copied")
        return copied_files
    
    def get_data_summary(self):
        """Get comprehensive summary of all available data"""
        logger.info("Comprehensive Data Summary:")
        
        total_size = 0
        total_files = 0
        
        # StackOverflow data
        so_dir = self.raw_dir / "stackoverflow"
        if so_dir.exists():
            so_files = list(so_dir.glob("*.csv"))
            so_size = sum(f.stat().st_size for f in so_files) / (1024**2)
            total_size += so_size
            total_files += len(so_files)
            logger.info(f"  StackOverflow Surveys: {len(so_files)} files, {so_size:.1f} MB")
        
        # BLS data
        bls_dir = self.raw_dir / "bls"
        if bls_dir.exists():
            bls_files = list(bls_dir.glob("*.xlsx"))
            bls_size = sum(f.stat().st_size for f in bls_files) / (1024**2)
            total_size += bls_size
            total_files += len(bls_files)
            logger.info(f"  BLS Data: {len(bls_files)} files, {bls_size:.1f} MB")
        
        # GitHub Archive data
        github_dir = self.raw_dir / "github"
        if github_dir.exists():
            github_files = list(github_dir.glob("*.json.gz"))
            github_size = sum(f.stat().st_size for f in github_files) / (1024**3)
            total_size += github_size * 1024  # Convert to MB
            total_files += len(github_files)
            logger.info(f"  GitHub Archive: {len(github_files)} files, {github_size:.2f} GB")
        
        # Kaggle data
        kaggle_dir = self.raw_dir / "kaggle"
        if kaggle_dir.exists():
            kaggle_files = list(kaggle_dir.rglob("*.csv")) + list(kaggle_dir.rglob("*.xlsx"))
            kaggle_size = sum(f.stat().st_size for f in kaggle_files) / (1024**2)
            total_size += kaggle_size
            total_files += len(kaggle_files)
            logger.info(f"  Kaggle Job Data: {len(kaggle_files)} files, {kaggle_size:.1f} MB")
        
        logger.info(f"  TOTAL: {total_files} files, {total_size/1024:.2f} GB")
    
    def process_stackoverflow_data(self):
        """Process StackOverflow survey data into bronze layer"""
        logger.info("Processing StackOverflow data...")
        
        so_dir = self.raw_dir / "stackoverflow"
        bronze_so_dir = self.bronze_dir / "stackoverflow"
        bronze_so_dir.mkdir(parents=True, exist_ok=True)
        
        survey_files = list(so_dir.glob("survey_*.csv"))
        
        for survey_file in survey_files:
            year = survey_file.stem.split('_')[1]
            logger.info(f"  Processing {year} survey...")
            
            try:
                # Read the survey data
                df = pd.read_csv(survey_file)
                
                # Add year column
                df['survey_year'] = int(year)
                
                # Basic cleaning
                df = df.dropna(how='all')  # Remove completely empty rows
                
                # Save to bronze layer
                bronze_file = bronze_so_dir / f"survey_{year}_bronze.csv"
                df.to_csv(bronze_file, index=False)
                
                logger.info(f"  Processed {len(df)} responses for {year}")
                
            except Exception as e:
                logger.error(f"   Error processing {year}: {e}")
    
    def process_kaggle_job_data(self):
        """Process Kaggle job datasets into bronze layer"""
        logger.info("Processing Kaggle job data...")
        
        kaggle_dir = self.raw_dir / "kaggle"
        bronze_kaggle_dir = self.bronze_dir / "kaggle"
        bronze_kaggle_dir.mkdir(parents=True, exist_ok=True)
        
        # Process main job datasets
        main_datasets = [
            "DataAnalyst.csv",
            "dice_com-job_us_sample.csv",
            "job_descriptions.csv"
        ]
        
        for dataset in main_datasets:
            source_file = kaggle_dir / dataset
            if source_file.exists():
                try:
                    logger.info(f"  Processing {dataset}...")
                    df = pd.read_csv(source_file)
                    
                    # Add source column
                    df['data_source'] = dataset.replace('.csv', '')
                    df['processed_date'] = datetime.now().strftime('%Y-%m-%d')
                    
                    # Basic cleaning
                    df = df.dropna(how='all')
                    
                    # Save to bronze layer
                    bronze_file = bronze_kaggle_dir / f"{dataset.replace('.csv', '')}_bronze.csv"
                    df.to_csv(bronze_file, index=False)
                    
                    logger.info(f"   Processed {len(df)} records from {dataset}")
                    
                except Exception as e:
                    logger.error(f" Error processing {dataset}: {e}")
        
        # Process archive-2 structured data
        archive2_dir = kaggle_dir / "archive-2"
        if archive2_dir.exists():
            bronze_archive2_dir = bronze_kaggle_dir / "archive-2"
            bronze_archive2_dir.mkdir(parents=True, exist_ok=True)
            
            for csv_file in archive2_dir.rglob("*.csv"):
                try:
                    rel_path = csv_file.relative_to(archive2_dir)
                    logger.info(f"  Processing archive-2/{rel_path}...")
                    
                    df = pd.read_csv(csv_file)
                    df['data_source'] = f"archive-2/{rel_path}"
                    df['processed_date'] = datetime.now().strftime('%Y-%m-%d')
                    
                    # Save to bronze layer
                    bronze_file = bronze_archive2_dir / rel_path
                    bronze_file.parent.mkdir(parents=True, exist_ok=True)
                    df.to_csv(bronze_file, index=False)
                    
                    logger.info(f"   Processed {len(df)} records from archive-2/{rel_path}")
                    
                except Exception as e:
                    logger.error(f"   Error processing archive-2/{rel_path}: {e}")
    
    def create_unified_salary_dataset(self):
        """Create a unified salary dataset from all sources"""
        logger.info("Creating unified salary dataset...")
        
        salary_data = []
        
        # Process StackOverflow salary data
        bronze_so_dir = self.bronze_dir / "stackoverflow"
        if bronze_so_dir.exists():
            for survey_file in bronze_so_dir.glob("survey_*_bronze.csv"):
                try:
                    df = pd.read_csv(survey_file)
                    
                    # Extract salary-related columns (these vary by year)
                    salary_cols = [col for col in df.columns if 'salary' in col.lower() or 'compensation' in col.lower()]
                    
                    if salary_cols:
                        salary_df = df[['survey_year'] + salary_cols].copy()
                        salary_df['source'] = 'stackoverflow'
                        salary_data.append(salary_df)
                        
                except Exception as e:
                    logger.warning(f"    Could not process salary data from {survey_file}: {e}")
        
        # Process Kaggle salary data
        bronze_kaggle_dir = self.bronze_dir / "kaggle"
        if bronze_kaggle_dir.exists():
            # Look for salary-related files
            salary_files = list(bronze_kaggle_dir.rglob("*salary*.csv")) + list(bronze_kaggle_dir.rglob("*salaries*.csv"))
            
            for salary_file in salary_files:
                try:
                    df = pd.read_csv(salary_file)
                    df['source'] = 'kaggle'
                    salary_data.append(df)
                    
                except Exception as e:
                    logger.warning(f"     Could not process salary data from {salary_file}: {e}")
        
        # Combine all salary data
        if salary_data:
            unified_salary = pd.concat(salary_data, ignore_index=True)
            unified_salary['processed_date'] = datetime.now().strftime('%Y-%m-%d')
            
            # Save to silver layer
            silver_file = self.silver_dir / "unified_salary_data.csv"
            unified_salary.to_csv(silver_file, index=False)
            
            logger.info(f"  Created unified salary dataset with {len(unified_salary)} records")
        else:
            logger.warning("    No salary data found to unify")
    
    def run_full_pipeline(self):
        """Run the complete data processing pipeline"""
        logger.info("Starting comprehensive data processing pipeline...")
        
        # Step 1: Copy all data
        logger.info("\n Step 1: Copying all datasets...")
        self.copy_kaggle_data()
        
        # Step 2: Show data summary
        logger.info("\n Step 2: Data summary...")
        self.get_data_summary()
        
        # Step 3: Process data into bronze layer
        logger.info("\n Step 3: Processing data into bronze layer...")
        self.process_stackoverflow_data()
        self.process_kaggle_job_data()
        
        # Step 4: Create unified datasets
        logger.info("\nStep 4: Creating unified datasets...")
        self.create_unified_salary_dataset()
        
        logger.info("\n Data processing pipeline complete!")
        logger.info("Data is now organized in:")
        logger.info(f"   Raw: {self.raw_dir}")
        logger.info(f"   Bronze: {self.bronze_dir}")
        logger.info(f"   Silver: {self.silver_dir}")
        logger.info(f"   Gold: {self.gold_dir}")

def main():
    """Main function"""
    processor = ComprehensiveDataProcessor()
    processor.run_full_pipeline()

if __name__ == "__main__":
    main()
