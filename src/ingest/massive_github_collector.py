#!/usr/bin/env python3
"""
Massive GitHub Archive Data Collector
Collects multiple months of GitHub Archive data to scale to 15+ GB
"""

import argparse
import gzip
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from tqdm import tqdm

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.common.paths import RAW_DIR
from src.common.logs import get_logger

logger = get_logger("ingest.massive_github")

class MassiveGitHubCollector:
    def __init__(self, output_dir=None):
        self.output_dir = Path(output_dir) if output_dir else RAW_DIR / "github"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.base_url = 'http://data.gharchive.org'
        
        # Statistics tracking
        self.total_downloaded = 0
        self.total_size = 0
        self.failed_downloads = 0

    def collect_month_range(self, start_year=2024, start_month=1, end_year=2024, end_month=12):
        """
        Collect multiple months of GitHub archive data
        """
        logger.info(f"🚀 Starting massive GitHub data collection...")
        logger.info(f"📅 Range: {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
        
        current_date = datetime(start_year, start_month, 1)
        end_date = datetime(end_year, end_month, 1)
        
        while current_date <= end_date:
            year = current_date.year
            month = current_date.month
            
            logger.info(f"\n📊 Collecting {year}-{month:02d}...")
            month_files = self.collect_month_data(year, month)
            
            if month_files:
                logger.info(f"✅ {year}-{month:02d}: {len(month_files)} files downloaded")
            else:
                logger.warning(f"⚠️  {year}-{month:02d}: No files downloaded")
            
            # Move to next month
            if month == 12:
                current_date = current_date.replace(year=year + 1, month=1)
            else:
                current_date = current_date.replace(month=month + 1)
        
        self.print_final_stats()

    def collect_month_data(self, year=2024, month=1):
        """
        Collect one month of GitHub archive data
        """
        # Calculate days in month
        if month == 12:
            next_month = datetime(year + 1, 1, 1)
        else:
            next_month = datetime(year, month + 1, 1)
        
        days_in_month = (next_month - datetime(year, month, 1)).days
        downloaded_files = []
        
        for day in range(1, days_in_month + 1):
            day_files = self.collect_day_data(year, month, day)
            downloaded_files.extend(day_files)
            
            # Be nice to the server - longer delay between days
            time.sleep(2)
        
        return downloaded_files

    def collect_day_data(self, year=2024, month=1, day=1):
        """
        Collect one day of GitHub archive data
        """
        downloaded_files = []
        
        for hour in range(24):
            filename = f"{year}-{month:02d}-{day:02d}-{hour}.json.gz"
            url = f"{self.base_url}/{filename}"
            output_path = self.output_dir / filename

            # Skip if already downloaded
            if output_path.exists():
                file_size = output_path.stat().st_size
                self.total_size += file_size
                self.total_downloaded += 1
                logger.info(f"⏭️  Skipping {filename} (already exists, {file_size/1024/1024:.1f} MB)")
                downloaded_files.append(filename)
                continue

            try:
                logger.info(f"⬇️  Downloading {filename}...")
                response = requests.get(url, stream=True, timeout=120)

                if response.status_code == 200:
                    total_size = int(response.headers.get('content-length', 0))
                    
                    with open(output_path, 'wb') as f:
                        with tqdm(total=total_size, unit='B', unit_scale=True, desc=filename) as pbar:
                            for chunk in response.iter_content(chunk_size=8192):
                                f.write(chunk)
                                pbar.update(len(chunk))

                    file_size = output_path.stat().st_size
                    self.total_size += file_size
                    self.total_downloaded += 1
                    downloaded_files.append(filename)
                    
                    logger.info(f"✅ Downloaded {filename} ({file_size/1024/1024:.1f} MB)")
                else:
                    logger.warning(f"❌ Failed to download {filename}: Status {response.status_code}")
                    self.failed_downloads += 1

                # Be nice to the server
                time.sleep(1)

            except Exception as e:
                logger.error(f"❌ Error downloading {filename}: {str(e)}")
                self.failed_downloads += 1
                continue

        return downloaded_files

    def collect_recent_weeks(self, weeks=8):
        """
        Collect recent weeks of data (useful for current analysis)
        """
        logger.info(f"📅 Collecting last {weeks} weeks of GitHub data...")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(weeks=weeks)
        
        current_date = start_date
        downloaded_files = []
        
        while current_date <= end_date:
            year = current_date.year
            month = current_date.month
            day = current_date.day
            
            day_files = self.collect_day_data(year, month, day)
            downloaded_files.extend(day_files)
            
            current_date += timedelta(days=1)
            time.sleep(1)  # Be nice to the server
        
        self.print_final_stats()
        return downloaded_files

    def collect_high_activity_days(self, year=2024, month=1, days=None):
        """
        Collect specific high-activity days (e.g., major releases, conferences)
        """
        if days is None:
            # Default to some high-activity days
            days = [1, 15, 28]  # Beginning, middle, end of month
        
        logger.info(f"📅 Collecting high-activity days for {year}-{month:02d}: {days}")
        
        downloaded_files = []
        for day in days:
            day_files = self.collect_day_data(year, month, day)
            downloaded_files.extend(day_files)
            time.sleep(2)
        
        return downloaded_files

    def validate_data(self, sample_size=50):
        """Validate downloaded GitHub data"""
        logger.info("🔍 Validating GitHub Archive data...")

        files = list(self.output_dir.glob("*.json.gz"))
        if not files:
            logger.warning("No files found to validate")
            return 0, []

        valid_files = 0
        invalid_files = []

        # Sample validation
        sample_files = files[:sample_size] if len(files) > sample_size else files

        for file_path in tqdm(sample_files, desc="Validating files"):
            try:
                with gzip.open(file_path, 'rt') as f:
                    # Try to read first few lines
                    for i, line in enumerate(f):
                        if i >= 5:  # Check first 5 lines
                            break
                        json.loads(line)
                valid_files += 1
            except Exception as e:
                invalid_files.append((file_path.name, str(e)))

        logger.info(f"✅ Valid files: {valid_files}")
        if invalid_files:
            logger.warning(f"❌ Invalid files: {len(invalid_files)}")
            for filename, error in invalid_files:
                logger.warning(f"   - {filename}: {error}")

        return valid_files, invalid_files

    def get_file_stats(self):
        """Get comprehensive statistics about downloaded files"""
        files = list(self.output_dir.glob("*.json.gz"))
        total_size = sum(f.stat().st_size for f in files)
        
        # Group by month
        monthly_stats = {}
        for file_path in files:
            filename = file_path.name
            # Extract date from filename (YYYY-MM-DD-HH.json.gz)
            date_part = filename.split('.')[0]  # Remove .json.gz
            year_month = '-'.join(date_part.split('-')[:2])  # YYYY-MM
            
            if year_month not in monthly_stats:
                monthly_stats[year_month] = {'files': 0, 'size': 0}
            
            monthly_stats[year_month]['files'] += 1
            monthly_stats[year_month]['size'] += file_path.stat().st_size
        
        logger.info(f"📊 File Statistics:")
        logger.info(f"   Total files: {len(files)}")
        logger.info(f"   Total size: {total_size / (1024**3):.2f} GB")
        
        logger.info(f"   Monthly breakdown:")
        for month, stats in sorted(monthly_stats.items()):
            size_gb = stats['size'] / (1024**3)
            logger.info(f"     {month}: {stats['files']} files, {size_gb:.2f} GB")
        
        return len(files), total_size

    def print_final_stats(self):
        """Print final collection statistics"""
        logger.info(f"\n🎉 Collection Complete!")
        logger.info(f"   Files downloaded: {self.total_downloaded}")
        logger.info(f"   Failed downloads: {self.failed_downloads}")
        logger.info(f"   Total size: {self.total_size / (1024**3):.2f} GB")
        
        if self.total_downloaded > 0:
            success_rate = (self.total_downloaded / (self.total_downloaded + self.failed_downloads)) * 100
            logger.info(f"   Success rate: {success_rate:.1f}%")

def main():
    parser = argparse.ArgumentParser(description="Massive GitHub Archive Data Collector")
    parser.add_argument("--mode", choices=['month-range', 'recent-weeks', 'high-activity'], 
                       default='month-range', help="Collection mode")
    parser.add_argument("--start-year", type=int, default=2024, help="Start year")
    parser.add_argument("--start-month", type=int, default=1, help="Start month")
    parser.add_argument("--end-year", type=int, default=2024, help="End year")
    parser.add_argument("--end-month", type=int, default=12, help="End month")
    parser.add_argument("--weeks", type=int, default=8, help="Number of recent weeks to collect")
    parser.add_argument("--validate", action="store_true", help="Validate downloaded data")
    parser.add_argument("--stats", action="store_true", help="Show file statistics")
    parser.add_argument("--output-dir", help="Output directory")

    args = parser.parse_args()

    collector = MassiveGitHubCollector(args.output_dir)

    if args.stats:
        collector.get_file_stats()
        return

    if args.validate:
        collector.validate_data()
        return

    if args.mode == 'month-range':
        collector.collect_month_range(args.start_year, args.start_month, args.end_year, args.end_month)
    elif args.mode == 'recent-weeks':
        collector.collect_recent_weeks(args.weeks)
    elif args.mode == 'high-activity':
        collector.collect_high_activity_days(args.start_year, args.start_month)

    # Show stats after collection
    collector.get_file_stats()

if __name__ == "__main__":
    main()
