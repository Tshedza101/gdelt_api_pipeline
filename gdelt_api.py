import os
import logging
import duckdb
import requests
import zipfile
import io
from apscheduler.schedulers.blocking import BlockingScheduler

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# GDELT v2 Export format has exactly 61 columns
GDELT_COLUMNS = {
    'GLOBALEVENTID': 'VARCHAR',
    'SQLDATE': 'VARCHAR',
    'MonthYear': 'VARCHAR',
    'Year': 'VARCHAR',
    'FractionDate': 'VARCHAR',
    'Actor1Code': 'VARCHAR',
    'Actor1Name': 'VARCHAR',
    'Actor1CountryCode': 'VARCHAR',
    'Actor1KnownGroupCode': 'VARCHAR',
    'Actor1EthnicCode': 'VARCHAR',
    'Actor1Religion1Code': 'VARCHAR',
    'Actor1Religion2Code': 'VARCHAR',
    'Actor1Type1Code': 'VARCHAR',
    'Actor1Type2Code': 'VARCHAR',
    'Actor1Type3Code': 'VARCHAR',
    'Actor2Code': 'VARCHAR',
    'Actor2Name': 'VARCHAR',
    'Actor2CountryCode': 'VARCHAR',
    'Actor2KnownGroupCode': 'VARCHAR',
    'Actor2EthnicCode': 'VARCHAR',
    'Actor2Religion1Code': 'VARCHAR',
    'Actor2Religion2Code': 'VARCHAR',
    'Actor2Type1Code': 'VARCHAR',
    'Actor2Type2Code': 'VARCHAR',
    'Actor2Type3Code': 'VARCHAR',
    'IsRootEvent': 'VARCHAR',
    'EventCode': 'VARCHAR',
    'EventBaseCode': 'VARCHAR',
    'EventRootCode': 'VARCHAR',
    'QuadClass': 'VARCHAR',
    'GoldsteinScale': 'VARCHAR',
    'NumMentions': 'VARCHAR',
    'NumSources': 'VARCHAR',
    'NumArticles': 'VARCHAR',
    'AvgTone': 'VARCHAR',
    'Actor1Geo_Type': 'VARCHAR',
    'Actor1Geo_FullName': 'VARCHAR',
    'Actor1Geo_CountryCode': 'VARCHAR',
    'Actor1Geo_ADM1Code': 'VARCHAR',
    'Actor1Geo_ADM2Code': 'VARCHAR',
    'Actor1Geo_Lat': 'VARCHAR',
    'Actor1Geo_Long': 'VARCHAR',
    'Actor1Geo_FeatureID': 'VARCHAR',
    'Actor2Geo_Type': 'VARCHAR',
    'Actor2Geo_FullName': 'VARCHAR',
    'Actor2Geo_CountryCode': 'VARCHAR',
    'Actor2Geo_ADM1Code': 'VARCHAR',
    'Actor2Geo_ADM2Code': 'VARCHAR',
    'Actor2Geo_Lat': 'VARCHAR',
    'Actor2Geo_Long': 'VARCHAR',
    'Actor2Geo_FeatureID': 'VARCHAR',
    'ActionGeo_Type': 'VARCHAR',
    'ActionGeo_FullName': 'VARCHAR',
    'ActionGeo_CountryCode': 'VARCHAR',
    'ActionGeo_ADM1Code': 'VARCHAR',
    'ActionGeo_ADM2Code': 'VARCHAR',
    'ActionGeo_Lat': 'VARCHAR',
    'ActionGeo_Long': 'VARCHAR',
    'ActionGeo_FeatureID': 'VARCHAR',
    'DATEADDED': 'VARCHAR',
    'SOURCEURL': 'VARCHAR'
}

RAW_DIR = r"gdelt_lakehouse\raw"
PROCESSED_DIR = r"gdelt_lakehouse\processed"

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

def process_bulk_file(file_url):
    zip_filename = file_url.split('/')[-1]
    csv_filename = zip_filename.replace('.zip', '')
    parquet_filename = csv_filename.replace('.export.CSV', '.parquet')
    
    raw_csv_path = os.path.join(RAW_DIR, csv_filename)
    processed_parquet_path = os.path.join(PROCESSED_DIR, parquet_filename)

    # Download and extract ZIP if CSV isn't locally available
    if not os.path.exists(raw_csv_path):
        logging.info(f"Downloading bulk file: {file_url}")
        res = requests.get(file_url)
        res.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(res.content)) as z:
            z.extractall(RAW_DIR)

    logging.info(f"Converting TSV to Parquet: {parquet_filename}")
    
    # Pre-clean file paths for SQL string interpolation without backslashes in f-string expression
    clean_csv_path = raw_csv_path.replace('\\', '/')
    clean_parquet_path = processed_parquet_path.replace('\\', '/')
    
    con = duckdb.connect()
    try:
        con.execute(f"""
            COPY (
                SELECT * FROM read_csv(
                    '{clean_csv_path}',
                    delim='\t',
                    header=False,
                    auto_detect=False,
                    columns={GDELT_COLUMNS},
                    quote='',
                    escape='',
                    null_padding=True,
                    ignore_errors=True
                )
            ) TO '{clean_parquet_path}' (FORMAT PARQUET);
        """)
    finally:
        con.close()

def run_job():
    logging.info("Executing scheduled GDELT sync...")
    manifest_url = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
    logging.info(f"Checking manifest: {manifest_url}")
    
    response = requests.get(manifest_url)
    response.raise_for_status()
    
    # Extract the export CSV file URL (first line of lastupdate.txt)
    lines = response.text.strip().split('\n')
    for line in lines:
        if '.export.CSV.zip' in line:
            export_url = line.split()[-1]
            try:
                process_bulk_file(export_url)
            except Exception as e:
                logging.error(f"Error processing {export_url}: {e}")
            break

class GDELTPipelineOrchestrator:
    @staticmethod
    def run_job():
        run_job()

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(GDELTPipelineOrchestrator.run_job, 'interval', minutes=15)
    
    logging.info("Adding job tentatively -- it will be properly scheduled when the scheduler starts")
    logging.info('Added job "GDELTPipelineOrchestrator.run_job" to job store "default"')
    logging.info("Scheduler started")
    logging.info("Scheduler started. Pipeline running every 15 minutes.")
    
    # Immediate execution on start
    run_job()
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
