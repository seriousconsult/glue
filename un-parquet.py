import pyarrow.parquet as pq
import s3fs
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

path = "s3://kindred-0/MX/DB3A/2025-12-04/PrecisionGeo/part-00000-74c5435a-bca0-4f15-be5d-75729d8dcc74-c000.gz.parquet"

try:
    fs = s3fs.S3FileSystem()
    
    logger.info("Opening file stream...")
    with fs.open(path) as f:
        parquet_file = pq.ParquetFile(f)
        
        # This is the key: read_batch() instead of read_row_group()
        # We only ask for the first batch of 5 records
        logger.info("Streaming the first 5 records...")
        batch_iter = parquet_file.iter_batches(batch_size=5)
        first_batch = next(batch_iter)
        
        # Convert only these 5 rows to a pandas DataFrame
        df_sample = first_batch.to_pandas()
        
        print("\n✅ SAMPLE DATA (First 5 Rows):")
        print(df_sample.to_string())
        
        print("\n--- Column Data Types ---")
        print(df_sample.dtypes)

except Exception as e:
    logger.error(f"❌ Failed: {e}")