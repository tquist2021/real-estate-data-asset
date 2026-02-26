import os
import pandas as pd
from dotenv import load_dotenv
from fredapi import Fred
import logging

# -------------------- Setup Logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()     
    ]
)
logger = logging.getLogger(__name__)

# -------------------- Load Environment --------------------
load_dotenv()
API_KEY = os.getenv("FRED_API_KEY")
file_path = os.getenv("file_path")


# -------------------- Initialize FRED --------------------
fred = Fred(api_key=API_KEY)

# -------------------- Functions --------------------
def get_brnz_extract(series_id: str) -> pd.DataFrame:
    """
    Fetches a time series from FRED and returns it as a pandas DataFrame.

    Parameters: 
        - series_id: the id from FRED that identifies which data will be pulled
    
    Returns: 
        - pandas_df: returns a pandas dataframe
    """
    logger.info(f"Fetching data '{series_id}' from FRED.")
    
    try:
        pandas_df = fred.get_series(series_id).to_frame().reset_index()
        logger.info(f"Successfully fetched {len(pandas_df)} rows for '{series_id}'")
        return pandas_df
    except Exception as e:
        logger.exception(f"Error fetching series '{series_id}': {e}")
        raise

def write_datalake(df: pd.DataFrame, med: str, table_name: str):
    """
    Writes the DataFrame to a single CSV file in the bronze layer.

    Parameters: 
        - df: pandas dataframe to be written
        - med: step in medallion architecture (bronze, silver, or gold.)
        - table_name: name of the table (file) that will be written
    """
    output_path = f"{file_path}/{med}/{table_name}.csv"
    try:
        df.to_csv(output_path, index=False)
        logger.info(f"Data successfully written to {output_path}")
    except Exception as e:
        logger.exception(f"Failed to write data to CSV '{output_path}': {e}")
        raise