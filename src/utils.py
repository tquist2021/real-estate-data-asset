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
data_lake_fp = os.getenv("data_lake_fp")


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
        - prefix: for file name ex: brnz, slvr, gold.
        - table_name: name of the table (file) that will be written
    """
    output_path = f"{data_lake_fp}/{med}/real-estate-data-asset/{med}_{table_name}.csv"
    try:
        df.to_csv(output_path, index=False)
        logger.info(f"Data successfully written to {output_path}")
    except Exception as e:
        logger.exception(f"Failed to write data to CSV '{output_path}': {e}")
        raise

def clean_data(df: pd.DataFrame, date_name: str, col_name: str) -> pd.DataFrame: 
    """
    Takes pandas DataFrame and drops duplicates, drops null values, assigns column names,
    Parameters: 
        - df str: name of DataFrame to be cleaned. 
    """ 
    df = df.drop_duplicates()
    df = df.dropna()

    cols = [x for x in df.columns]
    df = df.rename(
        columns = {
            cols[0] : date_name,
            cols[1] : col_name
        }
    )

    df[date_name] = pd.to_datetime(df[date_name])

    return df

def get_table_names(dir: str):
    """
    Returns a list of table (file) names that are in a directory.

    Parameters:
        - dir: str - folder where tables are located
    """
    files = []
    files = [f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f))]

    return files

