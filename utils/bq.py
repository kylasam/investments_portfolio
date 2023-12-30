import logging
import pandas_gbq
import prettytable as pt
import configparser
import os,sys
import datetime

def read_config(logger,):
    # Define the path to the config file
    config_file_path = os.path.join('config', 'config.ini')

    # Check if the config file exists
    if not os.path.exists(config_file_path):
        logger.error("\t\tConfig file NOT FOUND ,EXITING the code Execution ❌ ")
        sys.exit(98)

    # Create a ConfigParser object
    config = configparser.ConfigParser()

    try:
        # Read the config file
        config.read(config_file_path)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.get('bq','service_accnt_json_loc')
        os.environ["run_ts"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        logger.info("\t\tConfig file Exists and Read SUCCESSFULLY ✅ ")
        return config

    except configparser.Error as e:
        logger.error(f"Error reading config file: {e}")
        return None

def load_bq(src_df, project_id, dataset_id, table_id, write_mode, logger):
    """
    Load a Pandas DataFrame to BigQuery.

    Parameters:
    - src_df: DataFrame, the source DataFrame to be loaded to BigQuery.
    - project_id: str, the BigQuery project ID.
    - dataset_id: str, the BigQuery dataset ID.
    - table_id: str, the BigQuery table ID.
    - if_exists: str, default 'append', whether to append, replace, or fail if the table exists.
    - SA_file : Service account Json file location
    Returns:
    - None
    """

    #read_config(logger)
    try:
        os.environ["run_ts"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        logger.info(f"\t\tLoading DataFrame to BigQuery: {project_id}.{dataset_id}{table_id}")
        # Ensure the 'src_df' parameter is a DataFrame
        if not isinstance(src_df, pd.DataFrame):
            raise ValueError("The 'src_df' parameter must be a Pandas DataFrame.")

        # Load the DataFrame to BigQuery
        pandas_gbq.to_gbq(src_df, f'{project_id}.{dataset_id}.{table_id}', project_id=project_id, if_exists=write_mode)
        logger.info("\t\tData load to EDW BQ completed SUCCESSFULLY ✅ \n\n")
    except Exception as e:
        logger.error("\t\tERROR :%s in EDW DATA LOAD ❌ ",str(e))
        sys.exit(96)

