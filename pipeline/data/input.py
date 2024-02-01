import pyodbc as dbc
import json
import sqlalchemy
from sqlalchemy.engine import URL
from sqlalchemy import text as sql_text
import pandas as pd
from pathlib import Path
from pipeline.utils import params
import glob
import os

def create_sql_connection(connection_details: str):
    """
    Create connection to sql servers to load data in
    Args: 
        connection_details (str): Details of the database to connect to 
    Returns:
        connection variable
    """
    connection_url = URL.create(
        "mssql+pyodbc", query={"odbc_connect": connection_details}
    )
    return sqlalchemy.create_engine(connection_url)


def load_json_config_file(path):
    with open(path) as f:
        file = json.load(f)
    return file


def get_pomi_sql_strings(rpsd: str, rped: str) -> pd.DataFrame:
    """
    Create strings to import POMI data from sql. Columns imported and filters can be changed here
     
    Args:
        rpsd (str): report period start date in the format YYYY-MM-DD
        rped (str): report period end date in the format YYYY-MM-DD
    Returns:
         pd.DataFrame: five strings for each of the inputs from sql to be used to import data
    """
    gp_dim_sql_str = """
    SELECT 
    GP_Key, 
    GP_Code as PRACTICE_CODE, 
    Supplier, 
    GP_Name AS PRACTICE_NAME, 
    Total_Patients,
    Supplier_Version
    FROM ic.PRIM_POMI_GP_DIM
    WHERE Report_End between '{}' and '{}'
    """.format(rpsd, rped)

    prim_pomi_sql_str = """
    SELECT * 
    FROM ic.PRIM_POMI_FACT
    WHERE Report_End between '{}' and '{}'
    """.format(rpsd, rped)

    prim_pomi_inf_sql_str = """
    SELECT * 
    FROM ic.PRIM_POMI_FACT_INF
    WHERE Report_End between '{}' and '{}'
    """.format(rpsd, rped)

    prim_pomi_field_sql_str = """
    SELECT *
    FROM ic.PRIM_POMI_FIELD_DIM
    """

    gpes_sites_sql_str = """
    SELECT *
    FROM [dbo].[GPES_SITES_V01] as a
    WHERE DSS_RECORD_START_DATE between '{}' and '{}'
    OR DSS_RECORD_END_DATE IS NULL
    """.format(rpsd, rped)

    return gp_dim_sql_str, prim_pomi_sql_str, prim_pomi_inf_sql_str, prim_pomi_field_sql_str, gpes_sites_sql_str


def get_mapping_sql_query_strings(rpsd: str, rped: str) -> str:
    """
    Create strings to import sql data for practice mappings. Any mapping changes can be carried out here

    Args:
        rped (str): report period end date in the format YYYY-MM-DD
    Returns:
        str: four strings for each of the inputs from sql to be used to import mappings
    """
    open_active_sql_str = """
    SELECT 
    GP_Code AS PRACTICE_CODE, 
    GP_Name AS PRACTICE_NAME, 
    Region_Code as REGION_CODE,
    Region_Name,
    SubRegion_Code as ICB_CODE,
    STP_Name,
    CCG_Code as SUB_ICB_CODE,
    CCG_Name,
    Report_End
    FROM [PRIM_POMI].[ic].[PRIM_POMI_GP_DIM]
    WHERE Report_End >= '{}'
    AND Report_End <= '{}'
    """.format(rpsd, rped)

    sub_icb_mapping_sql_str = """
    SELECT DISTINCT  
    a.[DH_GEOGRAPHY_CODE] AS SUB_ICB_CODE, 
    a.[GEOGRAPHY_CODE] AS SUB_ICB_ONS_CODE,
    a.[GEOGRAPHY_NAME] AS SUB_ICB_NAME
    FROM [dbo].[ONS_CHD_GEO_EQUIVALENTS] as a
    INNER JOIN (SELECT DH_GEOGRAPHY_CODE, MAX(DATE_OF_OPERATION)
    AS DATE_OF_OPERATION FROM [dbo].[ONS_CHD_GEO_EQUIVALENTS] 
    WHERE DATE_OF_OPERATION <= '{}'
    AND ENTITY_CODE = 'E38'
    AND (DATE_OF_TERMINATION IS NULL OR DATE_OF_TERMINATION >= '{}')
    GROUP BY DH_GEOGRAPHY_CODE) as b
    ON a.DATE_OF_OPERATION = b.DATE_OF_OPERATION
    AND a.DH_GEOGRAPHY_CODE = b.DH_GEOGRAPHY_CODE
    """.format(rped, rped)
    
    icb_mapping_sql_str = """
    SELECT DISTINCT 
    a.[DH_GEOGRAPHY_CODE] AS ICB_CODE, 
    a.[GEOGRAPHY_CODE] AS ICB_ONS_CODE,
    a.[DH_GEOGRAPHY_NAME] AS ICB_NAME
    FROM [dbo].[ONS_CHD_GEO_EQUIVALENTS] as a
    INNER JOIN (SELECT DH_GEOGRAPHY_CODE, MAX(DATE_OF_OPERATION) 
    AS DATE_OF_OPERATION FROM [dbo].[ONS_CHD_GEO_EQUIVALENTS] 
    WHERE DATE_OF_OPERATION <= '{}'
    AND ENTITY_CODE = 'E54'
    AND (DATE_OF_TERMINATION IS NULL OR DATE_OF_TERMINATION >= '{}')
    GROUP BY DH_GEOGRAPHY_CODE) as b
    ON a.DATE_OF_OPERATION = b.DATE_OF_OPERATION
    AND a.DH_GEOGRAPHY_CODE = b.DH_GEOGRAPHY_CODE
    """.format(rped, rped)

    region_mapping_sql_str = """
    SELECT DISTINCT
    a.[DH_GEOGRAPHY_CODE] AS REGION_CODE,
    a.[GEOGRAPHY_CODE] AS REGION_ONS_CODE,
    a.[DH_GEOGRAPHY_NAME] AS REGION_NAME
    FROM [dbo].[ONS_CHD_GEO_EQUIVALENTS] as a
    INNER JOIN (SELECT DH_GEOGRAPHY_CODE, MAX(DATE_OF_OPERATION) 
    AS DATE_OF_OPERATION FROM [dbo].[ONS_CHD_GEO_EQUIVALENTS] 
    WHERE DATE_OF_OPERATION <= '{}'
    AND ENTITY_CODE = 'E40'
    AND (DATE_OF_TERMINATION IS NULL OR DATE_OF_TERMINATION >= '{}')
    GROUP BY DH_GEOGRAPHY_CODE) as b
    ON a.DATE_OF_OPERATION = b.DATE_OF_OPERATION
    AND a.DH_GEOGRAPHY_CODE = b.DH_GEOGRAPHY_CODE
    """.format(rped, rped)

    return open_active_sql_str, sub_icb_mapping_sql_str, icb_mapping_sql_str, region_mapping_sql_str


def get_sql_data(sql_str: str, connection: str) -> pd.DataFrame:
    """
    Read in SQL data based on SQL str using specified connection
    """
    return pd.read_sql(sql=sql_text(sql_str), con=connection.connect())


def get_exclude_list() -> pd.DataFrame:
    """
    Read the exclude list in from the repo
    """
    filepath = Path(params.params["ROOT_DIR"]) / params.params["DATA_FOLDER"] / "Fact Table Exclude List.csv"

    #HA added index_col=False param below
    exclude_list = pd.read_csv(filepath, names=['SYS_Timestamp','Supplier'], index_col=False)
    exclude_list['SYS_Timestamp'] = pd.to_datetime(exclude_list['SYS_Timestamp'], format=('%d%b%Y:%H:%M:%S'))
    exclude_list['SYS_Timestamp'] = pd.to_datetime(exclude_list['SYS_Timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')

    return exclude_list


def get_inf_exclude_list() -> pd.DataFrame:
    """
    Read the informatica exclude list in from the repo
    """
    filepath = Path(params.params["ROOT_DIR"]) / params.params["DATA_FOLDER"] / "Fact Table Exclude List INF.csv"
    
    inf_exclude_list = pd.read_csv(filepath, names=['SYS_Timestamp','Supplier'])
    inf_exclude_list['SYS_Timestamp'] = pd.to_datetime(inf_exclude_list['SYS_Timestamp'], format=('%d%b%Y:%H:%M:%S'))
    inf_exclude_list['SYS_Timestamp'] = pd.to_datetime(inf_exclude_list['SYS_Timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')

    return inf_exclude_list

def get_trend_monitor_template_path(): 
    """
    Read the trend monitor template
    """
    return Path(params.params["ROOT_DIR"]) / params.params["DATA_FOLDER"] / "Trend_Monitor_Template.xlsx"

def get_practicipation_dataframes(root):
    """
    """
    pattern = os.path.join(f"{root}\\Inputs\\", "GPWT_PARTICIPATION*.csv")
    csv_files = glob.glob(pattern)
    part_csv = csv_files[-1]
    print(f"Using the {part_csv} file")
    prac_df = pd.read_csv(part_csv)

    pattern = os.path.join(f"{root}\\Inputs\\", "QS Part Status*.csv")
    csv_files = glob.glob(pattern)
    status_csv = csv_files[-1]
    print(f"Using the {status_csv} file")
    status_df = pd.read_csv(status_csv, skiprows=9)

    return prac_df, status_df