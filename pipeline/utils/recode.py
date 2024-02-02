import numpy as np
import pandas as pd
from pipeline.utils import rename_columns

def replace_column_values_when_less_than(df: pd.DataFrame, change_col: str, by_col: str) -> pd.DataFrame:
    """
    Where one column is less than the other, set the column to equal the larger value.
    
    Args:
        df (pd.DataFrame): Containing the columns that are needed to compare
        change_col (str): The column you intend to change values based on the criteria
        by_col (str): The column used to compare against change_col
    
    Returns:
        pd.DataFrame: The same Dataframe with the amended column values
    """
    ## Change null values to -1 to satisfy the greater than statement
    df = df.fillna({change_col:-1,
                    by_col:-1})
    
    ## If by_col is greater than change_col, make change_col equal to by_col, otherwise do nothing
    df[change_col] = np.where(
        df[change_col] < df[by_col],
        df[by_col],
        df[change_col]
        )
    
    ## Replace -1 back to null to remove any DQ issues
    df[change_col] = df[change_col].replace(-1,np.nan)
    df[by_col] = df[by_col].replace(-1,np.nan)
    
    return df


def replace_column_values_with_sum(df: pd.DataFrame, change_col: str, to_sum: list) -> pd.DataFrame:
    """
    Change a column in a DataFrame to be equal to the sum of other columns.
    
    Args:
        df (pd.DataFrame): The DataFrame to change columns of
        change_col (str): The column that needs to be changed
        to_sum (list): A list including column titles to sum together
        
    Returns:
        pd.DataFrame: The original dataframe with changed columns
    """
    df[change_col] = df[to_sum].sum(axis = 1, skipna = True)
    
    return df


def replace_column_values_with_max(df: pd.DataFrame, change_col: str, find_max: list) -> pd.DataFrame:
    """
    Change a column in a DataFrame to equal to the maximum of other columns.
    
    Args:
        df (pd.DataFrame): The DataFrame to change columns of
        change_col (str): The column that needs to be changed
        find_max (list): A list including column titles to find maximum value of
        
    Returns:
        pd.DataFrame: The original dataframe with changed columns
    """
    df[change_col] = df[find_max].max(axis=1)
    
    return df


def replace_column_values_when_not_equal_two(df: pd.DataFrame, change_col: str, by_col: str) -> pd.DataFrame:
    """
    Where one column is not 2, set another column to 0, otherwise do nothing.
    
    Args:
        df (pd.DataFrame): Dataframe containing the columns that are needed to compare
        change_col (str): The column you intend to change values based on the criteria
        by_col (str): The column used to compare against change_col
    
    Returns:
        pd.DataFrame: The same Dataframe with the amended column values
    """
    df[change_col] = np.where(
        (df[by_col] != 2),
        0, 
        df[change_col]
        )
    return df


def replace_column_values_when_equal_two(df: pd.DataFrame, change_col: str, by_col: str, set_col: str) -> pd.DataFrame:
    """
    Where one column is 2, and the column is null, set column value to another column value, otherwise do nothing.
    
    Args:
        df (pd.DataFrame): Dataframe containing the columns that are needed to compare
        change_col (str): The column you intend to change values based on the criteria
        by_col (str): The column used to compare against change_col
        set_col (str): Set change column to the value of this column if conditions are met
    
    Returns:
        pd.DataFrame: The same Dataframe with the amended column values
    """
    df[change_col] = np.where(
        (df[by_col] == 2) & (df[change_col].isnull()),
        df[set_col],
        df[change_col]
        )
    return df


def replace_column_values_when_null(df: pd.DataFrame, change_col: str, by_col: str) -> pd.DataFrame:
    """
    If a column is null, set the value to equal another column, otherwise do nothing.

    Args:
        df (pd.DataFrame): Dataframe containing the columns that are needed to compare
        change_col (str): The column you intend to change values based on the criteria
        by_col (str): The column used to compare against change_col
    
    Returns:
        pd.DataFrame: The same Dataframe with the amended column values
    """

    df[change_col] = np.where(
        df[change_col].isnull(),
        df[by_col],
        df[change_col]
    )
    return df


def replace_choices_outputs(df: pd.DataFrame, change_col: str) -> pd.DataFrame:
    """
    If a column is 2, set the value to 1, otherwise set to 0.

    Args:
        df (pd.DataFrame): Dataframe containing the columns that are needed to compare
        change_col (str): The column you intend to change values based on the criteria
    
    Returns:
        pd.DataFrame: The same Dataframe with the amended column values
    """
    df[change_col] = np.where(
        df[change_col] == 2, 
        1, 
        0
    )
    return df

def replace_duplicate_supplier_tag(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Replace suppliers that have been mapped to EMIS (I) to EMIS
    """
    df[col] = np.where(
        df[col] == 'EMIS (I)',
        'EMIS',
        df[col]
    )
    return df


def change_region_names(df, to_change, to_compare):
    """
    Replace region names to be used in the dashboard
    """
    for i in rename_columns.nhs_regions:
        df[to_change] = np.where(
            df[to_compare] == i[0], 
            i[1], 
            df[to_change]
        )
        
    return df