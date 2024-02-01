import pandas as pd
from pipeline.utils import params


def filter_for_report_end(df: pd.DataFrame, col: str) -> pd.DataFrame:

    return df.loc[df[col] == params.get_report_period_end_date()]


def filter_for_financial_year(df: pd.DataFrame, col: str) -> pd.DataFrame:

    return df.loc[df[col] >= params.get_financial_year_start()]


def change_values_to_integer(df: pd.DataFrame, cols: list) -> pd.DataFrame:

    # for col in cols:
    #     df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int32')

    df[cols] = df[cols].astype('Int32')
    return df


def convert_column_datetype(df: pd.DataFrame, col: str, datetype: str, upper = False) -> pd.DataFrame:
    if upper == True:
        df[col] = pd.to_datetime(df[col]).dt.strftime(datetype).str.upper()
    else:
        df[col] = pd.to_datetime(df[col]).dt.strftime(datetype)
    return df


def pivot_pbi_tables(df: pd.DataFrame, col: str, prefix: str) -> pd.DataFrame:
    """
    Pivots the pbi_fields_long table, groups by a specified column and adds a prefix to column names to define whether
    the column is CCG, STP, or region. Returns a wide dataframe grouped by the geography specified
    
    Args:
        col: The column title you want to group by
        prefix: The string you want to add to the column titles
        
    Returns:
        data_wide: A wide dataframe containing the values grouped by, ICB, SUB_ICB, region or nation. With prefixes added
        to column titles containing values
    """

    data = (
        df
        .groupby(by=[col,'report_period_end','variable'],as_index=False)
        .agg({
            'GPPracticeCode':'count',
            'value':'sum'
        })
    )

    data['variable'] = prefix + data['variable']

    data_wide = (pd.pivot_table(
        data,
        values='value',
        index=[col,'report_period_end','GPPracticeCode'],
        columns=['variable']
        )
        .reset_index()
        .drop(columns=[prefix + 'Total_Pat_Enabled'])
    )
    
    data_wide = data_wide.rename(columns={'GPPracticeCode': prefix + 'PRAC_COUNT'})

    return data_wide