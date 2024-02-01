import pandas as pd
import numpy as np
from pipeline.utils import recode

def combine_pomi_datasets(prim_pomi_df: pd.DataFrame, gp_dim_df: pd.DataFrame) -> pd.DataFrame:
    """
    Join both POMI datasets together

    Args:
        prim_pomi_df (pd.DataFrame): Table containing all field keys to be used as columns in outputs
        gp_dim_df (pd.DataFrame): Table containing suppliers, practice names and patient list size

    Returns:
        pd.DataFrame: Data combined at a practice level
    """
    df = pd.merge(
        prim_pomi_df, 
        gp_dim_df[['GP_Key','Supplier']], 
        how='right', 
        on=['GP_Key']
    )
    return df

def drop_exclude_list(df: pd.DataFrame, exclude_list_df: pd.DataFrame, rpsd: str, rped: str) -> pd.DataFrame:
    """
    Remove rows included in the exclude list from the data.

    Args:
        df (pd.DataFrame): POMI datasets combined
        exclude_list_df (pd.DataFrame): Containing rows of data that should not be included in the data
        rpsd (str): Report period start date as defined in params
        rped (str): Report period end date as defined in params

    Returns:
        pd.DataFrame: Dataset with rows from exclude list deleted
    """
    to_drop = pd.merge(
        df, 
        exclude_list_df, 
        how='inner', 
        on=['Supplier','SYS_Timestamp']
    )
    
    period_fact = pd.merge(
        df, 
        exclude_list_df, 
        how='left', 
        on=['Supplier','SYS_Timestamp']
    )
    
    report_period_fact = period_fact.loc[
        (period_fact['Report_End'] >= rpsd) &
        (period_fact['Report_End'] <= rped)
    ]

    report_period_fact = report_period_fact.loc[
        (~report_period_fact['FACT_Key'].isin(to_drop['FACT_Key']))
    ]

    report_period_fact = report_period_fact.drop(['Supplier'],axis=1)
    
    return report_period_fact

def clean_and_join_inf_data(
    df: pd.DataFrame, 
    prim_pomi_inf_df: pd.DataFrame, 
    inf_exclude_list_df: pd.DataFrame, 
    rpsd: str, 
    rped: str
) -> pd.DataFrame:
    """
    Remove rows in the informatica exclude list, and then join to the other suppliers

    Args:
        df (pd.DataFrame): All POMI data cleaned
        prim_pomi_inf_df (pd.DataFrame): All POMI data from informatica
        inf_exclude_list_df (pd.DataFrame): Containing rows of data that should not be included in the data
        rpsd (str): Report period start date as defined in params
        rped (str): Report period end date as defined in params

    Returns:
        pd.DataFrame: POMI data with informatica data added
    """
    period_inf_fact = pd.merge(
        prim_pomi_inf_df,
        inf_exclude_list_df,
        how='left',
        on=['SYS_Timestamp']
    )
    
    report_period_inf_fact = period_inf_fact.loc[
        (period_inf_fact['Report_End'] >= rpsd) &
        (period_inf_fact['Report_End'] <= rped) &
        (period_inf_fact['SYS_Timestamp'] >= '2017-12-19 00:00:00') &
        (~period_inf_fact['SYS_Timestamp'].isin(inf_exclude_list_df['SYS_Timestamp']))
    ]
    
    report_period_fact_all = pd.concat([df, report_period_inf_fact])

    report_period_fact_all['Field_Key'] = pd.to_numeric(report_period_fact_all['Field_Key'], downcast='integer').astype(int)
    
    return report_period_fact_all

def create_metadata(df: pd.DataFrame, prim_pomi_field_df: pd.DataFrame):
    """
    POMI data with field key and column names added

    Args:
        df (pd.DataFrame): All POMI data with informatica and cleaned
        prim_pomi_field_dim_df (pd.DataFrame): Table containing field key and column names

    Returns:
        pd.DataFrame: POMI data with field keys altered and new names added
    """
    prim_pomi_field_dim = prim_pomi_field_df.copy()
    prim_pomi_field_dim['Field_Key'] = pd.to_numeric(prim_pomi_field_dim['Field_Key'], downcast='integer').astype(int)

    metadata = pd.merge(
        df.drop(['FACT_Key'],axis=1),
        prim_pomi_field_dim.drop(['Column_Key','Is_Current','Valid_From','Valid_End','SYS_Timestamp'],axis=1),
        how='left',
        on=['Field_Key']
    )
    metadata['Field_Key'] = 'FIELD_KEY_' + pd.to_numeric(metadata['Field_Key'], downcast='integer').astype(str)

    return metadata

def pivot_metadata(df: pd.DataFrame):
    """
    Pivot POMI data to make field key the columns

    Args: 
        df (pd.DataFrame): All POMI data
    
    Returns: 
        pd.DataFrame: Pivoted table with field keys now as columns
    """
    metadata_wide = pd.pivot_table(
        df, 
        values='Field_Value',
        index=['GP_Key','Report_End','SYS_Timestamp'],
        columns='Field_Key'
    ).reset_index().rename_axis(None, axis=1)
    
    return metadata_wide

def join_gp_dim(df: pd.DataFrame, gp_dim_df: pd.DataFrame):
    """
    Join back gp_dim_df to the pivoted data

    Args:
        df (pd.DataFrame): POMI data pivoted
        gp_dim_df (pd.DataFrame): Table containing codes, suppliers and patient list size

    Returns:
        pd.DataFrame: Both tables joined with online book cancel count added
    """
    wide_pracs = pd.merge(
        df, 
        gp_dim_df,
        how='left',
        on=['GP_Key']
    ).sort_values(['Report_End','Supplier','PRACTICE_CODE'])

    wide_pracs['online_book_cancel_count'] = wide_pracs[['FIELD_KEY_49','FIELD_KEY_50','FIELD_KEY_133']].sum(axis=1)
    
    return wide_pracs

def join_mapping(df: pd.DataFrame, mapping_df: pd.DataFrame):
    """
    Join mapping data to POMI data

    Args:
        df (pd.DataFrame): POMI data pivoted
        mapping_df (pd.DataFrame): All mapping data

    Returns:
        pd.DataFrame: Pivoted POMI data with mapping added
    """
    all_pomi = pd.merge(
        df,
        mapping_df,
        how='left',
        on=['PRACTICE_CODE','PRACTICE_NAME','Report_End']
    )
    return all_pomi

def tag_duplicates(df: pd.DataFrame):
    """
    If a practice appears more than once, tag with (I).

    Args:
        df (pd.DataFrame): Pivoted POMI data with mapping added
    
    Returns:   
        pd.DataFrame: Table with duplicate entries tagged
    """
    pomi_tagged = df.copy()
    pomi_tagged['Supplier'] = np.where(
        pomi_tagged.duplicated(['Report_End','PRACTICE_CODE'], keep=False),
        pomi_tagged['Supplier'].str.upper().astype(str) + ' (I)',
        pomi_tagged['Supplier']
    )

    pomi_tagged = pomi_tagged.groupby(by=[
        'Report_End','PRACTICE_CODE','PRACTICE_NAME','Supplier'
        ], as_index=False).max()
    
    return pomi_tagged

def create_all_pomi(
        prim_pomi_df: pd.DataFrame,
        gp_dim_df: pd.DataFrame, 
        exclude_list_df: pd.DataFrame,
        rpsd: str,
        rped: str,
        prim_pomi_inf_df: pd.DataFrame, 
        inf_exclude_list_df: pd.DataFrame,
        prim_pomi_field_df: pd.DataFrame,
        mapping_df: pd.DataFrame
        ) -> pd.DataFrame:
    """
    Run all aggregation functions together to produce a raw POMI dataset

    Args: 
        prim_pomi_df (pd.DataFrame): Containing Field_Keys and Field_Value counts for EMIS, TPP and Vision practices
        gp_dim_df (pd.DataFrame): Containg practice codes, names and suppliers from the GP_Key in 'prim_pomi' dataframes
        exclude_list_df (pd.DataFrame): Excluded timestamps and suppliers for practices
        rpsd (str): Report period start date as defined in params
        rped (str): Report period end date as defined in params
        prim_pomi_inf_df (pd.DataFrame): Containing Field_Keys and Field_Value counts for Informatica
        inf_exclude_list_df (pd.DataFrame): Excluded timestamps and suppliers for Informatica practices
        prim_pomi_field_dim_df (pd.DataFrame): Matches Field_Key integers to descriptive strings
        mapping_df (pd.DataFrame): Practice level mappings, Sub ICB, ICB, and Regions mapped to practices

    Returns:
        df (pd.DataFrame): With all inputs combined, aggregated and filtered. 
    """
    df = (
        combine_pomi_datasets(prim_pomi_df, gp_dim_df)
        .pipe(drop_exclude_list, exclude_list_df, rpsd, rped)
        .pipe(clean_and_join_inf_data, prim_pomi_inf_df, inf_exclude_list_df, rpsd, rped)
        .pipe(create_metadata, prim_pomi_field_df)
        .pipe(pivot_metadata)
        .pipe(join_gp_dim, gp_dim_df)
        .pipe(join_mapping, mapping_df)
        .pipe(tag_duplicates)
    )
    return df 

def create_month_summary_base_data(all_pomi: pd.DataFrame) -> pd.DataFrame:
    """
    Apply column recoding logic to the all_pomi dataset. DataFrame created for month_summary_dataset output.

    Args: 
        all_pomi (pd.DataFrame): All_pomi dataset - all the pomi data combined with mappings

    Returns:
        pd.DataFrame: all_pomi with columns recoded
    """
    df = all_pomi.copy()

    df = recode.replace_column_values_when_less_than(df, 'FIELD_KEY_21', 'FIELD_KEY_126')
    df = recode.replace_column_values_when_less_than(df, 'FIELD_KEY_22', 'FIELD_KEY_127')
    df = recode.replace_column_values_when_less_than(df, 'FIELD_KEY_24', 'FIELD_KEY_130')
    df = recode.replace_column_values_when_less_than(df, 'FIELD_KEY_32', 'FIELD_KEY_132')
    df = recode.replace_column_values_when_less_than(df, 'FIELD_KEY_34', 'FIELD_KEY_134')
    df = recode.replace_column_values_when_less_than(df, 'FIELD_KEY_38', 'FIELD_KEY_140')

    df = recode.replace_column_values_with_sum(df, 'online_book_cancel_count', ['FIELD_KEY_49','FIELD_KEY_133','FIELD_KEY_50'])
    df = recode.replace_column_values_with_sum(df, 'FIELD_KEY_51', ['FIELD_KEY_51','FIELD_KEY_135'])
    df = recode.replace_column_values_with_sum(df, 'FIELD_KEY_55', ['FIELD_KEY_55','FIELD_KEY_141'])

    df = recode.replace_column_values_with_max(df, 'FIELD_KEY_30', ['FIELD_KEY_30','FIELD_KEY_32','FIELD_KEY_34','FIELD_KEY_62','FIELD_KEY_132','FIELD_KEY_134'])

    return df


def create_base_data(all_pomi_recoded: pd.DataFrame) -> pd.DataFrame:
    """
    Apply further column recoding logic to the all_pomi_recoded dataset. DataFrame created for all other outputs.

    Args: 
        all_pomi_recoded (pd.DataFrame): All_pomi_recoded dataset - all the pomi data combined with mappings, with some columns recoded

    Returns:
        pd.DataFrame: all_pomi_recoded with further columns recoded
    """
    df = all_pomi_recoded.copy()

    df = recode.replace_column_values_when_not_equal_two(df, 'FIELD_KEY_32', 'FIELD_KEY_21')
    df = recode.replace_column_values_when_equal_two(df, 'FIELD_KEY_32', 'FIELD_KEY_21', 'FIELD_KEY_33')

    df = recode.replace_column_values_when_not_equal_two(df, 'online_book_cancel_count', 'FIELD_KEY_21')

    df = recode.replace_column_values_when_not_equal_two(df, 'FIELD_KEY_34', 'FIELD_KEY_22')
    df = recode.replace_column_values_when_equal_two(df, 'FIELD_KEY_34', 'FIELD_KEY_22', 'FIELD_KEY_35')

    df = recode.replace_column_values_when_not_equal_two(df, 'FIELD_KEY_51', 'FIELD_KEY_22')

    df = recode.replace_column_values_when_not_equal_two(df, 'FIELD_KEY_42', 'FIELD_KEY_26')
    df = recode.replace_column_values_when_equal_two(df, 'FIELD_KEY_42', 'FIELD_KEY_26', 'FIELD_KEY_43')

    df = recode.replace_column_values_when_not_equal_two(df, 'FIELD_KEY_44', 'FIELD_KEY_26')

    df = recode.replace_column_values_when_not_equal_two(df, 'FIELD_KEY_45', 'FIELD_KEY_27')
    df = recode.replace_column_values_when_equal_two(df, 'FIELD_KEY_45', 'FIELD_KEY_27', 'FIELD_KEY_46')

    df = recode.replace_column_values_when_not_equal_two(df, 'FIELD_KEY_56', 'FIELD_KEY_27')

    df = recode.replace_column_values_when_not_equal_two(df, 'FIELD_KEY_36', 'FIELD_KEY_23')
    df = recode.replace_column_values_when_equal_two(df, 'FIELD_KEY_36', 'FIELD_KEY_23', 'FIELD_KEY_37')

    df = recode.replace_column_values_when_not_equal_two(df, 'FIELD_KEY_38', 'FIELD_KEY_24')
    df = recode.replace_column_values_when_not_equal_two(df, 'FIELD_KEY_55', 'FIELD_KEY_24')
    df = recode.replace_column_values_when_not_equal_two(df, 'FIELD_KEY_40', 'FIELD_KEY_25')
    df = recode.replace_column_values_when_not_equal_two(df, 'FIELD_KEY_54', 'FIELD_KEY_25')
    df = recode.replace_column_values_when_not_equal_two(df, 'FIELD_KEY_62', 'FIELD_KEY_61')
    df = recode.replace_column_values_when_not_equal_two(df, 'FIELD_KEY_63', 'FIELD_KEY_61')

    df = recode.replace_column_values_when_null(df, 'FIELD_KEY_30', 'FIELD_KEY_28')

    df = df[[
        'REGION_CODE','REGION_NAME','ICB_CODE','ICB_NAME','SUB_ICB_CODE','SUB_ICB_NAME','PRACTICE_CODE','PRACTICE_NAME',
        'Report_End','Supplier_Version','Supplier','Total_Patients','FIELD_KEY_21','FIELD_KEY_22','FIELD_KEY_26','FIELD_KEY_27',
        'FIELD_KEY_24','FIELD_KEY_25','FIELD_KEY_61','FIELD_KEY_32','online_book_cancel_count','FIELD_KEY_34',
        'FIELD_KEY_51','FIELD_KEY_42','FIELD_KEY_45','FIELD_KEY_56','FIELD_KEY_7','FIELD_KEY_8','FIELD_KEY_9',
        'FIELD_KEY_10','FIELD_KEY_11','FIELD_KEY_12','FIELD_KEY_13','FIELD_KEY_14','FIELD_KEY_15','FIELD_KEY_16',
        'FIELD_KEY_17','FIELD_KEY_18','FIELD_KEY_20','FIELD_KEY_23','FIELD_KEY_28','FIELD_KEY_29','FIELD_KEY_36',
        'FIELD_KEY_37','FIELD_KEY_48','FIELD_KEY_52','FIELD_KEY_57','FIELD_KEY_58','FIELD_KEY_19','FIELD_KEY_64',
        'FIELD_KEY_39','FIELD_KEY_41','FIELD_KEY_43','FIELD_KEY_46','FIELD_KEY_33','FIELD_KEY_35','FIELD_KEY_66',
        'FIELD_KEY_67','FIELD_KEY_68','FIELD_KEY_49','FIELD_KEY_50','FIELD_KEY_55','FIELD_KEY_40','FIELD_KEY_62',
        'FIELD_KEY_31','FIELD_KEY_30','FIELD_KEY_63','FIELD_KEY_54','FIELD_KEY_47','FIELD_KEY_38','FIELD_KEY_65',
        'FIELD_KEY_60','FIELD_KEY_59','FIELD_KEY_44','FIELD_KEY_53'
        ]].sort_values(by=['Report_End','Supplier','PRACTICE_CODE'])
    
    return df
