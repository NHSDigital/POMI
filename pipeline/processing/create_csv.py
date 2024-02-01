import pandas as pd
import numpy as np
from pipeline.utils import params, rename_columns, csv_functions, recode

def create_pcd_output(df: pd.DataFrame) -> pd.DataFrame:
    """
    Selects columns, changes dataframe to long from wide, and only includes data for the current financial year 
    to be output for the website.
    Args:
        df (pd.DataFrame): All_pomi_adjusted containg all recoded pomi data
    Returns:
        pd.DataFrame: Output as POMI_MMMYYYY_to_MMMYYYY as the publication file
    """
    df = csv_functions.filter_for_financial_year(df, 'Report_End')

    df = pd.melt(
        df,
        id_vars=[
            'Report_End',
            'REGION_CODE',
            'REGION_NAME',
            'SUB_ICB_CODE',
            'SUB_ICB_NAME',
            'PRACTICE_CODE',
            'PRACTICE_NAME',
            'Supplier'
            ],
        value_vars=[
            'Total_Patients',
            'FIELD_KEY_21',
            'FIELD_KEY_32',
            'online_book_cancel_count',
            'FIELD_KEY_22',
            'FIELD_KEY_34',
            'FIELD_KEY_51',
            'FIELD_KEY_61',
            'FIELD_KEY_62',
            'FIELD_KEY_63',
            'FIELD_KEY_30',
            'FIELD_KEY_31',
            'FIELD_KEY_47'
            ],
        var_name='field'
        )

    df['field'].replace(rename_columns.rename_pcd_output, inplace=True) 
    df = df.rename(columns=rename_columns.rename_pcd_output)
    df = df.sort_values(by=[
            'report_period_end',
            'region_code',
            'sub_ICB_location_code',
            'system_supplier',
            'practice_code',
            'field'
            ]
            )
    df = csv_functions.convert_column_datetype(df, 'report_period_end', '%d-%b-%y')
    df = csv_functions.change_values_to_integer(df, ['value'])

    return df


def create_choices_output(df: pd.DataFrame) -> pd.DataFrame:
    """
    Selects columns for choices output, filters for current month, and recode three columns 

    Args: 
        df (pd.DataFrame): All_pomi_adjusted_df containing all pomi data recoded

    Returns:
        pd.DataFrame: Output to be sent to choices
    """
    cols = [
        'Report_End',
        'PRACTICE_CODE',
        'PRACTICE_NAME',
        'Supplier',
        'Total_Patients',
        'FIELD_KEY_21',
        'FIELD_KEY_32',
        'online_book_cancel_count',
        'FIELD_KEY_22',
        'FIELD_KEY_34',
        'FIELD_KEY_51',
        'FIELD_KEY_61',
        'FIELD_KEY_62',
        'FIELD_KEY_63'
        ]
    
    df = df[cols]
    df = csv_functions.filter_for_report_end(df, 'Report_End')
    df = csv_functions.convert_column_datetype(df, 'Report_End', '%d-%b-%y')
    
    df['FIELD_KEY_21'] = np.where(df['FIELD_KEY_21'] == 2, 1, 0)
    df['FIELD_KEY_22'] = np.where(df['FIELD_KEY_22'] == 2, 1, 0)
    df['FIELD_KEY_61'] = np.where(df['FIELD_KEY_61'] == 2, 1, 0)

    df = csv_functions.change_values_to_integer(df, cols[4:])
    df = df.rename(columns=rename_columns.rename_pcd_output)
    return df


def create_benefits_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select columns for the benefits export, filter for current month, and group by suppliers
     
    Args:
        df (pd.DataFrame): All_pomi_recoded, all pomi data with some recoding applied

    Returns:
        pd.DataFrame: File for the benefits export
    """
    cols = [
        'Report_End',
        'Supplier',
        'FIELD_KEY_8',
        'FIELD_KEY_10',
        'FIELD_KEY_11',
        'FIELD_KEY_12',
        'FIELD_KEY_13',
        'FIELD_KEY_14',
        'FIELD_KEY_15',
        'FIELD_KEY_16',
        'FIELD_KEY_17',
        'FIELD_KEY_30',
        'FIELD_KEY_31',
        'FIELD_KEY_40',
        'FIELD_KEY_42',
        'FIELD_KEY_44',
        'FIELD_KEY_45',
        'FIELD_KEY_47',
        'FIELD_KEY_48',
        'FIELD_KEY_49',
        'FIELD_KEY_50',
        'FIELD_KEY_51',
        'FIELD_KEY_52',
        'FIELD_KEY_53',
        'FIELD_KEY_54',
        'FIELD_KEY_56',
        'FIELD_KEY_57'
        ]

    df = df[cols]
    df = csv_functions.filter_for_report_end(df, 'Report_End')
    df = recode.replace_duplicate_supplier_tag(df, 'Supplier')
    df = csv_functions.convert_column_datetype(df, 'Report_End', '%d%b%Y', upper=True)
    df = csv_functions.change_values_to_integer(df, cols[2:])
    df = (df
          .groupby(['Report_End','Supplier'])
          .sum()
          .reset_index()
          )
    df = df.rename(columns=rename_columns.rename_benefits_output)

    return df

def pbi_pivot(code, prefix, pbi_fields_long):
    """
    Pivots the pbi_fields_long table, groups by a specified column and adds a prefix to column names to define whether
    the column is CCG, STP, or region. Returns a wide dataframe grouped by the geography specified
    
    Args:
        code: The column title you want to group by
        prefix: The string you want to add to the column titles
        
    Returns:
        data_wide: A wide dataframe containing the values grouped by, ICB, SUB_ICB, region or nation. With prefixes added
        to column titles containing values
    """
    data = pbi_fields_long.groupby(by=[code,'report_period_end','variable'],
                                as_index=False).agg(
        {
            'GPPracticeCode':'count',
            'value':'sum'
        })

    data['variable'] = prefix + data['variable']

    data_wide = pd.pivot_table(
        data,
        values='value', 
        index=[code,'report_period_end','GPPracticeCode'],
        columns=['variable']
    ).reset_index()
    
    data_wide = data_wide.rename(columns={'GPPracticeCode': prefix + 'PRAC_COUNT'})

    return data_wide

def create_pbi_output(all_pomi_adjusted: pd.DataFrame) -> pd.DataFrame:

    ## Begin the table for PBI outputs
    pbi_fields = all_pomi_adjusted[['REGION_CODE','REGION_NAME','ICB_CODE','ICB_NAME','SUB_ICB_CODE','SUB_ICB_NAME',
                                    'PRACTICE_CODE','PRACTICE_NAME','Supplier','Report_End','Total_Patients','FIELD_KEY_21',
                                    'FIELD_KEY_22','FIELD_KEY_61','FIELD_KEY_32','online_book_cancel_count','FIELD_KEY_30',
                                    'FIELD_KEY_34','FIELD_KEY_51','FIELD_KEY_62','FIELD_KEY_63']]
    pbi_fields.insert(0,'COUNTRY_CODE','E')
    pbi_fields = pbi_fields.rename(columns=
                                {
                                    'REGION_CODE':'RegionCode',
                                    'REGION_NAME':'RegionName',
                                    'PRACTICE_CODE':'GPPracticeCode',
                                    'PRACTICE_NAME':'GPPracticeName',
                                    'Report_End':'report_period_end',
                                    'Total_Patients':'NoPatients',
                                    'FIELD_KEY_21':'APPT_FUNC_FLAG',
                                    'FIELD_KEY_22':'PRESC_FUNC_FLAG',
                                    'FIELD_KEY_61':'DCR_FUNC_FLAG',
                                    'FIELD_KEY_32':'Pat_Appts_Enbld',
                                    'online_book_cancel_count':'Pat_Appts_Use',
                                    'FIELD_KEY_30':'Total_Pat_Enabled',
                                    'FIELD_KEY_34':'Pat_Presc_Enbld',
                                    'FIELD_KEY_51':'Pat_Presc_Use',
                                    'FIELD_KEY_62':'Pat_DetCodeRec_Enbld',
                                    'FIELD_KEY_63':'Pat_DetCodeRec_Use'
                                })
    ## Where a value is 2 set it to 1, if it is not 2 then set it to 0
    pbi_fields['APPT_FUNC_FLAG'] = np.where(pbi_fields['APPT_FUNC_FLAG'] == 2, 1, 0)
    pbi_fields['PRESC_FUNC_FLAG'] = np.where(pbi_fields['PRESC_FUNC_FLAG'] == 2, 1, 0)
    pbi_fields['DCR_FUNC_FLAG'] = np.where(pbi_fields['DCR_FUNC_FLAG'] == 2, 1, 0)

    ## Melt the table to produce a long table rather than a wide table
    pbi_fields_long = pd.melt(
        pbi_fields, 
        id_vars=[
            'COUNTRY_CODE','RegionCode','RegionName','ICB_CODE','ICB_NAME','SUB_ICB_CODE','SUB_ICB_NAME','GPPracticeCode',
            'GPPracticeName','Supplier','report_period_end'], 
        value_vars=[
            'NoPatients','APPT_FUNC_FLAG','PRESC_FUNC_FLAG','DCR_FUNC_FLAG','Pat_Appts_Enbld','Pat_Appts_Use',
            'Total_Pat_Enabled','Pat_Presc_Enbld','Pat_Presc_Use','Pat_DetCodeRec_Enbld','Pat_DetCodeRec_Use'])


    """
    Joins all PBI geography tables together to produce a table with values grouped by mappings.
    
    Returns:
        pomi_all_out: Dataframe that feeds into the PBI dashboard.
    """
    pomi_all_out = pd.merge(
        pbi_fields,
        pbi_pivot('COUNTRY_CODE','NAT_', pbi_fields_long),
        how='left',
        on=['COUNTRY_CODE','report_period_end'])

    pomi_all_out = pd.merge(
        pomi_all_out,
        pbi_pivot('SUB_ICB_CODE','SUB_ICB_',pbi_fields_long),
        how='left',
        on=['SUB_ICB_CODE','report_period_end'])

    pomi_all_out = pd.merge(
        pomi_all_out,
        pbi_pivot('ICB_CODE','ICB_',pbi_fields_long),
        how='left',
        on=['ICB_CODE','report_period_end'])

    pomi_all_out = pd.merge(
        pomi_all_out,
        pbi_pivot('RegionCode','REG_',pbi_fields_long),
        how='left',
        on=['RegionCode','report_period_end'])

    pomi_all_out = pomi_all_out[[
        'COUNTRY_CODE','RegionCode','RegionName','ICB_CODE','ICB_NAME','SUB_ICB_CODE','SUB_ICB_NAME','GPPracticeCode',
        'GPPracticeName','Supplier','report_period_end','NoPatients','REG_PRAC_COUNT','SUB_ICB_PRAC_COUNT',
        'NAT_PRAC_COUNT','APPT_FUNC_FLAG','PRESC_FUNC_FLAG','DCR_FUNC_FLAG','Pat_Appts_Enbld','Pat_Appts_Use',
        'Pat_Presc_Enbld','Pat_Presc_Use','Pat_DetCodeRec_Enbld','Pat_DetCodeRec_Use','NAT_APPT_FUNC_FLAG',
        'NAT_DCR_FUNC_FLAG','NAT_NoPatients','NAT_PRESC_FUNC_FLAG','NAT_Pat_Appts_Enbld','NAT_Pat_Appts_Use',
        'NAT_Pat_DetCodeRec_Enbld','NAT_Pat_DetCodeRec_Use','NAT_Pat_Presc_Enbld','NAT_Pat_Presc_Use',
        'REG_APPT_FUNC_FLAG','REG_DCR_FUNC_FLAG','REG_NoPatients','REG_PRESC_FUNC_FLAG','REG_Pat_Appts_Enbld',
        'REG_Pat_Appts_Use','REG_Pat_DetCodeRec_Enbld','REG_Pat_DetCodeRec_Use','REG_Pat_Presc_Enbld',
        'REG_Pat_Presc_Use','SUB_ICB_APPT_FUNC_FLAG','SUB_ICB_DCR_FUNC_FLAG','SUB_ICB_NoPatients',
        'SUB_ICB_PRESC_FUNC_FLAG','SUB_ICB_Pat_Appts_Enbld','SUB_ICB_Pat_Appts_Use','SUB_ICB_Pat_DetCodeRec_Enbld',
        'SUB_ICB_Pat_DetCodeRec_Use','SUB_ICB_Pat_Presc_Enbld','SUB_ICB_Pat_Presc_Use','ICB_APPT_FUNC_FLAG',
        'ICB_DCR_FUNC_FLAG','ICB_NoPatients','ICB_PRESC_FUNC_FLAG','ICB_Pat_Appts_Enbld','ICB_Pat_Appts_Use',
        'ICB_Pat_DetCodeRec_Enbld','ICB_Pat_DetCodeRec_Use','ICB_Pat_Presc_Enbld','ICB_Pat_Presc_Use',
        'ICB_PRAC_COUNT','Total_Pat_Enabled'
    ]].sort_values(by=['RegionCode'])
    
    ## Change region names to commissioning regions
    pomi_all_out = recode.change_region_names(pomi_all_out, 'RegionName', 'RegionCode')
    
    return pomi_all_out
