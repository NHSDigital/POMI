import pandas as pd
import numpy as np
from pipeline.utils import rename_columns
from datetime import datetime as ddt
import os
import glob
from ..data import input
from ..utils.params import *

def build_trend_monitor_time_series_comparison(df: pd.DataFrame, function, cols: list) -> pd.DataFrame:
    """
    Compare a month to previous month for the 12 month time series in all_pomi_adjusted_df

    Args:
        df (pd.DataFrame): Data containing all POMI data
        function: The function to be used to compare each month to previous
        cols (list): List of columns to rename the output table with
    
    Returns:
        pd.DataFrame: Time series for the function applied with columns renamed
    """
    output = []
    all_dates = get_list_of_months(df)
    
    for index, i in enumerate(all_dates):
        if index < (len(all_dates) - 1):
            data = function(df, all_dates[index], all_dates[index + 1])
            
        output.append(data)
        
    time_series_df = pd.DataFrame(output).head(11)
    
    time_series_df.columns = cols
    return time_series_df

def get_list_of_months(df: pd.DataFrame):
    """
    Using the input data frame, create a list of all months included
    """
    all_dates = list(df.sort_values(['Report_End'], ascending=False)['Report_End'].drop_duplicates())
    
    return all_dates

def create_registered_gp_patient_list_size(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create the list size output for the first tab of the trend monitor
    
    Args:
        df (pd.DataFrame): DataFrame containing all pomi data for last 12 months
    
    Returns:
        pd.DataFrame: List size output for trend monitor
    """
    df = (
        df
        .groupby(['Report_End'])
        .sum()
        .reset_index()
        .sort_values(['Report_End'], ascending=False)
        [['Report_End','Total_Patients']]
    )
    df['Monthly Change'] = df['Total_Patients'].diff(periods=-1)
    df['% change'] = round(df['Total_Patients'].pct_change(periods=-1), 4)
    
    df.columns = ['Date','Monthly patient list size', 'Monthly Change', '% change']
    return df

def create_number_of_patients_enabled(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create the number of patients enabled output for the second tab of the trend monitor
    
    Args:
        df (pd.DataFrame): DataFrame containing all pomi data for last 12 months
        
    Returns:
        pd.DataFrame: Number of patients enabled output for trend monitor
    """
    df = (
        df
        .groupby(['Report_End'])
        .sum()
        .reset_index()
        .sort_values(['Report_End'], ascending=False)
        [['Report_End','FIELD_KEY_30']]
    )
    df['Monthly Change'] = df['FIELD_KEY_30'].diff(periods=-1)
    df['% change'] = round(df['FIELD_KEY_30'].pct_change(periods=-1), 4)
    
    df.columns = ['Date', 'Monthly number of patients enabled online', 'Monthly change', '% change']
    return df

def create_transaction_volumes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create the transaction volumes for the third tab of the trend monitor
    
    Args:
        df (pd.DataFrame): DataFrame containing all pomi data for last 12 months
        
    Returns:
        pd.DataFrame: Transaction volumes output for trend monitor
    """
    df = (
        df
        .groupby(['Report_End'])
        .sum()
        .reset_index()
        .sort_values(['Report_End'], ascending=False)
        [['Report_End','FIELD_KEY_47']]
    )
    df['Monthly Change'] = df['FIELD_KEY_47'].diff(periods=-1)
    
    df.columns = ['Date', 'Total online transaction count', 'Monthly change']
    return df

def compare_number_of_practices(df: pd.DataFrame, curr_date: str, prev_date: str):
    """
    Helper function to compare numbers, and lists, of practices month to month.
    
    Args:
        df (pd.DataFrame): DataFrame containing all pomi data for last 12 months
        curr_date (str): Recent date in the format from df
        prev_date (str): One month previous from curr_date in the same format as df
    
    Returns:
        pd.DataFrame: Containing the report end date, number, and list, of practices no longer in output and
                      number, and list, of new practices compared to previous month
    """
    current_practices = set(list(df.loc[df['Report_End'] == curr_date]['PRACTICE_CODE']))
    previous_practices = set(list(df.loc[df['Report_End'] == prev_date]['PRACTICE_CODE']))
    
    difference_practices = sorted(previous_practices.difference(current_practices))
    difference_practices_list = len(list(previous_practices.difference(current_practices)))
    
    new_practices = sorted(current_practices.difference(previous_practices))
    new_practices_list = len(list(current_practices.difference(previous_practices)))
    
    return curr_date, difference_practices_list, ', '.join(map(str,sorted(difference_practices))), new_practices_list, ', '.join(map(str,sorted(new_practices)))

def create_practices_list_change(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create the practices list change for the fourth tab of the trend monitor
    
    Args:
        df (pd.DataFrame): DataFrame containing all pomi data for last 12 months
        
    Returns:
        pd.DataFrame: Practices list change output for trend monitor
    """
    practices_list_change = build_trend_monitor_time_series_comparison(
        df, 
        compare_number_of_practices, 
        [
            "Date", 
            "Number of practices in previous month that aren't in the current month", 
            "List of practices in previous month that aren't in the current month",
            "Number of new practices",
            "List of new practices"
        ]
    )
    return practices_list_change

def selector(row, current_month, last_month):
    """
    Search a dataframe row by row. Return true if the metric has decreased by 1 between this and last month
    """
    if row[current_month] == 1 and row[last_month] == 2 :
        return True
    else:
        return False
    
def online_enabled(df: pd.DataFrame, curr_date: str, prev_date: str):
    """
    Compare a month to the previous month to find practics that have disabled specific services. 
    This function will be looped to create a month by month time series

    Args:
        df (pd.DataFrame): All POMI data
        curr_date (str): A date within the df
        prev_date (str): One month subtracted from curr_date

    Returns:
        str: curr_date
        list: Ordered lists of practices that have disabled their services for each of the field keys defined in columns  
    """
    columns = [
        'PRACTICE_CODE',
        'FIELD_KEY_21',
        'FIELD_KEY_22',
        'FIELD_KEY_24',
        'FIELD_KEY_25',
        'FIELD_KEY_61'
    ]

    current_subset = df.loc[df['Report_End'] == curr_date].loc[:, df.columns.isin(columns)]
    previous_subset = df.loc[df['Report_End'] == prev_date].loc[:, df.columns.isin(columns)]

    joined_df = pd.merge(current_subset, previous_subset, on=['PRACTICE_CODE'], how='inner', suffixes=["_new", "_old"])

    new_list = [sub + '_new' for sub in columns[1:]]
    old_list = [sub + '_old' for sub in columns[1:]]

    titles = list(zip(new_list, old_list))

    data_dict = {}
    for i, j in titles: 

        df = joined_df.loc[:,joined_df.columns.isin(["PRACTICE_CODE", i, j])]

        practices = list(df[df.apply(lambda row : selector(row, i, j), axis=1)]['PRACTICE_CODE'])

        data_dict[i[:-4]] = practices

    output = []
    for k, v in data_dict.items():
        output.append(v)

    return curr_date, ', '.join(map(str,sorted(output[0]))), ', '.join(map(str,sorted(output[1]))), ', '.join(map(str,sorted(output[2]))), ', '.join(map(str,sorted(output[3]))), ', '.join(map(str,sorted(output[4])))

def create_online_services_enabled_status(df: pd.DataFrame) -> pd.DataFrame:
    """
    Using online_enabled() and build_trend_monitor_time_series_comparison(), create a time series for online enabled and stack the rows into a dataframe.
    Rename columns to match descriptions of field keys
    """
    services_enabled = build_trend_monitor_time_series_comparison(
        df, 
        online_enabled, 
        [
            'Date',
            'Online appointment enabled status',
            'Online prescription enabled status',
            'Online summary record view enabled status',
            'Online record view enabled status',
            'Coded record view enabled status'
        ]
    )
    return services_enabled

def total_transactions_counts(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """
    Check for practices where sum of all transactions is greater than total transactions

    Args:
        df (pd.DataFrame): All POMI data
        date (str): A date within the df

    Returns:
        str: date
        list: Ordered lists of practices where sum of all transactions is greater than total transactions 
    """
    columns = [
        'PRACTICE_CODE',
        'online_book_cancel_count', 
        'FIELD_KEY_51',
        'FIELD_KEY_63', 
        'FIELD_KEY_47'
    ]

    current_subset = df.loc[df['Report_End'] == date].loc[:, df.columns.isin(columns)]

    current_subset['sum_transactions'] = current_subset.iloc[:, 1:4].sum(axis=1)

    transactions = current_subset.query("sum_transactions > FIELD_KEY_47").iloc[:,0].tolist()

    return date, len(transactions), ', '.join(map(str,sorted(transactions)))

def create_total_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Loop through all months in all_pomi_adjusted to find any practices that fit criteria in total_transactions_counts()
    """
    output = []
    all_dates = get_list_of_months(df)
    
    for i in all_dates:
        data = total_transactions_counts(df, i)
        output.append(data)
        
    time_series_df = pd.DataFrame(output)
    time_series_df.columns = [
        'Date', 
        'Number of total transactions for appts, prescriptions and DCR views is greater than the total transactions',
        'Practices where total transactions for appts, prescriptions and DCR view is greater than the total transactions'
    ]
    
    return time_series_df

def perc_patients_enabled(df: pd.DataFrame, curr_date: str, prev_date: str):
    """
    Find practices for most recent month with more online patients than total patients. 
    Check if they had the same issue last month

    Args:
        df (pd.DataFrame): All POMI data
        curr_date (str): Most recent date
        prev_date (str): Previous month
    
    Returns:
        pd.DataFrame: Containing all practices with higher count of online patients than total patients
    """
    patients_enabled = df.copy()
    patients_enabled = patients_enabled[['Report_End', 'PRACTICE_CODE', 'FIELD_KEY_30', 'Total_Patients']]

    patients_enabled['percentage_enabled'] = round(100*(patients_enabled['FIELD_KEY_30']/patients_enabled['Total_Patients']), 2)

    patients_enabled_greater_100 = patients_enabled.loc[patients_enabled['percentage_enabled'] >= 100]
    current_patients_enabled_greater_100 = (
        patients_enabled_greater_100
        .loc[patients_enabled_greater_100['Report_End'] == curr_date]
        [['PRACTICE_CODE', 'FIELD_KEY_30', 'Total_Patients','percentage_enabled']]
    )

    prev_patients_enabled_greater_100 = (
        patients_enabled_greater_100
        .loc[patients_enabled_greater_100['Report_End'] == prev_date]
    )

    current_patients_enabled_greater_100['Inc last month'] = np.where(
        current_patients_enabled_greater_100['PRACTICE_CODE'].isin(list(prev_patients_enabled_greater_100['PRACTICE_CODE'].values)),
        'True',
        'False'
    )

    current_patients_enabled_greater_100.columns = [
        'Practice Code',
        'Online Patient Count',
        'Total Patients',
        'Percentage Enabled',
        'Included last month?'
    ]

    return current_patients_enabled_greater_100

def check_CQRS_participation(root : str, df: pd.DataFrame):
    """
    Checks if practice codes are approved and if not, display practices in the trend monitor.
    Args:
    status_df: The QS part status file
    prac_df: The praticipation file
    """

    prac_df, status_df = input.get_practicipation_dataframes(root)
    df_cols = [col for col in list(status_df.columns) if col.startswith("Unnamed") == False]
    status_df = status_df[df_cols]

    pracs = status_df[["Service\nProvider Id","Status", "Status Date"]]
    prac_qs_status = list(status_df["Service\nProvider Id"].unique())
    prac_check = []

    for i in range(len(pracs)):
        row = list(pracs.iloc[i])
        prac_code, status, status_date = row
        status_date = ddt.strptime(status_date, "%d/%m/%Y")
        expiry_date = ddt.strptime(get_report_period_start_date(),"%Y-%m-%d")
        
        if status == "Approved" and status_date <= expiry_date:
            prac_check.append(prac_code)
        
    prac_list = list(prac_df["PRACTICE_CODE"].unique())
    prac_not_approved = [prac for prac in prac_list if prac not in prac_check]

    report_end = df["Report_End"].sort_values(ascending=False).unique()[0]
    df = df[df["Report_End"] == report_end]
    
    prac_status = []
    prac_in_pub = list(df["PRACTICE_CODE"].unique())

    for prac in prac_not_approved:
        if prac in prac_in_pub:
            if prac in prac_qs_status:
                prac_status.append("Rejected")
            else:
                prac_status.append("In publication but not on QS status")
        else:
            prac_status.append("Practice not in publication")
    
    excel_df = pd.DataFrame({"Date":report_end, "Practices not approved":prac_not_approved, "Practice status":prac_status})
    
    return excel_df

def create_percentage_patients_enabled(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run perc_patients_enabled() for most recent month
    """
    current_date = get_list_of_months(df)[0]
    previous_date = get_list_of_months(df)[1]
    
    patients_enabled = perc_patients_enabled(df, current_date, previous_date)

    return patients_enabled

def trend_monitor_comparison_base_data(input_df: pd.DataFrame, supplier: str) -> pd.DataFrame:
    """
    Take all_pomi_adjusted and clean the data. Select most recent 3 months, select a single supplier, and group specified columns by practice.

    Args:
        input_df (pd.DataFrame): All POMI data
        supplier (str): A single supplier to run the comparison on

    Returns:
        pd.DataFrame: Data ready for month by month comparisons 
    """
    df = input_df.copy()

    df['Supplier'] = np.where(df['Supplier'] == 'EMIS (I)', 'EMIS', df['Supplier'])
    df['Supplier'] = np.where(df['Supplier'] == 'VISION (I)', 'VISION', df['Supplier'])
    df['Supplier'] = np.where(df['Supplier'] == 'TPP (I)', 'TPP', df['Supplier'])

    df = (
        df
        .loc[df['Report_End'].isin(get_list_of_months(input_df)[0:3])]
        .loc[df['Supplier'] == supplier]
        .groupby(['Report_End','Supplier'])
        .sum()
        .reset_index()
        .sort_values(['Report_End'], ascending=False) 
    )[[
        'Report_End',
        'Supplier',
        'FIELD_KEY_32',
        'online_book_cancel_count',
        'FIELD_KEY_34',
        'FIELD_KEY_51',
        'FIELD_KEY_62',
        'FIELD_KEY_63',
        'FIELD_KEY_30',
        'FIELD_KEY_31',
        'FIELD_KEY_47'
    ]]
    
    return df

def calculate_differences(df: pd.DataFrame):
    """
    Calculate the counts as a difference from the previous week. 
    Title the new dataframe as Appointment Count Week on Week Variation (Counts), and drop the column titles.
    
    Args:
        df (pd.DataFrame): Dataframe created by base_data fucntion
        
    Returns:
        pd.DataFrame: Dataframe containing all suppliers data difference from the previous week for a single metric
    """
    df2 = df.copy()
    for cols in df2.columns[2:]:
        df2[cols] = df2[cols].diff(periods=-1)
        
    return df2[:2]

def calculate_percentage_change(df: pd.DataFrame):
    """
    Calculate the counts as a percentage difference from the previous week. 
    Title the new dataframe as Appointment Count Week on Week Variation (Percentage), and drop the column titles.
    
    Args:
        df (pd.DataFrame): Dataframe created by base_data fucntion
        
    Returns:
        pd.DataFrame: Dataframe containing all suppliers data as a percentage change from previous week for a 
        single metric
    """
    df2 = df.copy()
    for cols in df2.columns[2:]:
        df2[cols] = round(df2[cols].pct_change(periods=-1), 3)
    
    return df2[:2]

def create_month_by_month_comparison(all_pomi_adjusted: pd.DataFrame):
    """
    Using table created in trend_monitor_comparison_base_data(), loop through each supplier and append to one another.
    Then calculate month on month differences, and percentage change 

    Args: 
        all_pomi_adjusted (pd.DataFrame): All POMI data
    
    Returns:
        pd.DataFrame: 3 dataframes with counts, differences, and percentages to be appended when exported
    """
    input_df = all_pomi_adjusted.copy()
    
    supplier_list = [
        'EMIS',
        'TPP',
        'VISION',
    ]
    counts = []
    differences = []
    percentages = []

    for supplier in supplier_list:

        df = trend_monitor_comparison_base_data(input_df, supplier)
        counts.append(df[:2])

        diff = calculate_differences(df)
        differences.append(diff) 

        perc = calculate_percentage_change(df)
        percentages.append(perc)    

    data = pd.concat(counts)
    differences = pd.concat(differences)
    percentages = pd.concat(percentages)

    data.rename(columns=rename_columns.rename_pcd_output, inplace = True)
    differences.rename(columns=rename_columns.rename_pcd_output, inplace = True)
    percentages.rename(columns=rename_columns.rename_pcd_output, inplace = True)

    return data, differences, percentages
