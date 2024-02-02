import pandas as pd
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
from pipeline.utils import params
from pipeline.output import csv_export
from pipeline.data import input
from pipeline.processing import create_trend_monitor
import os

def write_table_to_sheet(wb, table_data, sheet_name, start_cell):
    
    ws = wb[sheet_name]
    
    start_cell = openpyxl.utils.cell.coordinate_to_tuple(start_cell)
    
    rows_to_write = dataframe_to_rows(table_data, index=False, header=True)
    loc = list(start_cell)
    for row in rows_to_write:
        for cell in row:
            ws.cell(row=loc[0], column=loc[1]).value = cell
            loc[1] += 1
        loc[0] += 1
        loc[1] = start_cell[1]
        
    return wb

def write_trend_monitor(df: pd.DataFrame):
## move function to config
    output_folder = csv_export.get_export_location("TREND MONITOR")
    data_end = params.get_export_dates()
    root = params.get_root()
    wb = openpyxl.load_workbook(input.get_trend_monitor_template_path())

    wb = write_table_to_sheet(wb=wb, sheet_name="Registered GP patient list size", table_data=create_trend_monitor.create_registered_gp_patient_list_size(df), start_cell='A2')
    wb = write_table_to_sheet(wb=wb, sheet_name="Number of patients enabled", table_data=create_trend_monitor.create_number_of_patients_enabled(df), start_cell='A2')
    wb = write_table_to_sheet(wb=wb, sheet_name="Transaction volumes", table_data=create_trend_monitor.create_transaction_volumes(df), start_cell='A2')
    wb = write_table_to_sheet(wb=wb, sheet_name="Practices list change", table_data=create_trend_monitor.create_practices_list_change(df), start_cell='A2')
    wb = write_table_to_sheet(wb=wb, sheet_name="Online services enabled status", table_data=create_trend_monitor.create_online_services_enabled_status(df), start_cell='A3')
    wb = write_table_to_sheet(wb=wb, sheet_name="Total transactions", table_data=create_trend_monitor.create_total_transactions(df), start_cell='A2')
    wb = write_table_to_sheet(wb=wb, sheet_name="% Patients enabled", table_data=create_trend_monitor.create_percentage_patients_enabled(df), start_cell='A4')
    wb = write_table_to_sheet(wb=wb, sheet_name="Month by month comparison", table_data=create_trend_monitor.create_month_by_month_comparison(df)[0], start_cell='A5')
    wb = write_table_to_sheet(wb=wb, sheet_name="Month by month comparison", table_data=create_trend_monitor.create_month_by_month_comparison(df)[1], start_cell='A15')
    wb = write_table_to_sheet(wb=wb, sheet_name="Month by month comparison", table_data=create_trend_monitor.create_month_by_month_comparison(df)[2], start_cell='A25')
    wb = write_table_to_sheet(wb=wb, sheet_name="Participation", table_data=create_trend_monitor.check_CQRS_participation(root, df), start_cell='A1')

    wb.save(f'{output_folder}\\POMI_Trend_Monitor_{data_end}.xlsx')
