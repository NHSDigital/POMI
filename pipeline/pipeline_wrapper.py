from pipeline.utils import params
from pipeline.data import input
from pipeline.processing import mapping, aggregate, create_csv
from pipeline.output import csv_export, excel_export
import pandas as pd
import subprocess

def run(config: dict) -> None:

    print("Getting report period")
    rpsd = params.get_report_period_start_date()
    rped = params.get_report_period_end_date()

    print("Establishing SQL connection")
    pomi_connection = input.create_sql_connection(config["pomi_connection_string"])
    mapping_connection = input.create_sql_connection(config["mapping_connection_string"])

    print("Importing POMI data")
    gp_dim_sql_str, prim_pomi_sql_str, prim_pomi_inf_sql_str, prim_pomi_field_sql_str, gpes_sites_sql_str = input.get_pomi_sql_strings(rpsd, rped)

    print("getting gp_dim_df data")
    gp_dim_df = input.get_sql_data(gp_dim_sql_str, pomi_connection)

    print("getting prim_pomi_df data")
    prim_pomi_df = input.get_sql_data(prim_pomi_sql_str, pomi_connection)

    print("getting prim_pomi_inf_df data")
    prim_pomi_inf_df = input.get_sql_data(prim_pomi_inf_sql_str, pomi_connection)

    print("getting prim_pomi_field_df data")
    prim_pomi_field_df = input.get_sql_data(prim_pomi_field_sql_str, pomi_connection)

    print("Reading CSVs")
    print("Getting exclude_list_df")
    exclude_list_df = input.get_exclude_list()
    print("Getting inf_exclude_list_df")
    inf_exclude_list_df = input.get_inf_exclude_list()

    print("Importing mapping data")
    open_active_sql_str, sub_icb_mapping_sql_str, icb_mapping_sql_str, region_mapping_sql_str = input.get_mapping_sql_query_strings(rpsd, rped)

    open_active_df = input.get_sql_data(open_active_sql_str, pomi_connection)
    sub_icb_mapping_df = input.get_sql_data(sub_icb_mapping_sql_str, mapping_connection)
    icb_mapping_df = input.get_sql_data(icb_mapping_sql_str, mapping_connection)
    region_mapping_df = input.get_sql_data(region_mapping_sql_str, mapping_connection)

    mapping_df = mapping.create_mapping_df(open_active_df, sub_icb_mapping_df, icb_mapping_df, region_mapping_df)

    print("Building base data")
    all_pomi_df = aggregate.create_all_pomi(
        prim_pomi_df,
        gp_dim_df, 
        exclude_list_df,
        rpsd,
        rped,
        prim_pomi_inf_df, 
        inf_exclude_list_df,
        prim_pomi_field_df,
        mapping_df
        )

    all_pomi_recoded_df = aggregate.create_month_summary_base_data(all_pomi_df)
    all_pomi_adjusted_df = aggregate.create_base_data(all_pomi_recoded_df)

    print("Creating outputs")
    pcd_output_df = create_csv.create_pcd_output(all_pomi_adjusted_df)
    choices_output_df = create_csv.create_choices_output(all_pomi_adjusted_df)
    benefits_output_df = create_csv.create_benefits_dataset(all_pomi_recoded_df)
    pbi_output_df = create_csv.create_pbi_output(all_pomi_adjusted_df)

    print("Exporting files")
    csv_export.write_pcd_output(pcd_output_df)
    csv_export.write_choices_output(choices_output_df)
    csv_export.write_benefits_output(benefits_output_df)
    csv_export.write_pbi_output(pbi_output_df)
    excel_export.write_trend_monitor(all_pomi_adjusted_df)

    print("POMI Job completed. Opening Outputs folder")
    root = params.params["ROOT_DIR"]
    output_folder = f"{root}\\OUTPUTS"
    subprocess.Popen(["explorer", output_folder])
