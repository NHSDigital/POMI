import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import os
import json

def load_json_config_file(path):
    with open(path) as f:
        file = json.load(f)
    return file
config = load_json_config_file(".\\config.json")

if config["root_directory"] == "":
    current_dir = os.getcwd()
    config["root_directory"] = os.path.dirname(current_dir)

report_month = datetime.datetime.strptime(config["report_run_date"], "%Y-%m-%d")
report_month = report_month.date()

params = {
    "report_month": report_month,
    "ROOT_DIR": config["root_directory"],
    "DATA_FOLDER" : "INPUTS",
}

def get_root() -> str:
    
    return str(params["ROOT_DIR"])

def get_report_period_start_date() -> str:
    
    return str(params["report_month"] + relativedelta(months=-11) + relativedelta(day=31))


def get_report_period_end_date() -> str:

    return str(params["report_month"] + relativedelta(day=31))

def get_report_month() -> str:
    return str(params["report_month"].month)


def get_financial_year_start() -> str:

    if params["report_month"].month >= 4:
        return str(datetime.date(params["report_month"].year, 4, 1))
    else:
        return str(datetime.date(params["report_month"].year - 1, 4, 1))   
   

def get_export_dates() -> str:

    return params["report_month"].strftime("%b%Y").upper()


def get_financial_year_export() -> str:

    financial_year_start = get_financial_year_start()

    return pd.to_datetime(financial_year_start).strftime("%b%Y").upper()