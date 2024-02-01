import pandas as pd
from pipeline.utils import params
import datetime
import os
import zipfile
import xlwings

def create_xlsb_file(df, output_folder, data_start, data_end):
    """Creates xlsb file and allows user to run again if errors occured.
    """
    try: 
        xlsb_filepath = f"{output_folder}\\RESTRICTED_POMI_{data_start}_to_{data_end}.xlsb"
        wb = xlwings.Book()
        sht = wb.sheets[0]
        
        # Write column names to the first row
        sht.range("A1").expand('right').value = df.columns.tolist()
        
        # Write DataFrame values starting from the second row
        sht.range("A2").value = df.values
        
        wb.save(xlsb_filepath)
        wb.close()
        
    except Exception as e:
        print("An error occured: The process was interrupted. Please close any excel files that are open.", e)
        while True:
            answer = input("Do you want to try to run the xlsb process again? (Y/N)")
            if answer.upper() == "Y":
                try:
                    create_xlsb_file(df, output_folder, data_end, data_end)
                    break
                except Exception as e:
                    print("An error occured: The process was interrupted. Please close any excel files that are open.", e)
            elif answer.upper() == "N":
                print("Exiting xlsb process, continuing POMI process.")
                break
            else:
                print("Invalid input. Please enter Y or N")


def get_export_location(sub_folder: str) -> str:
    """
    Gets the export folder for all files from the root directory
    Args:
        sub_folder : The sub_folder where the file will be exported
    Returns:
        str: Filepath for all exports to go to 
    """
    root = params.params["ROOT_DIR"]
    output_folder = f"{root}\\OUTPUTS\\{sub_folder}"

    return output_folder


def write_pcd_output(df: pd.DataFrame):
    """
    Writes the main POMI file to the output folder with the correct file name     

    Args:
        df (pd.DataFrame): The final pcd output after all processing has been applied
    """   
    print("Warning! Excel will open while attempting to write a xlsb file. The excel will automatically close when the process is done.")
    output_folder = get_export_location("PUBLICATION")

    data_start = params.get_financial_year_export()
    data_end = params.get_export_dates()
    filename = f"POMI_{data_start}_to_{data_end}.csv"

    output_path = f"{output_folder}\\{filename}"
    df.to_csv(output_path, index=False)

    print("Zipping csv file")
    #zip file
    with zipfile.ZipFile(f"{output_folder}\\POMI_{data_start}_to_{data_end}.zip","w") as zipMe:
        zipMe.write(output_path, arcname=filename, compress_type=zipfile.ZIP_DEFLATED)

    print("Making xlsb file")
    #xlsb file
    create_xlsb_file(df, output_folder, data_start, data_end)


def write_choices_output(df: pd.DataFrame):
    """
    Writes the choices POMI file to the output folder with the correct file name     

    Args:
        df (pd.DataFrame): The final choices POMI output after all processing has been applied
    """   

    output_folder = get_export_location("CHOICES")

    data_end = params.get_export_dates()
    filename = f"CHOICES_POMI_SOURCE_{data_end}.csv"

    output_path = f"{output_folder}\\{filename}"

    df = df.sort_values(by=["practice_code"], ascending=True)

    df.to_csv(output_path, index=False)


def write_benefits_output(df: pd.DataFrame):
    """
    Writes the benefits POMI file to the output folder with the correct file name     

    Args:
        df (pd.DataFrame): The final beenfits output after all processing has been applied
    """   

    output_folder = get_export_location("BENEFITS")

    data_end = params.get_export_dates()
    filename = f"MONTH_SUMMARY_DATASET_{data_end}.csv"

    output_path = f"{output_folder}\\{filename}"

    df.to_csv(output_path, index=False)


def write_pbi_output(df: pd.DataFrame):
    """
    Writes the PowerBI POMI file to the output folder with the correct file name     

    Args:
        df (pd.DataFrame): The final PBI POMI output after all processing has been applied
    """   

    output_folder = get_export_location("PBI")

    filename = f"WORK_POMI_ALL_OUT.csv"

    output_path = f"{output_folder}\\{filename}"

    df.to_csv(output_path, index=False)