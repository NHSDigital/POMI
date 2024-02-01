import pandas as pd

def create_mapping_df(
        open_active_df: pd.DataFrame, 
        sub_icb_mapping_df : pd.DataFrame,
        icb_mapping_df: pd.DataFrame,
        region_mapping_df: pd.DataFrame
        ) -> pd.DataFrame:
    """
    
    """
    practice_mapping = (
        open_active_df
        .merge(sub_icb_mapping_df, how="left", on=["SUB_ICB_CODE"])
        .merge(icb_mapping_df, how="left", on=["ICB_CODE"])
        .merge(region_mapping_df, how="left", on=["REGION_CODE"])
    )[[
        "PRACTICE_CODE",
        "PRACTICE_NAME",
        "SUB_ICB_CODE",
        "SUB_ICB_NAME",
        "ICB_CODE",
        "ICB_NAME",
        "REGION_CODE",
        "REGION_NAME",
        "Report_End",
        ]]
    
    return practice_mapping