import os
import pandas as pd
import numpy as np

def get_local_dtb_dir(database_version):
    """Lấy direction """

    onedrive_dir = os.path.expandvars("%OneDriveConsumer%")
    if database_version < 31:
        local_dtb_dir = os.path.join(onedrive_dir, f"Amethyst Invest\Database\DATABASEv{database_version}")
    else:
        local_dtb_dir = os.path.join(onedrive_dir, f"Amethyst Invest\Database\db{database_version}")
    return local_dtb_dir


def get_stem_for_indicator(indicator, database_version):
    try:
        local_dtb_dir = get_local_dtb_dir(database_version)
        save_file_name = "ilut.parquet"
        ilut_df = pd.read_parquet(os.path.join(local_dtb_dir, save_file_name), engine= 'fastparquet')
        output = ilut_df[ilut_df["indicator"] == indicator]["stem"].values[0]
        return output
    except IndexError:
        print(f"I am having a problem with indicator: {indicator}")
        return np.nan


def get_factor_for_indicator(indicator, database_version):
    try:
        local_dtb_dir = get_local_dtb_dir(database_version)
        save_file_name = "ilut.parquet"
        ilut_df = pd.read_parquet(os.path.join(local_dtb_dir, save_file_name), engine= 'fastparquet')
        output = ilut_df[ilut_df["indicator"] == indicator]["factor"].values[0]
        return output
    except IndexError:
        print(f"I am having a problem with indicator: {indicator}")
        return np.nan


def load_indicator(df, database_version, indicator):
    """lấy indicator từ kho onedrive

    Args:
        df: Tên dataframe được lắp indicator vào
        indicator (string): tên indicator sử dụng
    """
    if indicator not in df.columns:
        local_dtb_dir = get_local_dtb_dir(database_version)
        factor = get_factor_for_indicator(indicator, database_version)
        stem = get_stem_for_indicator(indicator, database_version)
        df_tem = pd.read_parquet(os.path.join(local_dtb_dir,f"i-{factor}",f'{stem}.parquet'), engine= 'fastparquet')
        df = pd.merge(df, df_tem, how= 'left', on=['ticker', 'date'])   
        df = df.drop_duplicates(subset= ['ticker','date']).reset_index(drop= True)
    
    return df


def load_indicator_list(df, database_version, indicator_list):
    """Lấy list indicator lắp vào với nhau
       
    Args:
        df: Tên dataframe được lắp indicator vào
        database_version: là 15, 17, 19, hay 21 hay bất kỳ version nào mới hơn
        indicator (string): tên indicator sử dụng"""

    for indicator in indicator_list:
        df = load_indicator(df, database_version, indicator)
    return df


def load_pca(database_version):
    """Khởi tạo 1 dataframe bắt đầu với indicator Volume

    Args:
        df: Tên dataframe được lắp indicator vào
        database_version: là 15, 17, 19, hay 21 hay bất kỳ version nào mới hơn
        indicator (string): chính là tên indicator VALM
    """
    indicator = 'pca'
    local_dtb_dir = get_local_dtb_dir(database_version)
    factor = get_factor_for_indicator(indicator, database_version)
    stem = get_stem_for_indicator(indicator, database_version)

    df = pd.read_parquet(os.path.join(local_dtb_dir,f"i-{factor}",f'{stem}.parquet'), engine= 'fastparquet')
    df = df.drop_duplicates(subset= ['ticker','date']).reset_index(drop = True)

    return df