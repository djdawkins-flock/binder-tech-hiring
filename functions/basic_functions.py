import pandas as pd
import math
import numpy as np


def align_st(df, st_df):
    res_df = pd.concat([st_df, df], axis=1)
    res_df = st_df.merge(df, how='left', on='Service Territory')
    return res_df

def convert_list_date_cols_to_str(df):
    col_list = list(df.columns)
    res_list = [str(x)[:10] for x in col_list]

    return res_list

def convert_date_idx_to_str(df_ser):
    idx_list = list(df_ser.index)
    idx_str_list = [str(x)[:10] for x in idx_list]
    df_ser.index = idx_str_list 

    return df_ser

def get_rolling_df(df):
    res_df = df.cumsum(axis=1)
    return res_df

def get_main_col_list(monthly_inputs):
    main_col_list = convert_list_date_cols_to_str(monthly_inputs)
    return main_col_list

