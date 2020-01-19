import excel2parquet
from tabulate import tabulate
import glob
import pandas as pd
import pyarrow
import argparse

def read_parquet_folder(parquet_folder):
    parquet_files = glob.glob("{}/*.parquet".format(parquet_folder))
    return pd.concat(pd.read_parquet(f, engine='pyarrow') for f in parquet_files)

def get_max_temp_rec(df):
    # hot_day_df = df.sort_values(by=['ScreenTemperature','ObservationDate','Region',])
    query_columns = ['ScreenTemperature','Date','Region']
    max_tmp = df.ScreenTemperature.max()
    df['Date'] = pd.to_datetime(df['ObservationDate']).dt.strftime('%Y-%m-%d')
    max_tmp_rec = df[df.ScreenTemperature == max_tmp]
    return max_tmp_rec[query_columns].head(1)

def get_max_rec(file_path):
    df = read_parquet_folder(file_path)
    rec = get_max_temp_rec(df)
    print(tabulate(rec, headers='keys', tablefmt='psql'))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--input_location', required=False, default="./input/")
    parser.add_argument('--output_location', required=False, default="./output/")
    args = vars(parser.parse_args())
    parquet_files = glob.glob("{}/*.parquet".format(args['output_location']))
    if len(parquet_files) < 1:
        excel2parquet.to_parquet(args['input_location'],args['output_location'])
    get_max_rec(args['output_location'])
