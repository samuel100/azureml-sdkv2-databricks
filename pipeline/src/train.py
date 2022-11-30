import os
import glob
import argparse
import pandas as pd

# parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input", type=str)
args = parser.parse_args()

# read the input files
df = pd.read_parquet(args.input)
print(df)
