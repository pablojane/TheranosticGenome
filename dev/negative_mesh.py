import os
import json
import pandas as pd
from glob import glob
import pyarrow.parquet as pq

# Script parameters
N = 25000 # Number of output entries
tag = 'D019275:Radiopharmaceuticals' # MESH tag to not include in output
parquet_path = '../test/parquet'  # Path to parquet directory containing all source *.parquet files
output_path = '../data/mesh_negative_data.json' # Path to the filtered output *.json file

# Store *.parquet files metadata
parquet_metadata = dict()

nrows_tot = 0
for file in glob(os.path.join(parquet_path, '*.parquet')):
    metadata = pq.read_metadata(file)
    if all(elem in metadata.schema.names for elem in ['mesh_terms', 'abstract']):
        parquet_metadata[file] = metadata
        nrows_tot += metadata.num_rows
    else:
        print('File ' + file + ' will be ignored since it does not include a "mesh_terms" column')

# Read *.parquet files, filter them and write the desired output to list
output_list = list()
for file in parquet_metadata:
    n = int(parquet_metadata[file].num_rows/nrows_tot*N)
    df = pd.read_parquet(file, engine='pyarrow')
    df = df[~df['mesh_terms'].str.contains(tag)]
    df = df[df['abstract'] != ''] # Do not include entries with empty abstract
    df = df.sample(n=n, random_state = 1) # Random sampling
    for index, row in df.iterrows():
        output_list.append([row['abstract'], {'cats': {'POSITIVE_NUCL_MED': False, 'NEGATIVE_NUCL_MED': True}}])


# Write list to disk in *.json format
with open(output_path, 'w') as f:
   json.dump(output_list, f)
