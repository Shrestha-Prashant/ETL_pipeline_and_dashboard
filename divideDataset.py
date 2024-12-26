import pandas as pd
import os

# loading dataset
data = pd.read_csv('/Users/panda/Downloads/ECommerce_consumer_behaviour.csv')

# print(data.head())
# print(data.info())

chunk_size = 100000     # number of rows per chunk

os.makedirs('/Users/panda/Downloads/row_chunk', exist_ok=True)

# splitting the dataset into chunks and saving them
for i,start_row in enumerate(range(0, len(data), chunk_size)):
    chunk = data.iloc[start_row:start_row+chunk_size]
    chunk.to_csv(f'/Users/panda/Downloads/row_chunk/chunk_{i}.csv', index=False)
    print(f'chunk_{i}.csv saved')


    