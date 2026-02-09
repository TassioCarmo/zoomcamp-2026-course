#!/usr/bin/env python
# coding: utf-8

# In[52]:


from tqdm.auto import tqdm


# In[26]:


import pandas as pd
from sqlalchemy import create_engine


# In[39]:

pg_user = 'root'
pg_password = 'root'
pg_host = 'localhost'
pg_port = '5432'
pg_db = 'ny_taxi'
year = 2021
month = 1


# Read a sample of the data


url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{year}-{month:02d}.csv.gz'
df = pd.read_csv(url, nrows=100)


# In[40]:


# Display first rows
df.head()


# In[41]:


# Check data types
df.dtypes


# In[42]:


# Check data shape
df.shape


# In[43]:




engine = create_engine(f'postgresql+psycopg://root:{pg_password}@{pg_host}:{pg_port}/{pg_db}')


# In[44]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[45]:


df.dtypes


# In[46]:


df.dtypes


# In[47]:


df.head(0)


# In[48]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[49]:


df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# In[53]:

def run():

    chunksize = 100000

    year = 2021
    month = 1

    print("Starting ingestion from URL:", url)

    df_iter = pd.read_csv(
        url,
        chunksize=chunksize,
        compression='gzip',
        dtype={
            'VendorID': 'Int64',
            'passenger_count': 'Int64',
            'RatecodeID': 'Int64',
            'store_and_fwd_flag': str,
            'PULocationID': 'Int64',
            'DOLocationID': 'Int64',
            'payment_type': 'Int64'
        },
        parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
        on_bad_lines='warn'  # helpful for any parsing issues
    )

    print("Creating table from first chunk schema...")

    first_chunk = None
    try:
        first_chunk = next(df_iter)
    except StopIteration:
        print("Iterator empty — no data loaded from URL")
    except Exception as e:
        print("Error advancing iterator:", str(e))

    if first_chunk is not None and not first_chunk.empty:
        # Create table schema
        first_chunk.head(0).to_sql(
            name="yellow_taxi_data",
            con=engine,
            if_exists="replace",
            index=False
        )
        print("Table created / schema set")

        # Insert first chunk
        first_chunk.to_sql(
            name="yellow_taxi_data",
            con=engine,
            if_exists="append",
            index=False
        )
        print(f"Inserted first chunk: {len(first_chunk)} rows")

        # Remaining chunks
        chunk_count = 1
        for chunk in tqdm(df_iter, desc="Inserting chunks"):
            chunk.to_sql(
                name="yellow_taxi_data",
                con=engine,
                if_exists="append",
                index=False
            )
            print(f"Inserted chunk {chunk_count}: {len(chunk)} rows")
            chunk_count += 1
    else:
        print("No data found — check network, URL, or try downloading locally.")


if __name__ == "__main__":
    run()



