import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

@click.command()
@click.option('--user', default='root', help='Username for postgres')
@click.option('--password', default='root', help='Password for postgres')
@click.option('--host', default='pgdatabase', help='Host for postgres')
@click.option('--port', default='5432', help='Port for postgres')
@click.option('--db', default='ny_taxi', help='Database name for postgres')
@click.option('--table_name', default='yellow_taxi_data', help='Name of the table where we will write the results to')
@click.option('--year', default=2021, help='Year of the data')
@click.option('--month', default=1, help='Month of the data')
@click.option('--chunksize', default=100000, help='Size of chunks to be ingested')
def main(user, password, host, port, db, table_name, year, month, chunksize):
    # Construct connection string and URL
    engine = create_engine(f'postgresql+psycopg://{user}:{password}@{host}:{port}/{db}')
    url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{year}-{month:02d}.csv.gz'
    
    print(f"Starting ingestion from URL: {url}")

    # Initialize the iterator
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
        on_bad_lines='warn'
    )

    try:
        # Get the first chunk to create the schema
        first_chunk = next(df_iter)
        
        # Create table (replace if exists)
        first_chunk.head(0).to_sql(name=table_name, con=engine, if_exists="replace", index=False)
        print(f"Table '{table_name}' created / schema set.")

        # Insert first chunk
        first_chunk.to_sql(name=table_name, con=engine, if_exists="append", index=False)
        print(f"Inserted first chunk: {len(first_chunk)} rows")

        # Process remaining chunks
        for chunk in tqdm(df_iter, desc="Inserting chunks"):
            chunk.to_sql(name=table_name, con=engine, if_exists="append", index=False)
            
    except StopIteration:
        print("No data found in the provided URL.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    main()

