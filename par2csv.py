import os
import polars as pl

def convert_nested_to_string(value):
    if isinstance(value, (list, tuple)):
        return ",".join(str(v) for v in value)
    elif isinstance(value, dict):
        return ",".join(f"{k}:{v}" for k, v in value.item())
    else:
        return str(value)

def convert_parquet_to_csv(input_file, output_file, chunk_size=100000):
    # Read Parquet file as a Polars LazyFrame
    lf = pl.scan_parquet(input_file)

    # Convert LazyFrame to DataFrame in chunk_size
    header_written = False
    for i, series_chunk in enumerate(lf.collect(rechunk=chunk_size)):
        # Convert series to DataFrame
        df_chunk = series_chunk.to_frame()

        # Convert nested data to strings
        for col in df_chunk.columns:
            if df_chunk[col].dtype.is_nested():
                df_chunk = df_chunk.with_columns(
                    pl.col(col).map_elements(lambda x:convert_nested_to_string(x), return_dtype=pl.Utf8)
                )

        #Write each chunk to CSV file
        if not header_written:
            df_chunk.write_csv(output_file, include_header=True)
            header_written = True
        else:
            with open(output_file, 'a') as f:
                df_chunk.write_csv(f, include_header=False)

#Replace with the desired output CSV file path
input_parquet_file = '/d/downloads/medqar.parquet'
output_csv_file = '/d/downloads/medpqar.csv'

os.makedirs(os.path.dirname(output_csv_file), exist_ok=True)

convert_parquet_to_csv(input_parquet_file, output_csv_file, chunk_size=100000)
