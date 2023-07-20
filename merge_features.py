import argparse
import json
import duckdb
#import pyarrow.parquet as pq

def main(args):
    with open(args.ccw_json, 'r') as json_file:
        ccw_dict = json.load(json_file)

    conditions_list = list(ccw_dict.keys())

    # Connect to the database
    con_ccw = duckdb.connect()
    
    # Define the initial SQL query using the first table as the base table
    c_ = conditions_list[0]

    print(f"## Merging {c_} ----")
    #df = pq.read_table(f"{args.input_prefix}_{c_}_{args.year}.parquet").to_pandas()
    # print(f"## {df.shape[0]} rows in {c_}")
    # print(f"## {df.shape[1]} columns in {c_}")
    # print(df.columns)
    # print(f"## {df.memory_usage().sum() / 1e6} MB memory usage in {c_}")
    # print(f"## {df.memory_usage().sum() / df.shape[0]} bytes per row in {c_}")
    # print(f"## {df.memory_usage().sum() / df.shape[1]} bytes per column in {c_}")
    
    query = f"SELECT bene_id, {c_}, {c_}_ever FROM '{args.input_prefix}_{c_}_{args.year}.parquet'"
    result = con_ccw.execute(query).fetchdf()
    result.to_parquet(f"{args.scratch_prefix}_{args.year}.parquet")

    # Iterate over the remaining tables and perform left joins
    for i_ in range(1, len(conditions_list)):
        c_ = conditions_list[i_]
        print(f"## Merging {c_} ----")
        query = f"SELECT bene_id, {c_}, {c_}_ever FROM '{args.input_prefix}_{c_}_{args.year}.parquet'"
        # Update the query with the left join
        result = con_ccw.execute(f"""
            SELECT * FROM ({query})
            INNER JOIN '{args.scratch_prefix}_{args.year}.parquet'
            USING(bene_id)
        """).fetchdf()
        result.to_parquet(f"{args.scratch_prefix}_{args.year}.parquet")

    print("## Writing data ----")
    result = result.set_index(['bene_id'])
    result['rfrnc_yr'] = args.year
    output_file = f"{args.output_prefix}_{args.year}.{args.output_format}"
    if args.output_format == "parquet":
        result.to_parquet(output_file)
    elif args.output_format == "feather":
        result.to_feather(output_file)
    elif args.output_format == "csv":
        result.to_csv(output_file)

    print(f"## Output file written to {output_file}")

    # Close the connection
    con_ccw.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Merge chronic conditions')
    parser.add_argument(
        "--year", 
        default = 2000, 
        type=int)
    parser.add_argument(
        '--input_prefix',
        default="./data/intermediate/prev_hosp/prev_hosp", 
        help='Prefix of input file')
    parser.add_argument(
        '--output_prefix',
        default="./data/output/prev_hosp/prev_hosp",
        help='Prefix of output file')
    parser.add_argument(
        '--scratch_prefix',
        default="./data/intermediate/prev_hosp/prev_hosp",
        help='Prefix of output file')
    parser.add_argument(
        "--ccw_json", 
        default = "./data/input/remote_data/ccw.json")
    parser.add_argument(
        "--output_format", 
        default = "parquet", 
        choices=["parquet", "feather", "csv"])  
    args = parser.parse_args()
    main(args)

# python merge_features.py --year 2000 --input_prefix ./data/intermediate/prev_hosp/prev_hosp --output_prefix ./data/output/prev_hosp/prev_hosp --ccw_json ./data/input/remote_data/ccw.json --output_format parquet
