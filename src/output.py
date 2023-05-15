import pandas as pd
import argparse
import duckdb

def main(args):
    # Connect to the database
    con = duckdb.connect()

    #

    # Close the connection
    con.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Merge chronic conditions')
    parser.add_argument(
        '--input_prefix', 
        type=str,
        default="../data/intermediate/scratch/ccw_proxy", 
        help='Prefix of input file')
    parser.add_argument(
        '--output_prefix', 
        type=str, 
        default="../data/output/ccw_proxy/ccw_proxy",
        help='Prefix of output file')
    args = parser.parse_args()
    main(args)
