import numpy as np
import pandas as pd

import json
import os
import sys
import argparse

import duckdb

# diagnoses && ARRAY is not supported in duckdb

def read_ccw_json(ccw_json, condition):
    with open(ccw_json, 'r') as json_file:
        ccw_dict = json.load(json_file)
    
    diag_string = (
        ",".join([f"'{x}'" for x in ccw_dict[condition]["icd9"]]) + 
        "," +
        ",".join([f"'{x}'" for x in ccw_dict[condition]["icd10"]])
    )

    ref_period = ccw_dict[condition]["ref_period"]

    return ref_period, diag_string

def get_ccw_proxy_for_row(row):
    if row['claims_criteria'] == 0 and row['ffs_coverage'] == 0:
        return 0
    elif row['claims_criteria'] == 1 and row['ffs_coverage'] == 0:
        return 1
    elif row['claims_criteria'] == 0 and row['ffs_coverage'] == 1:
        return 2
    else:
        return 3

def get_years_in_ref_period(ref_year, ref_period, first_year):
    years_in_ref_period = [ref_year - i for i in range(ref_period)]
    years_in_ref_period = [year for year in years_in_ref_period if year >= first_year]
    return years_in_ref_period
    

def get_years_before_ref_year(ref_year, first_year):
    return [year for year in range(first_year, ref_year + 1)]
    
def prepare_data(dw_bene_prefix, dw_adm_prefix, diag_string, ref_year, ref_period, first_year, conn):
    
    print("## Preparing cc ----")
    diag_files = [f"{dw_adm_prefix}_{year}.parquet" for year in get_years_in_ref_period(ref_year, ref_period, first_year)]
    diag_queries = []

    for file in diag_files:
        q = f"""
            SELECT DISTINCT
                bene_id,
                admission_date, 
                diag 
            FROM '{file}', UNNEST(diagnoses) AS adm(diag)
            WHERE adm.diag IN ({diag_string})
        """
        diag_queries.append(q)

    diag_query = " UNION ALL ".join(diag_queries)

    cc_query = f"""
        WITH diag AS ({diag_query}) 
        SELECT 
            bene_id, 
            1 as claims_criteria
        FROM diag
        GROUP BY bene_id
        """

    cc = conn.execute(cc_query).fetchdf()
    print(cc.shape)

    print("## Preparing adm ----")
    diag_files = [f"{dw_adm_prefix}_{year}.parquet" for year in get_years_before_ref_year(2005, 2000)]
    adm = []
    for file in diag_files:
        diag = conn.execute(f"""
            SELECT DISTINCT
                bene_id,
                admission_date, 
                diag 
            FROM '{file}', UNNEST(diagnoses) AS adm(diag)
            WHERE adm.diag IN ({diag_string})
        """).fetchdf()
        adm.append(diag)
    
    adm = pd.concat(adm)
    adm = adm[['bene_id', 'admission_date']].groupby('bene_id').min().reset_index()
    adm.rename({'admission_date': 'min_adm_date'}, axis=1, inplace=True)
    print(adm.shape)

    print("## Preparing ffs ----")

    hmo_files = [f"{dw_bene_prefix}_{year}.parquet" for year in get_years_in_ref_period(ref_year, ref_period, first_year)]
    hmo_queries = []
    for file in hmo_files:
        q = f"""
            SELECT
                bene_id,
                year, 
                SUM(hmo_mo) as hmo_y 
            FROM '{file}'
            GROUP BY 
                bene_id, year
        """
        hmo_queries.append(q)
    hmo_query = " UNION ALL ".join(hmo_queries)

    ffs_query = f"""
        WITH hmo AS ({hmo_query}) 
        SELECT 
            bene_id,
            CASE WHEN SUM(hmo_y) = 0 THEN 1 ELSE 0 END AS ffs_coverage
        FROM hmo
        GROUP BY bene_id
        """

    ffs = conn.execute(ffs_query).fetchdf()
    print(ffs.shape)

    print("## Preparing bene ----")

    bene = conn.execute(f"""
         SELECT 
              bene_id, 
              year as rfrnc_yr
         FROM '{dw_bene_prefix}_{ref_year}.parquet'
    """).fetchdf()
    print(bene.shape)

    print("## Identify beneficiaries who satisfy the claims condition ----")
    bene = bene.merge(cc, on='bene_id', how='left')
    bene['claims_criteria'] = bene['claims_criteria'].fillna(0)

    print("## Identify beneficiaries who satisfy the FFS condition ----")
    bene = bene.merge(ffs, on='bene_id', how='left')
    ## how to handle missing values? leave as NA for now

    print("## Get CCW proxy ----")
    bene['condition'] = bene.apply(get_ccw_proxy_for_row, axis=1)
    
    print("## Merge beneficiaries first admission date ----")
    bene = bene.merge(adm[['bene_id', 'min_adm_date']], on='bene_id', how='left')
    bene = bene.drop(['ffs_coverage', 'claims_criteria'], axis=1)

    return(bene)


def main(args):
    print(f"## Reading {args.condition} from {args.ccw_json} ----")
    ref_period, diag_string = read_ccw_json(args.ccw_json, args.condition)
    print(f"## Reference period: {ref_period} ----")
    print(f"## Diagnoses: {diag_string} ----")
    
    conn = duckdb.connect()
    
    print("## Reading data ----")
    bene = prepare_data(args.dw_bene_prefix, args.dw_adm_prefix, diag_string, args.year, ref_period, args.first_year, conn)
    bene.rename(columns={'condition': args.condition, 'min_adm_date': f"{args.condition}_ever"})
    
    print("## Writing data ----")
    if args.output_format == "parquet":
        bene.to_parquet(f"{args.output_prefix}_{args.condition}_{args.year}.parquet")
    elif args.output_format == "feather":
        bene.to_feather(f"{args.output_prefix}_{args.condition}_{args.year}.feather")
    elif args.output_format == "csv":
        bene.to_csv(f"{args.output_prefix}_{args.condition}_{args.year}.csv", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", 
                        default = 2004, 
                        type=int
                       )
    parser.add_argument("--condition", 
                        default = "alzh",
                       )
    parser.add_argument("--ccw_json", 
                        default = "../data/input/remote_data/ccw.json"
                       )
    parser.add_argument("--dw_bene_prefix", 
                        default = "../data/input/local_data/data_warehouse/dw_bene_xu_sabath_00_16/bene"
                       )
    parser.add_argument("--dw_adm_prefix", 
                        default = "../data/input/local_data/data_warehouse/dw_adm_xu_sabath_00_16/adm"
                       )
    parser.add_argument("--output_format", 
                        default = "parquet", 
                        choices=["parquet", "feather", "csv"]
                       )           
    parser.add_argument("--output_prefix", 
                    default = "../data/intermediate/scratch/ccw_proxy"
                   )   
    parser.add_argument("--first_year", 
                        default = 2000
                       )     
    args = parser.parse_args()
    
    main(args)
