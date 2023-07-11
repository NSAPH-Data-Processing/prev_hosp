import pandas as pd
import json
import argparse
import duckdb

# diagnoses && ARRAY[] is not supported in duckdb

def read_ccw_json(ccw_json, condition):
    with open(ccw_json, 'r') as json_file:
        ccw_dict = json.load(json_file)
    
    diag_string = (
        ",".join([f"'{x}'" for x in ccw_dict[condition]["icd9"]]) + 
        "," +
        ",".join([f"'{x}'" for x in ccw_dict[condition]["icd10"]])
    )
    
    icd9_exclusion = ccw_dict[condition].get("icd9_exclusion", [])
    icd10_exclusion = ccw_dict[condition].get("icd10_exclusion", [])

    exclusion_list = []
    if icd9_exclusion:
        exclusion_list.extend(icd9_exclusion)
    if icd10_exclusion:
        exclusion_list.extend(icd10_exclusion)

    if icd9_exclusion and icd10_exclusion:
        exclusion_string = ",".join([f"'{x}'" for x in exclusion_list])
    elif icd10_exclusion:
        exclusion_string = f"'{exclusion_list[0]}'"
    else:
        exclusion_string = ""
        
        
    print("this is diag_string")
    print(diag_string)
    print("this is the exclusion string")
    print(exclusion_string)

    ref_period = ccw_dict[condition]["ref_period"]

    return ref_period, diag_string, exclusion_string

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

def prepare_cc(dw_adm_prefix, diag_string, ref_year, ref_period, first_year, conn, exclusion_string, claims_criteria='all'):
    diag_files = [f"{dw_adm_prefix}_{year}.parquet" for year in get_years_in_ref_period(ref_year, ref_period, first_year)]
    print("files: ", diag_files)
    diag_queries = []

    if claims_criteria == 'all':
        for file in diag_files:
            query = f"""
            SELECT DISTINCT bene_id, admission_date 
            FROM '{file}', UNNEST(diagnoses) AS adm(diag)
            WHERE adm.diag IN ({diag_string})
            """
            diag_queries.append(query)
    elif claims_criteria == 'primary':
        for file in diag_files:
            query = f"""
            SELECT DISTINCT bene_id, admission_date
            FROM '{file}'
            WHERE diagnoses[1] IN ({diag_string})
            """
            diag_queries.append(query)
    elif claims_criteria == 'first_two':
        for file in diag_files:
            query = f"""
            SELECT DISTINCT bene_id, admission_date 
            FROM '{file}'
            WHERE diagnoses[1] IN ({diag_string}) OR diagnoses[2] IN ({diag_string})
            """
            diag_queries.append(query)

    diag_query = " UNION ALL ".join(diag_queries)
    print("diag_query:", diag_query)

    if exclusion_string: 
        print("yayy masuk ketemu exclusion nyaa")
        print(exclusion_string)
    else: 
        print("there is no exclusion_string")
    
    if exclusion_string:
        cc_query = f"""
            WITH diag AS ({diag_query}) 
            SELECT 
                bene_id, 
                1 as claims_criteria
            FROM diag
            WHERE bene_id NOT IN (
                SELECT DISTINCT bene_id 
                FROM '{file}', UNNEST(diagnoses) AS adm(diag)
                WHERE adm.diag IN ({exclusion_string})           
            )
            GROUP BY bene_id
            """
    else:
        cc_query = f"""
            WITH diag AS ({diag_query}) 
            SELECT 
                bene_id, 
                1 as claims_criteria
            FROM diag
            GROUP BY bene_id
            """
    print("cc_query:")
    print(cc_query)

    cc_df = conn.execute(cc_query).fetchdf()
    print(cc_df.shape)
    print(cc_df.head())
    return cc_df

def prepare_adm(dw_adm_prefix, diag_string, ref_year, first_year, conn):
    print("## Preparing adm ----")
    diag_files = [f"{dw_adm_prefix}_{year}.parquet" for year in get_years_before_ref_year(ref_year, first_year)]
    print("files: ", diag_files)

    #### ####
    #start print query
    for file in diag_files:
        diag_query = f"""
            SELECT DISTINCT
                bene_id,
                admission_date, 
                diag 
            FROM '{file}', UNNEST(diagnoses) AS adm(diag)
            WHERE adm.diag IN ({diag_string})
        """
        print(diag_query)
    #end print query
    #### ####

    adm_df = pd.concat([
        conn.execute(f"""
            SELECT DISTINCT bene_id, admission_date, diag 
            FROM '{file}', UNNEST(diagnoses) AS adm(diag)
            WHERE adm.diag IN ({diag_string})
            """
        ).fetchdf() for file in diag_files
    ])
    adm_df = adm_df[['bene_id', 'admission_date']].groupby('bene_id').min().reset_index()
    adm_df.rename(columns={'admission_date': 'min_adm_date'}, inplace=True)
    print(adm_df.shape)
    return(adm_df)

def prepare_ffs(dw_bene_prefix, ref_year, ref_period, first_year, conn):
    print("## Preparing ffs ----")
    hmo_files = [f"{dw_bene_prefix}_{year}.parquet" for year in get_years_in_ref_period(ref_year, ref_period, first_year)]
    print("files: ", hmo_files)

    hmo_queries = [
        f"""
        SELECT bene_id, year, SUM(hmo_mo) as hmo_y 
        FROM '{file}'
        GROUP BY bene_id, year
        """
        for file in hmo_files
    ]
    hmo_query = ' UNION ALL '.join(hmo_queries)
    ffs_query = f"""
        WITH hmo AS ({hmo_query}) 
        SELECT bene_id, CASE WHEN SUM(hmo_y) = 0 THEN 1 ELSE 0 END AS ffs_coverage
        FROM hmo
        GROUP BY bene_id
        """
    #print query
    print(ffs_query)
    
    ffs_df = conn.execute(ffs_query).fetchdf()
    print(ffs_df.shape)
    return ffs_df

def prepare_data(dw_bene_prefix, dw_adm_prefix, diag_string, ref_year, ref_period, first_year, conn,exclusion_string, claims_criteria='all'):
    cc_df = prepare_cc(dw_adm_prefix, diag_string, ref_year, ref_period, first_year, conn, exclusion_string, claims_criteria)
    adm_df = prepare_adm(dw_adm_prefix, diag_string, ref_year, first_year, conn)
    ffs_df = prepare_ffs(dw_bene_prefix, ref_year, ref_period, first_year, conn)

    print("## Preparing bene ----")
    bene_query = f"""
         SELECT bene_id, year as rfrnc_yr
         FROM '{dw_bene_prefix}_{ref_year}.parquet'
    """
    #print query
    print(bene_query)
    df = conn.execute(bene_query).fetchdf()
    print(df.shape)
    
    print("## Identify beneficiaries who satisfy the claims condition ----")
    df = df.merge(cc_df, on='bene_id', how='left')
    df['claims_criteria'] = df['claims_criteria'].fillna(0)

    print("## Identify beneficiaries who satisfy the FFS condition ----")
    df = df.merge(ffs_df, on='bene_id', how='left')
    ## how to handle missing values? leave as NA for now

    print("## Get CCW proxy ----")
    df['condition'] = df.apply(get_ccw_proxy_for_row, axis=1)

    print("## Merge beneficiaries first admission date ----")
    df = df.merge(adm_df[['bene_id', 'min_adm_date']], on='bene_id', how='left')
    df = df.drop(['ffs_coverage', 'claims_criteria'], axis=1)
    
    return df

def main(args):
    print(f"## Reading {args.condition} from {args.ccw_json} ----")
    ref_period, diag_string, exclusion_string = read_ccw_json(args.ccw_json, args.condition)
    print(f"## Reference period: {ref_period} ----")
    print(f"## Diagnoses: {diag_string} ----")
    print(f"## Exclusion: {exclusion_string} ----")
    
    conn = duckdb.connect()
    
    print("## Preparing data ----")
    df = prepare_data(
        args.dw_bene_prefix, 
        args.dw_adm_prefix, 
        diag_string, 
        args.year, 
        ref_period, 
        args.first_year, 
        conn, 
        exclusion_string,
        args.claims_criteria
        )
    df = df.rename(columns={'condition': args.condition, 'min_adm_date': f"{args.condition}_ever"})
    
    print("## Writing data ----")
    df = df.set_index(['bene_id'])

    output_file = f"{args.output_prefix}_{args.condition}_{args.year}.{args.output_format}"
    if args.output_format == "parquet":
        df.to_parquet(output_file)
    elif args.output_format == "feather":
        df.to_feather(output_file)
    elif args.output_format == "csv":
        df.to_csv(output_file)

    print(f"## Output file written to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", 
                        default = 2000, 
                        type=int
                       )
    parser.add_argument("--condition", 
                        default = "stroke",
                       )
    parser.add_argument("--ccw_json", 
                        default = "./data/input/remote_data/ccw.json"
                       )
    parser.add_argument("--dw_bene_prefix", 
                        default = "./data/input/local_data/dw_bene_wu_sabath_00_16/bene"
                       )
    parser.add_argument("--dw_adm_prefix", 
                        default = "./data/input/local_data/dw_adm_wu_sabath_00_16/adm"
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
    parser.add_argument("--claims_criteria", 
                        default = "all", 
                        choices=["all", "primary", "first_two"]
                       ) 
    args = parser.parse_args()
    
    main(args)
