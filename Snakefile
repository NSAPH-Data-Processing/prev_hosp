import json
import itertools

with open("data/input/remote_data/ccw.json", "r") as f:
    conditions = json.load(f)
conditions = list(conditions.keys())
years = list(range(2000, 2017))

rule all:
    input:
        expand("data/output/prev_hosp/prev_hosp_{year}.parquet", year=years) 

rule generate:
    output:
        "data/intermediate/prev_hosp/prev_hosp_{condition}_{year}.parquet"
    log:
        out=".logs_generate/{condition}_{year}.out", 
        err=".logs_generate/{condition}_{year}.err"
    shell:
        """
        python generate_features.py --year {wildcards.year} --condition {wildcards.condition} 1> {log.out} 2> {log.err}
        """

rule merge:
    input:
        expand("data/intermediate/prev_hosp/prev_hosp_{condition}_{year}.parquet", condition=conditions, year=years)
    output:
        "data/output/prev_hosp/prev_hosp_{year}.parquet"
    log:
        out=".logs_merge/{year}.out", 
        err=".logs_merge/{year}.err"
    shell:
        """
        python merge_features.py --year {wildcards.year} 1> {log.out} 2> {log.err}
        """

# import json
# import itertools

# with open("data/input/remote_data/ccw.json", "r") as f:
#     conditions = json.load(f)
# conditions = list(conditions.keys())
# years = list(range(2000, 2001))

# # Output files
# output_files = expand("data/intermediate/prev_hosp/prev_hosp_{condition}_{year}.parquet", condition=conditions, year=years)

# rule all:
#     input:
#         output_files 

# rule generate:
#     output:
#         "data/intermediate/prev_hosp/prev_hosp_{wildcards.condition}_{wildcards.year}.parquet"
#     log:
#         out=".logs/{wildcards.condtion}_{wildcards.year}.out", 
#         err=".logs/{wildcards.condition}_{wildcards.year}.err"
#     shell:
#         """
#         python generate_features.py --year {wildcards.year} --condition {wildcards.condition} 1> {log.out} 2> {log.err}
#         """

