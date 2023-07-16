import json
import itertools

with open("data/input/remote_data/ccw.json", "r") as f:
    conditions = json.load(f)
conditions = list(conditions.keys())
years = list(range(2000, 2016))

rule all:
    input:
        expand("data/intermediate/prev_hosp/prev_hosp_{condition}_{year}.parquet", condition=conditions, year=years) 

rule generate:
    output:
        "data/intermediate/prev_hosp/prev_hosp_{condition}_{year}.parquet"
    log:
        out=".logs/{condition}_{year}.out", 
        err=".logs/{condition}_{year}.err"
    shell:
        """
        python generate_features.py --year {wildcards.year} --condition {wildcards.condition} 1> {log.out} 2> {log.err}
        """

# rule merge:
#     input:
#         output_files  # This rule requires all the output files from the condition rules
#     shell:
#         """
#         python merge_features.py
#         """

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

