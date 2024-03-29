# prev_hosp

Code to generate previous hospitalizations of beneficiaries is CMS/Resdac files. This task depends on previously harmonized input obtained from CMS/Resdac files: the Master Beneficiary Summary Files (MBSF) and the inpatient hospitalizations data (MEDPAR).

Using a partial approach to the [Chronic Conditions algorithm](https://www2.ccwdata.org/documents/10280/19139421/ccw-chronic-condition-algorithms.pdf) we generate 27 beneficiary-level covariates. 

Each covariate represents a beneficiary's proneness to a chronic condition according to its inpatient hospitalizations records. The algorithm looks for ICD code matches in inpatient hospitalizations in a given window of time. Specifically, we used the ICD codes and time windows as described in the pdf's in [resources](resources).

We generate previous hospitalizations for:
1. Acute Myocardial Infarction (1 year)
2. Alzheimer's Disease  (3 years)
3. Alzheimer's Disease and Related Disorders or Senile Dementia (3 years)
4. Atrial Fibrillation (1 year)
5. Cataract (1 year)
6. Chronic Kidney Disease (2 years)
7. Chronic Obstructive Pulmonary Disease (1 year)
8. Heart Failure (2 years)
9. Diabetes (2 years)
10. Glaucoma (1 year)
11. Hip/Pelvic Fracture (1 year)
12. Ischemic Heart Disease (2 years)
13. Depression (1 year)
14. Osteoporosis (1 year)
15. Rheumatoid Arthritis / Osteoarthritis (2 years)
16. Stroke / Transient Ischemic Attack (1 year)
17. Breast Cancer (1 year)
18. Colorectal Cancer (1 year)
19. Prostate Cancer (1 year)
20. Lung Cancer (1 year)
21. Endometrial Cancer (1 year)
22. Anemia (1 year)
23. Asthma (1 year)
24. Hyperlipidemia (1 year)
25. Benign Prostatic Hyperplasia (1 year)
26. Hypertension (1 year)
27. Acquired Hypothyroidism (1 year)

The accuracy of the ccw algorithm has been studied, for example, with the NHANES population in this paper https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3207195/

## Data Dictionary

Use the codebook in [resources/codebook-mbsf-cc.pdf](resources/codebook-mbsf-cc.pdf) for reference, or

the excel sheet in [resources/record-layout-mbsf-cc.xlsx](resources/record-layout-mbsf-cc.xlsx).


Previous hospitalizations values:

|   | Code value                                                                                 |
|---|--------------------------------------------------------------------------------------------|
| 0 | Beneficiary did not meet claims criteria or have sufficient fee-for-service (FFS) coverage |
| 1 | Beneficiary met claims criteria but did not have sufficient FFS coverage                   |
| 2 | Beneficiary did not meet claims criteria but had sufficient FFS coverage                   |
| 3 | Beneficiary met claims criteria and had sufficient FFS coverage                            |

## Sample head

The sample shown is filled with fake values.

```
bene_id,hypoth,hypoth_ever,hypert,hypert_ever,hyperl,hyperl_ever,asthma,asthma_ever,ra_oa,ra_oa_ever,osteoprs,osteoprs_ever,depressn,depressn_ever,ischmcht,ischmcht_ever,hipfrac,hipfrac_ever,glaucoma,glaucoma_ever,hyperp,hyperp_ever,endometrialCancer,endometrialCancer_ever,lungCancer,lungCancer_ever,prostateCancer,prostateCancer_ever,colorectalCancer,colorectalCancer_ever,breastCancer,breastCancer_ever,stroke,stroke_ever,diabetes,diabetes_ever,chf,chf_ever,copd,copd_ever,chrnkidn,chrnkidn_ever,cataract,cataract_ever,atrialfb,atrialfb_ever,alzhdmta,alzhdmta_ever,alzh,alzh_ever,ami,ami_ever,anemia,anemia_ever,rfrnc_yr
lllllll0o4loXUX,2,,3,2000-07-27,2,,2,,3,2000-07-27,2,,2,,2,,3,2000-07-27,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2000
llllllloo77l0oO,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2003
A03881767,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,2013
lllllll04O00Slo,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2,,2008
lllllllX78lOoX0,2,,3,2003-01-07,3,2002-12-12,2,,2,,2,,3,2003-01-13,3,2003-01-13,2,,2,,2,,2,,2,,2,,2,,2,,2,2002-12-03,2,,2,,2,,2,,2,,2,,2,,2,,2,,3,2002-12-03,2003
llllllloo870lUo,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,0,,2006
```

## Run

Clone the repository and create a conda environment.

```bash
git clone <https://github.com/<user>/repo>
cd <repo>

conda env create -f requirements.yml
conda activate <env_name> #environment name as found in requirements.yml
```

It is also possible to use `mamba`.

```bash
mamba env create -f requirements.yml
mamba activate <env_name>
```

### Entrypoints

Add symlinks to input, intermediate and output folders inside the corresponding `/data` subfolders.

For example:

```bash
export HOME_DIR=$(pwd)

cd $HOME_DIR/data/input/ .
ln -s <input_path> . #paths as found in data/input/README.md

cd $HOME_DIR/data/output/
ln -s <output_path> . #paths as found in data/output/README.md
```

The README.md files inside the `/data` subfolders contain path documentation for NSAPH internal purposes.

### Pipeline

Run the `generate_counts.py` script for all years:

```bash
python ./src/generate_counts.py --year <year>
```

then run 

```bash
python ./src/merge_counts.py
```

or run the pipeline:

```bash
snakemake --cores
```

In addition, `.sbatch` templates are provided for SLURM users. Be mindful that each HPC clusters has a different configuration and the `.sbatch` files might need to be modified accordingly. 
