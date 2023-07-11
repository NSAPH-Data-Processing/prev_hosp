# prev_hosp

Code to generate previous hospitalizations of beneficiaries is CMS/Resdac files. This task depends on previously harmonized input obtained from CMS/Resdac files: the Master Beneficiary Summary Files (MBSF) and the inpatient hospitalizations data (MEDPAR).

Using a partial approach to the [Chronic Conditions algorithm](https://www2.ccwdata.org/documents/10280/19139421/ccw-chronic-condition-algorithms.pdf) we generate 27 beneficiary-level covariates.

Each covariate represents a beneficiary's proneness to a chronic condition according to its inpatient hospitalizations records. The algorithm looks for ICD code matches in inpatient hospitalizations in a given window of time. 

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

## Run

Clone the repository and create a conda environment.

```bash
git clone <https://github.com/<user>/repo>
cd <repo>

conda env create -f requirements.yml
conda activate prev_hosp_env
```

It is also possible to use `mamba`.

```bash
mamba env create -f requirements.yml
mamba activate prev_hosp_env
```

### Entrypoints

Add symlinks to input, intermediate and output folders inside the corresponding `/data` subfolders.

For example:

```
export HOME_DIR=$(pwd)

cd $HOME_DIR/data/input/local_data/
ln -s /n/dominici_nsaph_l3/Lab/data/data_warehouse/dw_bene_wu_sabath_00_16 .
ln -s /n/dominici_nsaph_l3/Lab/data/data_warehouse/dw_adm_wu_sabath_00_16 .

cd $HOME_DIR/data/intermediate/
mkdir /n/holyscratch01/LABS/dominici_nsaph/Lab/data_processing/prev_hosp
ln -s /n/holyscratch01/LABS/dominici_nsaph/Lab/data_processing/prev_hosp .

cd $HOME_DIR/data/output/
mkdir /n/dominici_nsaph_l3/Lab/projects/analytic/ccw_proxy
ln -s /n/dominici_nsaph_l3/Lab/projects/analytic/ccw_proxy .
```
