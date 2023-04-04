# ccw proxy

Producing proxy chronic conditions from MBSF and MEDPAR.

```
export HOME_DIR=$(pwd)

cd $HOME_DIR/data/input/local_data/
ln -s /n/dominici_nsaph_l3/Lab/data/data_warehouse/ .

cd $HOME_DIR/data/intermediate.
ln -s /n/dominici_nsaph_l3/Lab/data_processing/ccw_proxy/scratch .

cd $HOME_DIR/data/output/
mkdir /n/dominici_nsaph_l3/Lab/projects/analytic/ccw_proxy
ln -s /n/dominici_nsaph_l3/Lab/projects/analytic/ccw_proxy .

cd $HOME_DIR/src/
job01=$(sbatch ALZH.sbatch)

```
