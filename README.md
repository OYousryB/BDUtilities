## Clone repo 
### Prerequisites: Install git
git clone git@github.com:OYousryB/BDUtilities.git

## Install 
### Prerequisites: Install sbt
sbt clean package assembly

## Clone TPC-H / TPC-DS repos
### Make sure to build the project after cloning

### References
https://github.com/gregrahn/tpch-kit
https://github.com/gregrahn/tpcds-kit

## Download Spark with version 2.3 or higher
Optional: Configure spark env if needed 

## Update the params

### cp scripts/env.txt.template scripts/env.txt

### cp scripts/params.txt.template scripts/params.txt

### cp scripts/spark-conf.txt.template scripts/spark-conf.txt 

### Execute scripts

example: ./scripts/tpch/generate-all.sh
