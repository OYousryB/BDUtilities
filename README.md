## Clone repo 
git clone git@github.com:OYousryB/BDUtilities.git

## Install 
sbt clean package assembly

## Update script env variables

## Clone TPC-H / TPC-DS repos
Make sure to build the project after cloning
https://github.com/gregrahn/tpch-kit
https://github.com/gregrahn/tpcds-kit

## Download Spark with version 2.3 or higher
Configure spark env if needed 
Start up spark master and worker

### cp scripts/env.txt.template scripts/env.txt

### cp scripts/params.txt.template scripts/params.txt
### cp scripts/spark-conf.txt.template scripts/spark-conf.txt

### Update the params 

### Execute scripts

example: ./scripts/tpch/generate-all.sh
