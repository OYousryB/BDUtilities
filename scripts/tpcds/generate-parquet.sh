#!/usr/bin/env bash

source scripts/env.txt
source scripts/params.txt
source scripts/spark-conf.txt

resetSparkCluster()
{
    $SPARK_HOME/sbin/stop-all.sh
    wait
    $SPARK_HOME/sbin/start-all.sh
    wait
}

for SF in $SCALES
do
    OUTPUT=$DATA_DB/tpcds/$SF

    mkdir -p $DB_UTILITIES/logs/gen/tpcds

    TBL_DIR=$OUTPUT/tbl
    PARQUET_DIR=$OUTPUT/parquet

    if [[ ! -d $PARQUET_DIR ]] || [[ $OVERWRITE_PARQUET_DATA -eq 1 ]]
    then
        # Convert into Parquet ...............................
        rm -rf $PARQUET_DIR
        mkdir -p $PARQUET_DIR

        resetSparkCluster

        $SPARK_HOME/bin/spark-submit \
            --class benchmark.tpcds.gen.ParquetGen \
            --master "local[*]" \
            --conf spark.driver.memory=$DRIVER_MEMORY \
            $JAR_PATH \
            $TBL_DIR $PARQUET_DIR $PARQUET_PARTITIONS > $DB_UTILITIES/logs/gen/tpcds/s$SF-parquet.txt
    fi
done

