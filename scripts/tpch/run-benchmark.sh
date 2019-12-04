#!/usr/bin/env bash

source scripts/env.txt
source scripts/params.txt
source scripts/spark-conf.txt

for SF in $SCALES
do
    mkdir -p $DB_UTILITIES/logs

    if [ $ENABLE_PROFILING -eq 1 ]
    then
        mkdir -p $DB_UTILITIES/jfr/tpch/$SF/driver
        mkdir -p $DB_UTILITIES/jfr/tpch/$SF/executor
    fi

    mkdir -p $DB_UTILITIES/logs/tpch/$SF

    cleanup()
    {
        if [[ $MASTER != local* ]]
        then
            echo "Shutting down Spark"
            $SPARK_HOME/sbin/stop-all.sh >> $DB_UTILITIES/logs/tpch/$SF.out
            echo "Starting up Spark"
            $SPARK_HOME/sbin/start-all.sh >> $DB_UTILITIES/logs/tpch/$SF.out
            # sudo su
            # echo 1 > /proc/sys/vm/drop_caches
            # exit
        fi
    }

    cleanup
    echo "Benchmarking TPCH SCLAE $SF on Spark using 'benchmark.tpch.run.QueryTest $BENCHMARK_ARGS'"
    $SPARK_HOME/bin/spark-submit --class benchmark.tpch.run.$BENCHMARK \
        --master $MASTER \
        --conf spark.executor.instances=$EXECUTOR_INSTANCES \
        --conf spark.executor.cores=$CORES_PER_EXECUTOR \
        --conf spark.driver.memory=$DRIVER_MEMORY \
        --conf spark.executor.memory=$MEMORY_PER_EXECUTOR \
        --conf spark.sql.shuffle.partitions=$SPARK_SHUFFLE_PARTITIONS \
        --conf spark.local.dir=$SPARK_LOCAL_DIR \
        $JAR_PATH \
        $BENCHMARK_ARGS > $DB_UTILITIES/logs/tpch/$SF/s$SF-c$TOTAL_EXECUTOR_CORES-spark.txt
done

echo "Spark Result"
grep TPCH_ $DB_UTILITIES/logs/tpch/$SF/s$SF-c$TOTAL_EXECUTOR_CORES-spark.txt