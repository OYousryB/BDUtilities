package com.utilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

final public class Conf {

    public static final long GLOBAL_SERIALIZATION_ID = 858253132543L;

    private static final String CONF_FILE = "hermes.conf";

    /**
     * Number of threads used by the engine for each partition to compute the view. Default = 1.
     */
    public static final int VIEW_COMPUTE_THREADS;
    private static final String VIEW_COMPUTE_THREADS_DEFAULT = "1";
    private static final String VIEW_COMPUTE_THREADS_PROPERTY_NAME = "engine.view-compute-threads";

    /**
     * Number of threads used by the engine for each partition to materialize the columns within the view. Default = 1.
     */
    public static final int COLUMN_MATERIALIZATION_THREADS;
    private static final String COLUMN_MATERIALIZATION_THREADS_DEFAULT = "1";
    private static final String COLUMN_MATERIALIZATION_THREADS_PROPERTY_NAME = "engine.column-materialization-threads";

    /**
     * Minimum number of rows in a partition. Default = 64.
     */
    public static final long MIN_PARTITION_ROWS;
    private static final String MIN_PARTITION_ROWS_DEFAULT = "64";
    private static final String MIN_PARTITION_ROWS_PROPERTY_NAME = "engine.min-partition-rows";

    /**
     * Maximum number of rows in a partition. Default = 1000000.
     */
    public static final long MAX_PARTITION_ROWS;
    private static final String MAX_PARTITION_ROWS_DEFAULT = "1000000";
    private static final String MAX_PARTITION_ROWS_PROPERTY_NAME = "engine.max-partition-rows";

    /**
     * Maximum estimated size of a partition in MiB when default partitioning is used. Default = 64.
     */
    public static final long MAX_PARTITION_SIZE;
    private static final String MAX_PARTITION_SIZE_DEFAULT = "64";
    private static final String MAX_PARTITION_SIZE_PROPERTY_NAME = "engine.max-partition-size";

    /**
     * Format of the stored tables
     */
    public static final String TABLE_FORMAT;
    private static final String TABLE_FORMAT_DEFAULT = "parquet";
    private static final String TABLE_FORMAT_PROPERTY_NAME = "engine.table-format";

    /**
     * Indicates whether to contact the memory column server for column materialization or not
     */
    public static final boolean USE_MEMORY_COLUMNS;
    private static final String USE_MEMORY_COLUMNS_DEFAULT = "true";
    private static final String USE_MEMORY_COLUMNS_PROPERTY_NAME = "engine.use-memory-columns";

    /**
     * Maximum size of each engine page in bytes. Smaller pages result in larger indexing overhead, but it may benefit
     * in load times when the fetched records are highly random and rarely access the same page more than once. Default
     * = 1048576 (1 MiB)
     */
    public static final int PAGE_SIZE;
    private static final String PAGE_SIZE_DEFAULT = "1048576";
    private static final String PAGE_SIZE_PROPERTY_NAME = "format.corta.page-size";

    /**
     * Maximum size of a column part file in pages. This value should match the storage block size. Default = 128.
     */
    public static final int PART_FILE_LENGTH;
    private static final String PART_FILE_LENGTH_DEFAULT = "128";
    private static final String PART_FILE_LENGTH_PROPERTY_NAME = "format.corta.part-file-length";

    /**
     * Number of threads used by the optimizer to calculate join files. This value controls the amount of system
     * resources to be used when a join index is being calculated. Default = 4.
     */
    public static final int JOIN_CALCULATE_THREADS;
    private static final String JOIN_CALCULATE_THREADS_DEFAULT = "4";
    private static final String JOIN_CALCULATE_THREADS_PROPERTY_NAME = "optimizer.join-calculate-threads";

    /**
     * Maximum number of in-memory partitions when converting a table from one format to another. Actual number of
     * in-memory partitions may exceed this value by 1. This value should typically be more than 2. In cases where
     * machine memory is abundant and IO is extremely fast, it is recommended to set this value to match the machine's
     * number of logical CPUs. Default = 4.
     */
    public static final int DATA_CONVERT_MAX_IN_MEMORY_PARTITIONS;
    private static final String DATA_CONVERT_MAX_IN_MEMORY_PARTITIONS_DEFAULT = "4";
    private static final String DATA_CONVERT_MAX_IN_MEMORY_PARTITIONS_PROPERTY_NAME = "optimizer.data-convert-max-in-memory-partitions";

    /**
     * Number of IO threads used by the optimizer to convert a table from one format to another. Preferably, this value
     * should match maximum number of in-memory partitions. Default = 4.
     */
    public static final int DATA_CONVERT_IO_THREADS;
    private static final String DATA_CONVERT_IO_THREADS_DEFAULT = "4";
    private static final String DATA_CONVERT_IO_THREADS_PROPERTY_NAME = "optimizer.data-convert-io-threads";

    /**
     * Connect URL to MySQL database to store the benchmark results
     */
    public static final String BENCHMARK_MYSQL_URL;
    private static final String BENCHMARK_MYSQL_URL_DEFAULT = "jdbc:mysql://localhost?user=root&password=root";
    private static final String BENCHMARK_MYSQL_URL_PROPERTY_NAME = "benchmark.mysql-server-url";

    static {
        Properties properties = new Properties();

        File confFile = new File(CONF_FILE);
        try {
            properties.load(new FileInputStream(confFile));
        } catch (IOException e) {
            Logger.getLogger("hermes-config").warning("Configuration file '" + confFile.getAbsolutePath() + "' not found! Using defaults");
        }

        VIEW_COMPUTE_THREADS = Integer.parseInt(properties.getProperty(VIEW_COMPUTE_THREADS_PROPERTY_NAME, VIEW_COMPUTE_THREADS_DEFAULT));
        Logger.getLogger("hermes-config").info(VIEW_COMPUTE_THREADS_PROPERTY_NAME + " = " + VIEW_COMPUTE_THREADS);

        COLUMN_MATERIALIZATION_THREADS = Integer.parseInt(properties.getProperty(COLUMN_MATERIALIZATION_THREADS_PROPERTY_NAME, COLUMN_MATERIALIZATION_THREADS_DEFAULT));
        Logger.getLogger("hermes-config").info(COLUMN_MATERIALIZATION_THREADS_PROPERTY_NAME + " = " + COLUMN_MATERIALIZATION_THREADS);

        MIN_PARTITION_ROWS = Long.parseLong(properties.getProperty(MIN_PARTITION_ROWS_PROPERTY_NAME, MIN_PARTITION_ROWS_DEFAULT));
        Logger.getLogger("hermes-config").info(MAX_PARTITION_ROWS_PROPERTY_NAME + " = " + MIN_PARTITION_ROWS);

        MAX_PARTITION_ROWS = Long.parseLong(properties.getProperty(MAX_PARTITION_ROWS_PROPERTY_NAME, MAX_PARTITION_ROWS_DEFAULT));
        Logger.getLogger("hermes-config").info(MAX_PARTITION_ROWS_PROPERTY_NAME + " = " + MAX_PARTITION_ROWS);

        MAX_PARTITION_SIZE = Long.parseLong(properties.getProperty(MAX_PARTITION_SIZE_PROPERTY_NAME, MAX_PARTITION_SIZE_DEFAULT)) * 1048576L;
        Logger.getLogger("hermes-config").info(MAX_PARTITION_SIZE_PROPERTY_NAME + " = " + MAX_PARTITION_SIZE);

        TABLE_FORMAT = properties.getProperty(TABLE_FORMAT_PROPERTY_NAME, TABLE_FORMAT_DEFAULT);
        Logger.getLogger("hermes-config").info(TABLE_FORMAT_PROPERTY_NAME + " = " + TABLE_FORMAT);

        USE_MEMORY_COLUMNS = Boolean.parseBoolean(properties.getProperty(USE_MEMORY_COLUMNS_PROPERTY_NAME, USE_MEMORY_COLUMNS_DEFAULT));
        Logger.getLogger("hermes-config").info(USE_MEMORY_COLUMNS_PROPERTY_NAME + " = " + USE_MEMORY_COLUMNS);

        PAGE_SIZE = Integer.parseInt(properties.getProperty(PAGE_SIZE_PROPERTY_NAME, PAGE_SIZE_DEFAULT));
        Logger.getLogger("hermes-config").info(PAGE_SIZE_PROPERTY_NAME + " = " + PAGE_SIZE);

        PART_FILE_LENGTH = Integer.parseInt(properties.getProperty(PART_FILE_LENGTH_PROPERTY_NAME, PART_FILE_LENGTH_DEFAULT));
        Logger.getLogger("hermes-config").info(PART_FILE_LENGTH_PROPERTY_NAME + " = " + PART_FILE_LENGTH);

        JOIN_CALCULATE_THREADS = Integer.parseInt(properties.getProperty(JOIN_CALCULATE_THREADS_PROPERTY_NAME, JOIN_CALCULATE_THREADS_DEFAULT));
        Logger.getLogger("hermes-config").info(JOIN_CALCULATE_THREADS_PROPERTY_NAME + " = " + JOIN_CALCULATE_THREADS);

        DATA_CONVERT_MAX_IN_MEMORY_PARTITIONS = Integer.parseInt(properties.getProperty(DATA_CONVERT_MAX_IN_MEMORY_PARTITIONS_PROPERTY_NAME, DATA_CONVERT_MAX_IN_MEMORY_PARTITIONS_DEFAULT));
        Logger.getLogger("hermes-config").info(DATA_CONVERT_MAX_IN_MEMORY_PARTITIONS_PROPERTY_NAME + " = " + DATA_CONVERT_MAX_IN_MEMORY_PARTITIONS);

        DATA_CONVERT_IO_THREADS = Integer.parseInt(properties.getProperty(DATA_CONVERT_IO_THREADS_PROPERTY_NAME, DATA_CONVERT_IO_THREADS_DEFAULT));
        Logger.getLogger("hermes-config").info(DATA_CONVERT_IO_THREADS_PROPERTY_NAME + " = " + DATA_CONVERT_IO_THREADS);

        BENCHMARK_MYSQL_URL = properties.getProperty(BENCHMARK_MYSQL_URL_PROPERTY_NAME, BENCHMARK_MYSQL_URL_DEFAULT);
        Logger.getLogger("hermes-config").info(BENCHMARK_MYSQL_URL_PROPERTY_NAME + " = " + BENCHMARK_MYSQL_URL);
    }
}
