package benchmark.tpch.run.schemaProvider

import com.utilities.SparkBuilder
import org.apache.spark.sql.DataFrame


class VanillaTpchSchemaProvider(inputDir: String) extends TpchSchemaProvider {

  override val dfMap: Map[String, DataFrame] = Map(
    "customer" -> SparkBuilder.spark.read.parquet(inputDir + "/customer"),

    "lineitem" -> SparkBuilder.spark.read.parquet(inputDir + "/lineitem"),

    "nation" -> SparkBuilder.spark.read.parquet(inputDir + "/nation"),

    "region" -> SparkBuilder.spark.read.parquet(inputDir + "/region"),

    "order" -> SparkBuilder.spark.read.parquet(inputDir + "/order"),

    "part" -> SparkBuilder.spark.read.parquet(inputDir + "/part"),

    "partsupp" -> SparkBuilder.spark.read.parquet(inputDir + "/partsupp"),

    "supplier" -> SparkBuilder.spark.read.parquet(inputDir + "/supplier")
  )
}
