package benchmark.tpcds.run.schemaProvider

import com.utilities.SparkBuilder


class VanillaTpcdsSchemaProvider(inputDir: String) extends TpcdsSchemaProvider {

  override val dfMap = Map(
    "catalog_sales" -> SparkBuilder.spark.read.parquet(inputDir + "/catalog_sales"),

    "catalog_returns" -> SparkBuilder.spark.read.parquet(inputDir + "/catalog_returns"),

    "inventory" -> SparkBuilder.spark.read.parquet(inputDir + "/inventory"),

    "store_sales" -> SparkBuilder.spark.read.parquet(inputDir + "/store_sales"),

    "store_returns" -> SparkBuilder.spark.read.parquet(inputDir + "/store_returns"),

    "web_sales" -> SparkBuilder.spark.read.parquet(inputDir + "/web_sales"),

    "web_returns" -> SparkBuilder.spark.read.parquet(inputDir + "/web_returns"),

    "catalog_page" -> SparkBuilder.spark.read.parquet(inputDir + "/catalog_page"),

    "customer" -> SparkBuilder.spark.read.parquet(inputDir + "/customer"),

    "customer_address" -> SparkBuilder.spark.read.parquet(inputDir + "/customer_address"),

    "customer_demographics" -> SparkBuilder.spark.read.parquet(inputDir + "/customer_demographics"),

    "date_dim" -> SparkBuilder.spark.read.parquet(inputDir + "/date_dim"),

    "household_demographics" -> SparkBuilder.spark.read.parquet(inputDir + "/household_demographics"),

    "income_band" -> SparkBuilder.spark.read.parquet(inputDir + "/income_band"),

    "item" -> SparkBuilder.spark.read.parquet(inputDir + "/item"),

    "promotion" -> SparkBuilder.spark.read.parquet(inputDir + "/promotion"),

    "reason" -> SparkBuilder.spark.read.parquet(inputDir + "/reason"),

    "ship_mode" -> SparkBuilder.spark.read.parquet(inputDir + "/ship_mode"),

    "store" -> SparkBuilder.spark.read.parquet(inputDir + "/store"),

    "time_dim" -> SparkBuilder.spark.read.parquet(inputDir + "/time_dim"),

    "warehouse" -> SparkBuilder.spark.read.parquet(inputDir + "/warehouse"),

    "web_page" -> SparkBuilder.spark.read.parquet(inputDir + "/web_page"),

    "web_site" -> SparkBuilder.spark.read.parquet(inputDir + "/web_site"),

    "call_center" -> SparkBuilder.spark.read.parquet(inputDir + "/call_center")
  )

  dfMap.foreach(x => x._2.createOrReplaceTempView(x._1))
}
