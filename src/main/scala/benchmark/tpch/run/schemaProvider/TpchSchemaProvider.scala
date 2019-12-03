package benchmark.tpch.run.schemaProvider

import benchmark.SchemaProvider
import org.apache.spark.sql.DataFrame

abstract class TpchSchemaProvider extends SchemaProvider{

  val dfMap = Map[String, DataFrame]()

  // for implicits
  def customer: DataFrame = dfMap("customer")
  def lineitem: DataFrame = dfMap("lineitem")
  def nation: DataFrame = dfMap("nation")
  def region: DataFrame = dfMap("region")
  def orders: DataFrame = dfMap("orders")
  def part: DataFrame = dfMap("part")
  def partsupp: DataFrame = dfMap("partsupp")
  def supplier: DataFrame = dfMap("supplier")
}
