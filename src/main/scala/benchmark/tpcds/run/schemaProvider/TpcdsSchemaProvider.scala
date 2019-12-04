package benchmark.tpcds.run.schemaProvider

import benchmark.SchemaProvider
import org.apache.spark.sql.DataFrame

abstract class TpcdsSchemaProvider extends SchemaProvider{

  val dfMap: Map[String, DataFrame] = Map[String, DataFrame]()

  // for implicits
  def catalog_sales: DataFrame = dfMap("catalog_sales")
  def catalog_returns: DataFrame = dfMap("catalog_returns")
  def inventory: DataFrame = dfMap("inventory")
  def store_sales: DataFrame = dfMap("store_sales")
  def store_returns: DataFrame = dfMap("store_returns")
  def web_sales: DataFrame= dfMap("web_sales")
  def web_returns: DataFrame = dfMap("web_returns")
  def catalog_page: DataFrame = dfMap("catalog_page")
  def customer: DataFrame = dfMap("customer")
  def customer_address: DataFrame = dfMap("customer_address")
  def customer_demographics: DataFrame = dfMap("customer_demographics")
  def date_dim: DataFrame = dfMap("date_dim")
  def household_demographics: DataFrame = dfMap("household_demographics")
  def income_band: DataFrame = dfMap("income_band")
  def item: DataFrame = dfMap("item")
  def promotion: DataFrame = dfMap("promotion")
  def reason: DataFrame = dfMap("reason")
  def ship_mode: DataFrame = dfMap("ship_mode")
  def store: DataFrame = dfMap("store")
  def time_dim: DataFrame = dfMap("time_dim")
  def warehouse: DataFrame = dfMap("warehouse")
  def web_page: DataFrame = dfMap("web_page")
  def web_site: DataFrame = dfMap("web_site")
  def call_center: DataFrame = dfMap("call_center")
  def dbgen_version: DataFrame= dfMap("dbgen_version")
}
