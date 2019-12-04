package benchmark.tpcds.gen

import benchmark.tpcds.schema.{CallCenter, CatalogPage, CatalogReturns, CatalogSales, Customer, CustomerAddress, CustomerDemographics, DateDim, DbgenVersion, HouseholdDemographics, IncomeBand, Inventory, Item, Promotion, Reason, ShipMode, Store, StoreReturns, StoreSales, TimeDim, Warehouse, WebPage, WebReturns, WebSales, WebSite}
import com.utilities.SparkBuilder
import org.apache.spark.sql.SaveMode

object ParquetGen {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Missing args!")
      return
    }

    val inputDir = args(0)
    val outputDir = args(1)

    val separator = "\\|"
    val limit = -1

    def double(v: String): Double = {
      try {
        v.trim.toDouble
      } catch {
        case _: Exception => 0
      }
    }

    def long(v: String): Long = {
      try {
        v.trim.toLong
      } catch {
        case _: Exception => 0
      }
    }

    def str(v: String) = v.trim

    // this is used to implicitly convert an RDD to a DataFrame.
    val sc = SparkBuilder.sc
    val sqlContext = SparkBuilder.sqlContext
    import sqlContext.implicits._

    val dfMap = Map(
      "catalog_sales" -> sc.textFile(inputDir + "/catalog_sales/*").map(_.split(separator, limit)).map(p =>
        CatalogSales(long(p(0)), long(p(1)), long(p(2)), long(p(3)), long(p(4)), long(p(5)), long(p(6)), long(p(7)), long(p(8)), long(p(9)), long(p(10)), long(p(11)),
          long(p(12)), long(p(13)), long(p(14)), long(p(15)), long(p(16)), long(p(17)), long(p(18)), double(p(19)), double(p(20)), double(p(21)), double(p(22)),
          double(p(23)), double(p(24)), double(p(25)), double(p(26)), double(p(27)), double(p(28)), double(p(29)), double(p(30)), double(p(31)), double(p(32)), double(p(33)))).toDF(),

      "catalog_returns" -> sc.textFile(inputDir + "/catalog_returns/*").map(_.split(separator, limit)).map(p =>
        CatalogReturns(long(p(0)), long(p(1)), long(p(2)), long(p(3)), long(p(4)), long(p(5)), long(p(6)), long(p(7)), long(p(8)), long(p(9)), long(p(10)), long(p(11)),
          long(p(12)), long(p(13)), long(p(14)), long(p(15)), long(p(16)), long(p(17)), double(p(18)), double(p(19)), double(p(20)), double(p(21)), double(p(22)), double(p(23)), double(p(24)), double(p(25)), double(p(26)))).toDF(),

      "inventory" -> sc.textFile(inputDir + "/inventory/*").map(_.split(separator, limit)).map(p =>
        Inventory(long(p(0)), long(p(1)), long(p(2)), long(p(3)))).toDF(),

      "store_sales" -> sc.textFile(inputDir + "/store_sales/*").map(_.split(separator, limit)).map(p =>
        StoreSales(long(p(0)), long(p(1)), long(p(2)), long(p(3)), long(p(4)), long(p(5)), long(p(6)), long(p(7)), long(p(8)), long(p(9)), long(p(10)), double(p(11)),
          double(p(12)), double(p(13)), double(p(14)), double(p(15)), double(p(16)), double(p(17)), double(p(18)), double(p(19)), double(p(20)), double(p(21)), double(p(22)))).toDF(),

      "store_returns" -> sc.textFile(inputDir + "/store_returns/*").map(_.split(separator, limit)).map(p =>
        StoreReturns(long(p(0)), long(p(1)), long(p(2)), long(p(3)), long(p(4)), long(p(5)), long(p(6)), long(p(7)), long(p(8)), long(p(9)), long(p(10)), double(p(11)),
          double(p(12)), double(p(13)), double(p(14)), double(p(15)), double(p(16)), double(p(17)), double(p(18)), double(p(19)))).toDF(),

      "web_sales" -> sc.textFile(inputDir + "/web_sales/*").map(_.split(separator, limit)).map(p =>
        WebSales(long(p(0)), long(p(1)), long(p(2)), long(p(3)), long(p(4)), long(p(5)), long(p(6)), long(p(7)), long(p(8)), long(p(9)), long(p(10)), long(p(11)),
          long(p(12)), long(p(13)), long(p(14)), long(p(15)), long(p(16)), long(p(17)),  long(p(18)), double(p(19)), double(p(20)), double(p(21)), double(p(22)),
          double(p(23)), double(p(24)), double(p(25)), double(p(26)), double(p(27)), double(p(28)), double(p(29)), double(p(30)), double(p(31)), double(p(32)), double(p(33)))).toDF(),

      "web_returns" -> sc.textFile(inputDir + "/web_returns/*").map(_.split(separator, limit)).map(p =>
        WebReturns(long(p(0)), long(p(1)), long(p(2)), long(p(3)), long(p(4)), long(p(5)), long(p(6)), long(p(7)), long(p(8)), long(p(9)), long(p(10)), long(p(11)),
          long(p(12)), long(p(13)), long(p(14)), double(p(15)), double(p(16)), double(p(17)), double(p(18)), double(p(19)), double(p(20)), double(p(21)), double(p(22)), double(p(23)))).toDF(),

      "call_center" -> sc.textFile(inputDir + "/call_center/*").map(_.split(separator, limit)).map(p =>
        CallCenter(long(p(0)), str(p(1)), str(p(2)), str(p(3)), long(p(4)), long(p(5)), str(p(6)), str(p(7)), long(p(8)), long(p(9)), str(p(10)), str(p(11)),
          long(p(12)), str(p(13)), str(p(14)), str(p(15)), long(p(16)), str(p(17)), long(p(18)), str(p(19)), str(p(20)), str(p(21)), str(p(22)), str(p(23)),
          str(p(24)), str(p(25)), str(p(26)), str(p(27)), str(p(28)), double(p(29)), double(p(30)))).toDF(),

      "catalog_page" -> sc.textFile(inputDir + "/catalog_page/*").map(_.split(separator, limit)).map(p =>
        CatalogPage(long(p(0)), str(p(1)), long(p(2)), long(p(3)), str(p(4)), long(p(5)), long(p(6)), str(p(7)), str(p(8)))).toDF(),

      "customer" -> sc.textFile(inputDir + "/customer/*").map(_.split(separator, limit)).map(p =>
        Customer(long(p(0)), str(p(1)), long(p(2)), long(p(3)), long(p(4)), long(p(5)), long(p(6)), str(p(7)), str(p(8)), str(p(9)), str(p(10)), long(p(11)),
          long(p(12)), long(p(13)), str(p(14)), str(p(15)), str(p(16)), long(p(17)))).toDF(),

      "customer_address" -> sc.textFile(inputDir + "/customer_address/*").map(_.split(separator, limit)).map(p =>
        CustomerAddress(long(p(0)), str(p(1)), str(p(2)), str(p(3)), str(p(4)), str(p(5)), str(p(6)), str(p(7)), str(p(8)), str(p(9)), str(p(10)), double(p(11)), str(p(12)))).toDF(),

      "customer_demographics" -> sc.textFile(inputDir + "/customer_demographics/*").map(_.split(separator, limit)).map(p =>
        CustomerDemographics(long(p(0)), str(p(1)), str(p(2)), str(p(3)), long(p(4)), str(p(5)), long(p(6)), long(p(7)), long(p(8)))).toDF(),

      "date_dim" -> sc.textFile(inputDir + "/date_dim/*").map(_.split(separator, limit)).map(p =>
        DateDim(long(p(0)), str(p(1)), str(p(2)), long(p(3)), long(p(4)), long(p(5)), long(p(6)), long(p(7)), long(p(8)), long(p(9)), long(p(10)), long(p(11)),
          long(p(12)), long(p(13)), str(p(14)), str(p(15)), str(p(16)), str(p(17)), str(p(18)), long(p(19)), long(p(20)), long(p(21)), long(p(22)), str(p(23)),
          str(p(24)), str(p(25)), str(p(26)), str(p(27)))).toDF(),

      "household_demographics" -> sc.textFile(inputDir + "/household_demographics/*").map(_.split(separator, limit)).map(p =>
        HouseholdDemographics(long(p(0)), long(p(1)), str(p(2)), long(p(3)), long(p(4)))).toDF(),

      "income_band" -> sc.textFile(inputDir + "/income_band/*").map(_.split(separator, limit)).map(p =>
        IncomeBand(long(p(0)), long(p(1)), long(p(2)))).toDF(),

      "item" -> sc.textFile(inputDir + "/item/*").map(_.split(separator, limit)).map(p =>
        Item(long(p(0)), str(p(1)), str(p(2)), str(p(3)), str(p(4)), double(p(5)), double(p(6)), long(p(7)), str(p(8)), long(p(9)), str(p(10)), long(p(11)),
          str(p(12)), long(p(13)), str(p(14)), str(p(15)), str(p(16)), str(p(17)), str(p(18)), str(p(19)), long(p(20)), str(p(21)))).toDF(),

      "promotion" -> sc.textFile(inputDir + "/promotion/*").map(_.split(separator, limit)).map(p =>
        Promotion(long(p(0)), str(p(1)), long(p(2)), long(p(3)), long(p(4)), double(p(5)), long(p(6)), str(p(7)), str(p(8)), str(p(9)), str(p(10)), str(p(11)),
          str(p(12)), str(p(13)), str(p(14)), str(p(15)), str(p(16)), str(p(17)), str(p(18)))).toDF(),

      "reason" -> sc.textFile(inputDir + "/reason/*").map(_.split(separator, limit)).map(p =>
        Reason(long(p(0)), str(p(1)), str(p(2)))).toDF(),

      "ship_mode" -> sc.textFile(inputDir + "/ship_mode/*").map(_.split(separator, limit)).map(p =>
        ShipMode(long(p(0)), str(p(1)), str(p(2)), str(p(3)), str(p(4)), str(p(5)))).toDF(),

      "store" -> sc.textFile(inputDir + "/store/*").map(_.split(separator, limit)).map(p =>
        Store(long(p(0)), str(p(1)), str(p(2)), str(p(3)), long(p(4)), str(p(5)), long(p(6)), long(p(7)), str(p(8)), str(p(9)), long(p(10)), str(p(11)),
          str(p(12)), str(p(13)), long(p(14)), str(p(15)), long(p(16)), str(p(17)), str(p(18)), str(p(19)), str(p(20)), str(p(21)), str(p(22)), str(p(23)),
          str(p(24)), str(p(25)), str(p(26)), double(p(27)), double(p(28)))).toDF(),

      "time_dim" -> sc.textFile(inputDir + "/time_dim/*").map(_.split(separator, limit)).map(p =>
        TimeDim(long(p(0)), str(p(1)), long(p(2)), long(p(3)), long(p(4)), long(p(5)), str(p(6)), str(p(7)), str(p(8)), str(p(9)))).toDF(),

      "warehouse" -> sc.textFile(inputDir + "/warehouse/*").map(_.split(separator, limit)).map(p =>
        Warehouse(long(p(0)), str(p(1)), str(p(2)), long(p(3)), str(p(4)), str(p(5)), str(p(6)), str(p(7)), str(p(8)), str(p(9)), str(p(10)), str(p(11)), str(p(12)), double(p(13)))).toDF(),

      "web_page" -> sc.textFile(inputDir + "/web_page/*").map(_.split(separator, limit)).map(p =>
        WebPage(long(p(0)), str(p(1)), str(p(2)), str(p(3)), long(p(4)), long(p(5)), str(p(6)), long(p(7)), str(p(8)), str(p(9)), long(p(10)), long(p(11)), long(p(12)), long(p(13)))).toDF(),

      "web_site" -> sc.textFile(inputDir + "/web_site/*").map(_.split(separator, limit)).map(p =>
        WebSite(long(p(0)), str(p(1)), str(p(2)), str(p(3)), str(p(4)), long(p(5)), long(p(6)), str(p(7)), str(p(8)), long(p(9)), str(p(10)), str(p(11)), str(p(12)),
          long(p(13)), str(p(14)), str(p(15)), str(p(16)), str(p(17)), str(p(18)), str(p(19)), str(p(20)), str(p(21)), str(p(22)), str(p(23)), str(p(24)), str(p(25)))).toDF(),

      "dbgen_version" -> sc.textFile(inputDir + "/dbgen_version/*").map(_.split(separator, limit)).map(p =>
        DbgenVersion(str(p(0)), str(p(1)), str(p(2)), str(p(3)))).toDF()
    )

    dfMap.foreach {
      case (key, value) =>
        value.write.format("parquet").option("compression", "none").mode(SaveMode.Overwrite).save(outputDir + "/" + key)
    }
  }
}