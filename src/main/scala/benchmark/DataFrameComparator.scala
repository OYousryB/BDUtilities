package benchmark

import java.io.File

import com.utilities.SparkBuilder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

import scala.collection.mutable.ListBuffer

object DataFrameComparator {

  var DECIMAL_PLACES = 2

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("Missing args")
      return
    }

    val dir1 = args(0)
    val dir2 = args(1)

    if (args.length > 2) {
      DECIMAL_PLACES = args(2).toInt
    }

    val compareItems = new File(dir1).listFiles().toList.sortBy(_.getName)
      .zip(new File(dir2).listFiles().toList.sortBy(_.getName))

    val results = new ListBuffer[(String, String, (Boolean, String))]

    for (c <- compareItems) {
      val df1Path = c._1.getAbsolutePath
      val df2Path = c._2.getAbsolutePath

      val res = compareUnordered(
        SparkBuilder.spark.read.format("parquet").load(df1Path),
        SparkBuilder.spark.read.format("parquet").load(df2Path)
      )

      results += ((df1Path, df2Path, res))
    }

    for (r <- results) {
      println(r)
    }
  }

  def compareUnordered(df1: DataFrame, df2: DataFrame): (Boolean, String) = {

    var d1 = df1
    var d2 = df2

    for (c <- d1.schema.fields) {
      d1 =
        if (c.dataType == DataTypes.FloatType || c.dataType == DataTypes.DoubleType)
          d1.withColumn(c.name, format_number(d1.col(c.name), DECIMAL_PLACES))
        else
          d1
    }

    for (c <- d2.schema.fields) {
      d2 =
        if (c.dataType == DataTypes.FloatType || c.dataType == DataTypes.DoubleType)
          d2.withColumn(c.name, format_number(d2.col(c.name), DECIMAL_PLACES))
        else
          d2
    }

    d1 = d1.cache()
    d2 = d2.cache()

    val d1Count = d1.count()
    val d2Count = d2.count()

    val difference = d1.except(d2)

    if (d1Count != d2Count) {
      df1.show()
      df2.show()
      (false, s"Mismatching count; df1 = $d1Count, df2 = $d2Count")
    }
    else if (difference.count() != 0) {
      difference.show()
      (false, s"Mismatching result; df1 != df2.")
    }
    else
      (true, "SUCCESS")
  }
}
