package benchmark.tpch.run.queries

import benchmark.BenchmarkQuery
import benchmark.tpch.run.schemaProvider.TpchSchemaProvider
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{sum, udf}

class Q08 extends BenchmarkQuery {

  override def execute(sc: SparkContext, schemaProvider: benchmark.SchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val tpc = schemaProvider.asInstanceOf[TpchSchemaProvider]
    import sqlContext.implicits._
    import tpc._

    val getYear = udf { (x: String) => x.substring(0, 4) }
    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val isBrazil = udf { (x: String, y: Double) => if (x == "BRAZIL") y else 0 }

    val fregion = region.filter($"r_name" === "AMERICA")
    val forder = orders.filter($"o_orderdate" <= "1996-12-31" && $"o_orderdate" >= "1995-01-01")
    val fpart = part.filter($"p_type" === "ECONOMY ANODIZED STEEL")

    val nat = nation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))

    val line = lineitem.select($"l_partkey", $"l_suppkey", $"l_orderkey",
      decrease($"l_extendedprice", $"l_discount").as("volume")).
      join(fpart, $"l_partkey" === fpart("p_partkey"))
      .join(nat, $"l_suppkey" === nat("s_suppkey"))

    nation.join(fregion, $"n_regionkey" === fregion("r_regionkey"))
      .select($"n_nationkey")
      .join(customer, $"n_nationkey" === customer("c_nationkey"))
      .select($"c_custkey")
      .join(forder, $"c_custkey" === forder("o_custkey"))
      .select($"o_orderkey", $"o_orderdate")
      .join(line, $"o_orderkey" === line("l_orderkey"))
      .select(getYear($"o_orderdate").as("o_year"), $"volume",
        isBrazil($"n_name", $"volume").as("case_volume"))
      .groupBy($"o_year")
      .agg(sum($"case_volume") / sum("volume"))
      .sort($"o_year")
  }

}
