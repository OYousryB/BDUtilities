package benchmark.tpch.run

import java.io.FileInputStream

import benchmark.BenchmarkQuery
import benchmark.tpch.run.schemaProvider.VanillaTpchSchemaProvider
import com.utilities.SparkBuilder


object QueryTestSQL {

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      println("Missing args")
      return
    }

    val inputDir = args(0)
    val outputDir = inputDir + "/output"

    val schemaProvider = {
        SparkBuilder.clearOptimizations()
        new VanillaTpchSchemaProvider(inputDir)
      }

    val engine = "spark"

    val queryIndices =
      if (args.length > 1)
        Seq(args(1).toInt)
      else
        1 to 22

    val source = new FileInputStream("src/main/scala/benchmark/tpch/schema/tpch_sql_queries_raw.txt")
    val buf = new Array[Byte](source.available())
    source.read(buf)
    val queryText = new String(buf).split('\n')

    val queries = queryIndices.map(queryNo => (SparkBuilder.sql(queryText(queryNo - 1)), s"Q$queryNo"))

    BenchmarkQuery.process("tpch", engine, queries, outputDir)
  }
}
