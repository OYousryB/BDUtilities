package benchmark.tpch.run

import benchmark.BenchmarkQuery
import benchmark.tpch.run.schemaProvider.VanillaTpchSchemaProvider
import com.utilities.SparkBuilder

object QueryTest {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("Missing args")
      return
    }

    val inputDir = args(0)
    val provider = args(1)
    val outputDir = inputDir + "/output"

    val schemaProvider = {
      SparkBuilder.clearOptimizations()
      new VanillaTpchSchemaProvider(inputDir)
    }

    val engine = "spark"

    val queryIndices =
      if (args.length > 2)
        Seq(args(2).toInt)
      else
        1 to 22

    val queries = queryIndices
      .map(queryNo =>
        (
          Class.forName(f"benchmark.tpch.run.queries.Q$queryNo%02d")
            .newInstance.asInstanceOf[BenchmarkQuery]
            .execute(SparkBuilder.sc, schemaProvider),
          s"Q$queryNo"
        )
      )

    BenchmarkQuery.process("tpch", engine, queries, outputDir)
  }
}
