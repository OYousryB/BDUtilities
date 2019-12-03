package com.utilities

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.{SparkConf, SparkContext}

object SparkBuilder {
  private var sparkContext: SparkContext = _
  private var sparkSession: SparkSession = _

  private def createSparkContext = {
    val conf = new SparkConf().setAppName("Big Data Utility Helper")
    sparkContext = {
        if (!conf.contains("spark.master")) {
          System.err.println(("\n" + "#"*80)*2)
          System.err.println("##" + " "*76 + "##")
          System.err.println("##" + " "*26 + "USING LOCAL[*] AS MASTER" + " "*26 + "##")
          System.err.println("##" + " "*76 + "##")
          System.err.println(("#"*80 + "\n")*2)
          conf.setMaster("local[*]")
        }
        new SparkContext(conf)
      }
    sparkContext
  }

  def setSparkContext(sparkContext: SparkContext): Unit = this.sparkContext = sparkContext

  def sc: SparkContext = if (sparkContext != null) sparkContext else createSparkContext

  private def createSparkSession = {
    sc
    sparkSession = SparkSession.builder().getOrCreate()
    sparkSession
  }

  def setSparkSession(sparkSession: SparkSession): Unit = {
    this.sparkSession = sparkSession
  }

  def spark: SparkSession = if (sparkSession == null) createSparkSession else sparkSession

  def sqlContext: SQLContext = spark.sqlContext

  def sql(query: String): DataFrame = spark.sql(query)

  def stop(): Unit = {
    if (sparkContext != null) {
      sparkContext.stop()
    }

    sparkContext = null
    sparkSession = null
  }

  def addStrategy(strategy: Strategy): Unit =
    sqlContext.experimental.extraStrategies = sqlContext.experimental.extraStrategies :+ strategy

  def addOptimization(optimization: Rule[LogicalPlan]): Unit =
    sqlContext.experimental.extraOptimizations = sqlContext.experimental.extraOptimizations :+ optimization

  def clearOptimizations(): Unit =
    sqlContext.experimental.extraOptimizations = Seq()
}
