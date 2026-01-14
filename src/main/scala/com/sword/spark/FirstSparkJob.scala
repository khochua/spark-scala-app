package com.sword.spark
import org.apache.spark.sql.SparkSession

object FirstSparkJob {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("First_Spark_Job")
      .getOrCreate()

    import spark.implicits._

    val pDate = args(0)

    spark.stop()
  }
}