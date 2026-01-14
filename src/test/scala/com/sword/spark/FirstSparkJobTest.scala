package com.sword.spark

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession

class FirstSparkJobTest extends AnyFunSuite {

  test("Spark session should be created") {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("test")
      .getOrCreate()

    assert(spark != null)
    assert(spark.version.startsWith("3.5"))

    spark.stop()
  }
}