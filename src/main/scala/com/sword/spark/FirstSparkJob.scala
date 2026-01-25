package com.sword.spark
import org.apache.spark.sql.types.{StringType}
import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}
import org.apache.spark.sql.functions._

object FirstSparkJob {

  def requireEnv(name: String): String =
    sys.env.getOrElse(name,
      throw new IllegalArgumentException(s"Missing ENV variable: $name")
    )

  val JDBC_URL = requireEnv("JDBC_URL")
  val DB_USER = requireEnv("DB_USER")
  val DB_PASS = requireEnv("DB_PASSWORD")
  val JDBC_PROPS = Map(
    "user" -> DB_USER,
    "password" -> DB_PASS,
    "driver" -> "org.postgresql.Driver"
  )

  // val KAFKA_BOOTSTRAP_SERVERS = requireEnv("KAFKA_BOOTSTRAP_SERVERS")
  // val KAFKA_USERNAME = requireEnv("KAFKA_USERNAME")
  // val KAFKA_PASSWORD = requireEnv("KAFKA_PASSWORD")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("First_Spark_Job")
      .getOrCreate()


    spark.stop()
  }

}