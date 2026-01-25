package com.sword.spark
import org.apache.spark.sql.types.{StringType, StructType, StructField}
import org.apache.spark.sql.Column
import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}
import org.apache.spark.sql.functions._

object FirstSparkJob {

  def extractJsonStruct(jsonCol: Column, schema: StructType): Column = {
    struct(schema.fields.map { field =>
      get_json_object(jsonCol, s"$$.${field.name}").cast(field.dataType).alias(field.name)
    }: _*)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("First_Spark_Job")
      .getOrCreate()

    val schema = StructType(Array(
      StructField("id", StringType, true),
      StructField("name", StringType, true),
      StructField("is_active", StringType, true),
      StructField("salary", StringType, true)
    ))

    val data = Seq(
      """{"id": "1", "name": "Alice", "is_active": "true", "salary": "5000"}""",
      """{"id": "2", "name": "Bob", "is_active": "false", "salary": "6000"}""",
      """{"id": "3", "name": "Charlie", "is_active": "true", "salary": null}"""
    )
    import spark.implicits._
    val df = data.toDF("raw_json")

    val resultDf = df.withColumn("json_data", extractJsonStruct(col("raw_json"), schema))
    
    resultDf.select(col("raw_json"), col("json_data.*")).show(false)

    spark.stop()
  }
}