package com.sword.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

case class Employee(id: Int, name: String, department: String, salary: Double, age: Int)

object SecondSparkJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Second_Spark_Job")
      .getOrCreate()

    import spark.implicits._

    val employees = Seq(
      Employee(1, "Alice", "Engineering", 7000.0, 30),
      Employee(2, "Bob", "HR", 5000.0, 45),
      Employee(3, "Charlie", "Engineering", 8000.0, 25),
      Employee(4, "David", "Marketing", 6000.0, 35),
      Employee(5, "Eve", "Engineering", 7500.0, 28)
    )

    val df = employees.toDF()
    df.show(false)

    df.printSchema()

    df.select(col("name"), col("department")).show(false)

    df.filter(col("salary") > 6000).show(false)

    df.filter(col("department") === "Engineering").show(false)

    df.withColumn("senior", col("age") > 30).show(false)

    df.withColumnRenamed("salary", "monthly_salary").show(false)

    spark.stop()
  }
}