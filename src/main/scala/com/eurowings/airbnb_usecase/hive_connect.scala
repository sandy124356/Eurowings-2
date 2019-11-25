package com.eurowings.airbnb_usecase
import com.eurowings.airbnb_usecase.Filter_columns
import org.apache.spark.sql.{Row, SaveMode, SparkSession}


object hive_connect {

  def main(args: Array[String]): Unit = {

    val spark = new SparkSession
    .Builder()
      .master("local[*]")
      .appName("Eurowings_dataEngine")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql
    spark.sql("use airbnb_data")


  }
}

