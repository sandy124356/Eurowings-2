package com.eurowings
import org.apache.spark.SparkContext
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object testing {



  def main(args: Array[String]): Unit ={

    val spark= new SparkSession.Builder().master("local[*]").getOrCreate()

   // val rdd=spark.sparkContext.textFile("hdfs://localhost:9000/eurowings/eurowings/listings_2018-01-01.csv")

    import spark.implicits._
    //val newrdd= rdd.map(x=>x.replace(",","@#$"))

   // val schemas=StructType(List(
    //  StructField("id",IntegerType,false),
     // StructField("name",StringType,false),
      //StructField("address",StringType,true)

    //)
    //)

    val schemas=StructType(List(
      StructField("id",IntegerType,false),
      StructField("listing_url",StringType,true),
      StructField("scrape_id",StringType,true),
      StructField("last_scraped",StringType,true),
      StructField("name",StringType,true),
      StructField("summary",StringType,true),
      StructField("space",StringType,true),
      StructField("description",StringType,true)))

    val df_original=spark.read.option("header","true")
      .schema(schemas)
      .option("delimiter",",")
      .option("escape", '"')
       //.option("escape", "")
       .csv("C:\\Users\\u6062310\\Desktop\\eurowings\\personal\\listings_2018-01-01.csv")
    //newrdd.saveAsTextFile("C:\\Users\\u6062310\\Desktop\\out")

   // val df_original=spark.read.option("header","true")
     // .option("ignoreLeadingWhiteSpace", "true")
      //.option("delimiter",",")
      //.option("inferSchema", "true")
      //.csv("C:\\Users\\u6062310\\Desktop\\eurowings\\modified.csv")


    //.csv("hdfs://localhost:9000/eurowings/eurowings/listings_2018-01-01.csv")



    df_original.printSchema()

    df_original.select("id").show(10)

    df_original.show()

    df_original.write.option("header","true").csv("C:\\Users\\u6062310\\Desktop\\out\\")

    // df_original.select("id").show(100)

  }
}
