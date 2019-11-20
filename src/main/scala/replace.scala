package com.eurowings
import org.apache.spark.SparkContext
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object replace {



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

    val rdd=spark.sparkContext.textFile("C:\\Users\\u6062310\\Desktop\\eurowings\\listings_2018-01-01.csv")

      val rdd2=rdd.map(x=>x.replaceAll("[\r\n]", " "))

    rdd2.take(2).foreach(println)
    //newrdd.saveAsTextFile("C:\\Users\\u6062310\\Desktop\\out")

    // val df_original=spark.read.option("header","true")
    // .option("ignoreLeadingWhiteSpace", "true")
    //.option("delimiter",",")
    //.option("inferSchema", "true")
    //.csv("C:\\Users\\u6062310\\Desktop\\eurowings\\modified.csv")


    //.csv("hdfs://localhost:9000/eurowings/eurowings/listings_2018-01-01.csv")





    // df_original.select("id").show(3)


    val df = spark.read
      .option("sep", ",")
      .option("quote", "\"")
      .option("multiLine", "true")

      .option("header","true")
      .csv("C:\\Users\\u6062310\\Desktop\\eurowings\\listings_2018-01-01.csv")

    df.take(1).foreach(println)






  }
}
