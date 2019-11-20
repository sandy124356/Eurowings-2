package com.eurowings.airbnb_usecase
import org.apache.spark.SparkContext
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object Filter_columns_old {

  def format_currency_data1: String => Double = _.trim.replaceAll("[$,{,]", "").toDouble
  def format_currency_data_udf1 = udf(format_currency_data1)

  def main(args: Array[String]): Unit ={

    val spark= new SparkSession.Builder().master("local[*]").appName("Eurowings_dataEngine")getOrCreate()

    import spark.implicits._

    val original_df= spark.read
      .option("wholeFile", true)
      .option("multiLine", "true")
      .option("delimiter",",")
      .option("header","true")
      .option("escape", "\"")
      .csv("C:\\Users\\u6062310\\Desktop\\eurowings\\listings_2018-01-01.csv")

    original_df.printSchema()

    original_df.take(1).foreach(print)

    original_df.select("id").show(100)

    original_df.select("price").show(100)

   // original_df.withColumn("real_price",$"price")


/*
    def format_currency_data2=(S: String) =>{
      try {
        println("in here")
        var formatted:Double=0

        formatted=S.replaceAll("[$,{,]", "").trim.toDouble
        println(formatted)

      } catch {
        case e: Exception => println("Exception while converting currency values from input file")
          e.printStackTrace
          e.getMessage
      }

    }

    def format_currency_data_udf2 = udf(format_currency_data2)
*/
    /*val format_array=Array("price",
      "weekly_price",
      "monthly_price",
      "security_deposit",
      "cleaning_fee",
      "extra_people")

    //original_df.withColumn("new_price",format_data_udf($"price")).select("new_price").show(100)

   val formatted_df=original_df.columns.map(x=> if (format_array.contains(x)) {
     print("got it",x)
     format_currency_data_udf1(original_df.col(x))} else {
     original_df.col(x)
     print("doesnt exist",x)})

    //print(formatted_df.getClass())

    //formatted_df.foreach(print)
      println("now back after schema")
*/
    val formatted_df2=original_df
      .withColumn("price_formatted", format_currency_data_udf1($"price") )
      .withColumn("weekly_price_formatted",format_currency_data_udf1($"weekly_price"))
      .withColumn("monthly_price_formatted",format_currency_data_udf1($"monthly_price"))
      .withColumn("security_deposit_formatted",format_currency_data_udf1($"security_deposit"))
      .withColumn("cleaning_fee_formatted",format_currency_data_udf1($"cleaning_fee"))
      .withColumn("extra_people_formatted",format_currency_data_udf1($"extra_people"))

    formatted_df2.select($"price_formatted").show(100)

    formatted_df2.groupBy("price_formatted").sum("price_formatted").show(100)




    //original_df.withColumn("latest_curr",format_currency_data_udf1($"monthly_price")).select("latest_curr").show(100)

    println("now back after schema step2")

    //formatted_df2.show(100)

    println("now back after schema step3")
    //original_df.schema.fieldNames.foreach(println)
    //original_df.schema.foreach(println)
    spark.stop()
  }
}


// columns with $ : price	,weekly_price,	monthly_price,	security_deposit,	cleaning_fee, extra_people


//columns as list : host_verifications, amenities, jurisdiction_names