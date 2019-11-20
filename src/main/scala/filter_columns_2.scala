package com.eurowings.airbnb_usecase
import org.apache.spark.SparkContext
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object Filter_columns_2 {



  def main(args: Array[String]): Unit ={

    val spark= new SparkSession.Builder().master("local[*]").appName("Eurowings_dataEngine")getOrCreate()

    import spark.implicits._

    Thread.sleep(3000)
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

    original_df.withColumn("real_price",$"price")

    def format_data: String => String = _.trim.replaceAll("[$,{,]", "")
    def format_data_udf = udf(format_data)


    val format_array=Array("price",
      "weekly_price",
      "monthly_price",
      "security_deposit",
      "cleaning_fee",
      "extra_people")

    //original_df.withColumn("new_price",format_data_udf($"price")).select("new_price").show(100)

   /* val formatted_df=original_df.columns.map(x=> if (format_array.contains(x)) {
      print("got it",x)
      format_data_udf(original_df.col(x))} else {
      original_df.col(x)
      print("doesnt exist",x)})

    print(formatted_df.getClass())

    formatted_df.foreach(print)
*/

    val formatted_df2=original_df
      .withColumn("price_formatted", format_data_udf($"price") )
      .withColumn("weekly_price_formatted",format_data_udf($"weekly_price"))
      .withColumn("monthly_price_formatted",format_data_udf($"monthly_price"))
      .withColumn("security_deposit_formatted",format_data_udf($"security_deposit"))
      .withColumn("cleaning_fee_formatted",format_data_udf($"cleaning_fee"))
      .withColumn("extra_people_formatted",format_data_udf($"extra_people"))

    original_df.withColumn("latest_curr",format_data_udf($"monthly_price")).select("latest_curr").show(100)

    original_df.schema.foreach(println)
  }
}


// columns with $ : price	,weekly_price,	monthly_price,	security_deposit,	cleaning_fee, extra_people


//columns as list : host_verifications, amenities, jurisdiction_names