package com.eurowings.airbnb_usecase
import org.apache.spark.SparkContext
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.functions._
object Filter_columns {

  //def format_currency_data1: String => Double = _.trim.replaceAll("[ $,{,% ]", "").toDouble
  //def format_currency_data_udf1 = udf(format_currency_data1)

  //def format_numeric_data1: String => Int = _.trim.replaceAll("[ $,{,% ]", "").toInt
  //def format_numeric_data_udf1 = udf(format_numeric_data1)


  def format_currency_data(input: String): Double = {
    if(input.trim().isEmpty ){
      return 0.00
    }else{
      input.trim.replaceAll("[ $,{,% ]", "").toDouble
    }
  }

  def format_numeric_data(input: String): Int = {
    if(input.trim().isEmpty ){
      return 0
    }else{
      input.trim.replaceAll("[ $,{,% ]", "").toInt
    }
  }

  //def format_decimal_data1: String => Int = _.toInt
//  def format_decimal_data_udf1 = udf(format_decimal_data1)
  val format_currency_data_udf1 = udf((input : String) => format_currency_data(input))
  val format_numeric_data_udf1 = udf((input : String) => format_numeric_data(input))
/*
  def format_currency_data2 (S: String) :Double ={
    try {
      println("in here")
      var formatted:Double=0
      println(formatted)
      formatted=S.replaceAll("[$,{,]", "").trim.toDouble
      return formatted
    } catch {
      case e: Exception => println("Exception while converting currency values from input file")
        e.printStackTrace
        e.getMessage
      return 0.0
    }

  }

  def format_currency_data_udf2 = udf(format_currency_data2)

  */
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

    */
    val format_array=Array("price",
      "weekly_price",
      "monthly_price",
      "security_deposit",
      "cleaning_fee",
      "extra_people")

    //original_df.withColumn("new_price",format_data_udf($"price")).select("new_price").show(100)
/*
   val formatted_df=original_df.columns.map(x=> {
                                                if (format_array.contains(x)) {
                                                  print("got it",x)

                                                  original_df.withColumn(s"$x formatted",format_currency_data_udf1($""$x""))
                                                }else {
                                                  print("doesnt exist",x)
                                                }})

*/
    //print(formatted_df.getClass())

    //formatted_df.foreach(print)
      println("now back after schema")

    val formatted_df2=original_df
     // .withColumn("price_formatted", format_currency_data_udf1( $"price"))

      .withColumn("price_formatted", format_currency_data_udf1(when ($"price".isNull,0.00).otherwise($"price")))

      .withColumn("weekly_price_formatted",format_currency_data_udf1(when ($"weekly_price".isNull,0.00).otherwise($"weekly_price")))
      .withColumn("monthly_price_formatted",format_currency_data_udf1(when ($"monthly_price".isNull,0.00).otherwise($"monthly_price")))
      .withColumn("security_deposit_formatted",format_currency_data_udf1(when ($"security_deposit".isNull,0.00).otherwise($"security_deposit")))
      .withColumn("cleaning_fee_formatted",format_currency_data_udf1(when ($"cleaning_fee".isNull,0.00).otherwise($"cleaning_fee")))
      .withColumn("extra_people_formatted",format_currency_data_udf1(when ($"extra_people".isNull,0.00).otherwise($"extra_people")))
      .withColumn("bathrooms_formatted",format_currency_data_udf1(when ($"bathrooms".isNull,0.00).otherwise($"bathrooms")))
      .withColumn("reviews_per_month_formatted",format_currency_data_udf1(when ($"reviews_per_month".isNull,0.00).otherwise($"reviews_per_month")))


      .withColumn("id_formatted",format_numeric_data_udf1(when ($"id".isNull,0).otherwise($"id")))
      .withColumn("host_id_formatted",format_numeric_data_udf1(when ($"host_id".isNull,0).otherwise($"host_id")))
      .withColumn("host_response_rate_formatted",format_numeric_data_udf1(when ($"host_response_rate".isNull,0).otherwise($"host_response_rate")))
      .withColumn("host_acceptance_rate_formatted",format_numeric_data_udf1(when ($"host_acceptance_rate".isNull,0).otherwise($"host_acceptance_rate")))
      .withColumn("host_listings_count_formatted",format_numeric_data_udf1(when ($"host_listings_count".isNull,0).otherwise($"host_listings_count")))
      .withColumn("host_total_listings_count_formatted",format_numeric_data_udf1(when ($"host_total_listings_count".isNull,0).otherwise($"host_total_listings_count")))
      .withColumn("accommodates_formatted",format_numeric_data_udf1(when ($"accommodates".isNull,0).otherwise($"accommodates")))


      .withColumn("bedrooms_formatted",format_numeric_data_udf1(when ($"bedrooms".isNull,0).otherwise($"bedrooms")))
      .withColumn("beds_formatted",format_numeric_data_udf1(when ($"beds".isNull,0).otherwise($"beds")))
      .withColumn("square_feet_formatted",format_numeric_data_udf1(when ($"square_feet".isNull,0).otherwise($"square_feet")))
      .withColumn("guests_included_formatted",format_numeric_data_udf1(when ($"guests_included".isNull,0).otherwise($"guests_included")))
      .withColumn("minimum_nights_formatted",format_numeric_data_udf1(when ($"minimum_nights".isNull,0).otherwise($"minimum_nights")))
      .withColumn("maximum_nights_formatted",format_numeric_data_udf1(when ($"maximum_nights".isNull,0).otherwise($"maximum_nights")))
      .withColumn("availability_30_formatted",format_numeric_data_udf1(when ($"availability_30".isNull,0).otherwise($"availability_30")))
      .withColumn("availability_60_formatted",format_numeric_data_udf1(when ($"availability_60".isNull,0).otherwise($"availability_60")))
      .withColumn("availability_90_formatted",format_numeric_data_udf1(when ($"availability_90".isNull,0).otherwise($"availability_90")))
      .withColumn("availability_365_formatted",format_numeric_data_udf1(when ($"availability_365".isNull,0).otherwise($"availability_365")))
      .withColumn("number_of_reviews_formatted",format_numeric_data_udf1(when ($"number_of_reviews".isNull,0).otherwise($"number_of_reviews")))
      .withColumn("review_scores_rating_formatted",format_numeric_data_udf1(when ($"review_scores_rating".isNull,0).otherwise($"review_scores_rating")))
      .withColumn("review_scores_accuracy_formatted",format_numeric_data_udf1(when ($"review_scores_accuracy".isNull,0).otherwise($"review_scores_accuracy")))
      .withColumn("review_scores_cleanliness_formatted",format_numeric_data_udf1(when ($"review_scores_cleanliness".isNull,0).otherwise($"review_scores_cleanliness")))
      .withColumn("review_scores_checkin_formatted",format_numeric_data_udf1(when ($"review_scores_checkin".isNull,0).otherwise($"review_scores_checkin")))
      .withColumn("review_scores_communication_formatted",format_numeric_data_udf1(when ($"review_scores_communication".isNull,0).otherwise($"review_scores_communication")))
      .withColumn("review_scores_location_formatted",format_numeric_data_udf1(when ($"review_scores_location".isNull,0).otherwise($"review_scores_location")))
      .withColumn("review_scores_value_formatted",format_numeric_data_udf1(when ($"review_scores_value".isNull,0).otherwise($"review_scores_value")))
      .withColumn("calculated_host_listings_count_formatted",format_numeric_data_udf1(when ($"calculated_host_listings_count".isNull,0).otherwise($"calculated_host_listings_count")))


 .withColumn("latitude_formatted",$"latitude".cast("Double"))
 .withColumn("longitude_formatted",$"longitude".cast("Double"))



/*
   :boolean columns
    host_has_profile_pic
    host_identity_verified
    is_location_exact
    has_availability
    requires_license
    instant_bookable
    is_business_travel_ready
    require_guest_profile_picture
    require_guest_phone_verification
*/


    /*
  date fields
  */


    formatted_df2.select($"price_formatted").show(100)

    formatted_df2.groupBy("price_formatted").sum("price_formatted").show(100)




    //original_df.withColumn("latest_curr",format_currency_data_udf1($"monthly_price")).select("latest_curr").show(100)

    println("now back after schema step2")

    //formatted_df2.show(100)

    println("now back after schema step3")
    //original_df.schema.fieldNames.foreach(println)
    //original_df.schema.foreach(println)
    formatted_df2.show(10)

    spark.stop()
  }
}


// columns with $ : price	,weekly_price,	monthly_price,	security_deposit,	cleaning_fee, extra_people


//columns as list : host_verifications, amenities, jurisdiction_names