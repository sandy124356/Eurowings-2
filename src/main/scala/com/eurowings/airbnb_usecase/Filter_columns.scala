package com.eurowings.airbnb_usecase
import org.apache.spark.SparkContext
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.functions._
import java.sql.Date
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
    import java.sql.Date

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


    .withColumn("last_scraped_date",to_date(unix_timestamp(col("last_scraped"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("host_since_date",to_date(unix_timestamp(col("host_since"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("calendar_last_scraped_date",to_date(unix_timestamp(col("calendar_last_scraped"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("first_review_date",to_date(unix_timestamp(col("first_review"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("last_review_date",to_date(unix_timestamp(col("last_review"),"MM/dd/yyyy").cast("timestamp")))



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
  date fields:

  last_scraped
  host_since
  calendar_last_scraped
  first_review
  last_review

  */


    //formatted_df2.select($"price_formatted").show(100)

    //formatted_df2.groupBy("price_formatted").sum("price_formatted").show(100)




    //original_df.withColumn("latest_curr",format_currency_data_udf1($"monthly_price")).select("latest_curr").show(100)

    println("now back after schema step2")

    //formatted_df2.show(100)

    println("now back after schema step3")
    //original_df.schema.fieldNames.foreach(println)
    //original_df.schema.foreach(println)
    //formatted_df2.show(10)

    val AllcolumnNameSeq =
      Seq("id_formatted",
        "listing_url",
        "scrape_id",
        "last_scraped_date",
        "name",
        "summary",
        "space",
        "description",
        "experiences_offered",
        "neighborhood_overview",
        "notes",
        "transit",
        "access",
        "interaction",
        "house_rules",
        "thumbnail_url",
        "medium_url",
        "picture_url",
        "xl_picture_url",
        "host_id_formatted",
        "host_url",
        "host_name",
        "host_since_date",
        "host_location",
        "host_about",
        "host_response_time",
        "host_response_rate_formatted",
        "host_acceptance_rate_formatted",
        "host_is_superhost",
        "host_thumbnail_url",
        "host_picture_url",
        "host_neighbourhood",
        "host_listings_count_formatted",
        "host_total_listings_count_formatted",
        "host_verifications",
        "host_has_profile_pic",
        "host_identity_verified",
        "street",
        "neighbourhood",
        "neighbourhood_cleansed",
        "neighbourhood_group_cleansed",
        "city",
        "state",
        "zipcode",
        "market",
        "smart_location",
        "country_code",
        "country",
        "latitude_formatted",
        "longitude_formatted",
        "is_location_exact",
        "property_type",
        "room_type",
        "accommodates_formatted",
        "bathrooms_formatted",
        "bedrooms_formatted",
        "beds",
        "bed_type",
        "amenities",
        "square_feet_formatted",
        "price_formatted",
        "weekly_price_formatted",
        "monthly_price_formatted",
        "security_deposit_formatted",
        "cleaning_fee_formatted",
        "guests_included_formatted",
        "extra_people_formatted",
        "minimum_nights_formatted",
        "maximum_nights_formatted",
        "calendar_updated",
        "has_availability",
        "availability_30_formatted",
        "availability_60_formatted",
        "availability_90_formatted",
        "availability_365_formatted",
        "calendar_last_scraped_date",
        "number_of_reviews_formatted",
        "first_review_date",
        "last_review_date",
        "review_scores_rating_formatted",
        "review_scores_accuracy_formatted",
        "review_scores_cleanliness_formatted",
        "review_scores_checkin_formatted",
        "review_scores_communication_formatted",
        "review_scores_location_formatted",
        "review_scores_value_formatted",
        "requires_license",
        "license",
        "jurisdiction_names",
        "instant_bookable",
        "is_business_travel_ready",
        "cancellation_policy",
        "require_guest_profile_picture",
        "require_guest_phone_verification",
        "calculated_host_listings_count_formatted",
        "reviews_per_month_formatted"
    )
    //val final_df=formatted_df2.select(col("abc"))
    val final_df=formatted_df2.select(AllcolumnNameSeq.map(x=>col(x)):_* )

    final_df.show(100)

    final_df.select("id_formatted").show(100)

  // case class sss(a:Double)

    case class airbnb_data(		id_formatted:Int,
                               listing_url:String,
                               scrape_id:String,
                               last_scraped_date:String,
                               name:String,
                               summary:String,
                               space:String,
                               description:String,
                               experiences_offered:String,
                               neighborhood_overview:String,
                               notes:String,
                               transit:String,
                               access:String,
                               interaction:String,
                               house_rules:String,
                               thumbnail_url:String,
                               medium_url:String,
                               picture_url:String,
                               xl_picture_url:String,
                               host_id_formatted:Int,
                               host_url:String,
                               host_name:String,
                               host_since_date:String,
                               host_location:String,
                               host_about:String,
                               host_response_time:String,
                               host_response_rate_formatted:Int,
                               host_acceptance_rate_formatted:Int,
                               host_is_superhost:String,
                               host_thumbnail_url:String,
                               host_picture_url:String,
                               host_neighbourhood:String,
                               host_listings_count_formatted:Int,
                               host_total_listings_count_formatted:Int,
                               host_verifications:String,
                               host_has_profile_pic:String,
                               host_identity_verified:String,
                               street:String,
                               neighbourhood:String,
                               neighbourhood_cleansed:String,
                               neighbourhood_group_cleansed:String,
                               city:String,
                               state:String,
                               zipcode:String,
                               market:String,
                               smart_location:String,
                               country_code:String,
                               country:String,
                               latitude_formatted:Double,
                               longitude_formatted:Double,
                               is_location_exact:String,
                               property_type:String,
                               room_type:String,
                               accommodates_formatted:Int,
                               bathrooms_formatted:Double,
                               bedrooms_formatted:Int,
                               beds:Int,
                               bed_type:String,
                               amenities:String,
                               square_feet_formatted:Int,
                               price_formatted:Double,
                               weekly_price_formatted:Double,
                               monthly_price_formatted:Double,
                               security_deposit_formatted:Double,
                               cleaning_fee_formatted:Double,
                               guests_included_formatted:Int,
                               extra_people_formatted:Double,
                               minimum_nights_formatted:Int,
                               maximum_nights_formatted:Int,
                               calendar_updated:String,
                               has_availability:String,
                               availability_30_formatted:Int,
                               availability_60_formatted:Int,
                               availability_90_formatted:Int,
                               availability_365_formatted:Int,
                               calendar_last_scraped_date:String,
                               number_of_reviews_formatted:Int,
                               first_review_date:String,
                               last_review_date:String,
                               review_scores_rating_formatted:Int,
                               review_scores_accuracy_formatted:Int,
                               review_scores_cleanliness_formatted:Int,
                               review_scores_checkin_formatted:Int,
                               review_scores_communication_formatted:Int,
                               review_scores_location_formatted:Int,
                               review_scores_value_formatted:Int,
                               requires_license:String,
                               license:String,
                               jurisdiction_names:String,
                               instant_bookable:String,
                               is_business_travel_ready:String,
                               cancellation_policy:String,
                               require_guest_profile_picture:String,
                               require_guest_phone_verification:String,
                               calculated_host_listings_count_formatted:Int,
                               reviews_per_month_formatted:Double)

    println("num of columns are :",final_df.columns.size)

    //val final_dataset =final_df.as[airbnb_data]

    //final_dataset.select("host_acceptance_rate_formatted").show(100)

    spark.stop()
  }
}


// columns with $ : price	,weekly_price,	monthly_price,	security_deposit,	cleaning_fee, extra_people


//columns as list : host_verifications, amenities, jurisdiction_names