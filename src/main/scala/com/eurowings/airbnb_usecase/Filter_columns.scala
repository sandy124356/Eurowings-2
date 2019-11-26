//Developed in spark 2.4.3
//Hive 2.1
//sbt 1.3 for build

package com.eurowings.airbnb_usecase
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import java.sql.Date
object Filter_columns {


  //Functions used to format data coming from Excel for easy type conversions while working with dataframes and datasets

  //  format_currency_data can be used by columns referring currencies/pricing
  def format_currency_data(input: String): Double = {
    if(input.trim().isEmpty ){
      return 0.00.toDouble
    }else{
      input.trim.replaceAll("[ $,{,% ]", "").toDouble
    }
  }

  // format_numeric_data can be used by columns whose values should be Integers
  def format_numeric_data(input: String): Int = {
    if(input.trim().isEmpty ){
      return 0.toInt
    }else{
      input.trim.replaceAll("[ $,{,% ]", "").toInt
    }
  }
  //format_boolean_data_original can be used by columns whose values can be either true or false. lets consider them as boolean type for next processes.
  def format_boolean_data(input: String): Boolean = {
    if (input != null)
    {
      if (input.trim().toLowerCase() == "t")
        return true
      else (input.trim().toLowerCase() == "f")
      return false
    }
    else
      return false
  }


  // format_amenities will be used by amenities column in the file, this takes input String and make it a comma(,) seperated String
  def format_amenities(input: String): String = {
    if(input.isEmpty ){
      return null
    }else{
      input.replace("{", "").replace("}", "").replace("\"", "").replace("translation missing: en.hosting_amenity_49","").replace("translation missing: en.hosting_amenity_50","")
    }
  }

  // format_host_verifications used by host_verifications column in the file. open/close brackets will be removed to make it comma seperated
    def format_host_verifications(input: String): String = {
    if(input.isEmpty ){
      return null
    }else{
      //input.replace("[","").replace("]","").replace("'","")
      input.replaceAll("[\\[\\]\\']","")
    }
  }


 //Below functions will be used to register the scala functions into spark udfs, so that they can be used directly in spark-sql.

  //def format_decimal_data1: String => Int = _.toInt
//  def format_decimal_data_udf1 = udf(format_decimal_data1)
  val format_currency_data_udf1 = udf((input : String) => format_currency_data(input))
  val format_numeric_data_udf1 = udf((input : String) => format_numeric_data(input))
  val format_boolean_data_udf1 = udf((input : String) => format_boolean_data(input))
  val format_amenities_data_udf1=udf((input:String)=> format_amenities(input))
  val format_host_verifications_data_udf1=udf((input:String)=>format_host_verifications(input))

  def main(args: Array[String]): Unit ={

    val spark= new SparkSession
                  .Builder()
                  .master("local[*]")     // using master as local to test in my machine for now, this can be the url of YARN cluster in production
                  .appName("Eurowings_dataEngine")
                  .enableHiveSupport()            // enabling hive support to store the formatted dataframe into Hive tables.
                  .getOrCreate()

    import spark.implicits._
    import java.sql.Date
    //total 96 columns in the input file.
    val original_df= spark.read
      .option("wholeFile", true)
      .option("multiLine", "true")              // Since some of the reviews columns are having /n characters and in order spark should escape them, multiLine=true
      .option("delimiter",",")
      .option("header","true")
      .option("escape", "\"")
      .csv("hdfs://localhost:9000/eurowings/ETL/listings_2018-01-01.csv")  //Reading file from HDFS. The file name can be sent as an argument to main during scheduled runs using spark submit.

    original_df.printSchema()   //checking if spark read the file correctly with all headers

    original_df.take(1).foreach(print)

    original_df.select("id").show(100)  //checking if spark read the first column correctly

    original_df.select("price").show(100)  //checking if spark read the Price column correctly

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
    println("now back after schema")

    //Original_df formed above will be formatted for all the necessary important columns using withColumn option
    val formatted_df2=original_df
      //Identified price columns and Formatting them using udf.
      .withColumn("price_formatted", format_currency_data_udf1(when ($"price".isNull,0.00).otherwise($"price")))
      .withColumn("weekly_price_formatted",format_currency_data_udf1(when ($"weekly_price".isNull,0.00).otherwise($"weekly_price")))
      .withColumn("monthly_price_formatted",format_currency_data_udf1(when ($"monthly_price".isNull,0.00).otherwise($"monthly_price")))
      .withColumn("security_deposit_formatted",format_currency_data_udf1(when ($"security_deposit".isNull,0.00).otherwise($"security_deposit")))
      .withColumn("cleaning_fee_formatted",format_currency_data_udf1(when ($"cleaning_fee".isNull,0.00).otherwise($"cleaning_fee")))
      .withColumn("extra_people_formatted",format_currency_data_udf1(when ($"extra_people".isNull,0.00).otherwise($"extra_people")))
      .withColumn("bathrooms_formatted",format_currency_data_udf1(when ($"bathrooms".isNull,0.00).otherwise($"bathrooms")))
      .withColumn("reviews_per_month_formatted",format_currency_data_udf1(when ($"reviews_per_month".isNull,0.00).otherwise($"reviews_per_month")))

      //Identified Numeric columns and Formatting them using udf.
      .withColumn("id_formatted",format_numeric_data_udf1(when ($"id".isNull,0.toInt).otherwise($"id")))
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

      //convert latittude, longitude from string to decimal
      .withColumn("latitude_formatted",$"latitude".cast("Double"))
      .withColumn("longitude_formatted",$"longitude".cast("Double"))

      //Identified date columns and format them accordingly to unix_timestamp.
      .withColumn("last_scraped_date",to_date(unix_timestamp(col("last_scraped"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("host_since_date",to_date(unix_timestamp(col("host_since"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("calendar_last_scraped_date",to_date(unix_timestamp(col("calendar_last_scraped"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("first_review_date",to_date(unix_timestamp(col("first_review"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("last_review_date",to_date(unix_timestamp(col("last_review"),"MM/dd/yyyy").cast("timestamp")))

      //amenities, host_verifications are in bad format so bringing them to comma seperated list
      .withColumn("amenities_formatted",format_amenities_data_udf1(when($"amenities".isNull,"").otherwise($"amenities") ))
      .withColumn("host_verifications_formatted", format_host_verifications_data_udf1(when($"host_verifications".isNull,"").otherwise($"host_verifications")))
      .withColumn("jurisdiction_names_formatted", format_amenities_data_udf1(when ($"jurisdiction_names".isNull,"").otherwise($"jurisdiction_names")))

      // Identified some boolean columns which have either 't' or 'f'. formatting them to cast as boolean type.
      .withColumn("host_has_profile_pic_b",format_boolean_data_udf1(when ($"host_has_profile_pic".isNull,null).otherwise(value=$"host_has_profile_pic")))
      .withColumn("host_identity_verified_b",format_boolean_data_udf1(when ($"host_identity_verified".isNull,value=null).otherwise(value=$"host_identity_verified")))
      .withColumn("is_location_exact_b",format_boolean_data_udf1(when ($"is_location_exact".isNull,value=null).otherwise(value=$"is_location_exact")))
      .withColumn("has_availability_b",format_boolean_data_udf1(when ($"has_availability".isNull,value=null).otherwise(value=$"has_availability")))
      .withColumn("requires_license_b",format_boolean_data_udf1(when($"requires_license".isNull,value=null).otherwise(value=$"requires_license")))
      .withColumn("instant_bookable_b",format_boolean_data_udf1(when ($"instant_bookable".isNull,value=null).otherwise(value=$"instant_bookable")))
      .withColumn("is_business_travel_ready_b",format_boolean_data_udf1(when ($"is_business_travel_ready".isNull,value=null).otherwise(value=$"is_business_travel_ready")))
      .withColumn("require_guest_profile_picture_b",format_boolean_data_udf1(when ($"require_guest_profile_picture".isNull,value=null).otherwise(value=$"require_guest_profile_picture")))
      .withColumn("require_guest_phone_verification_b",format_boolean_data_udf1(when ($"require_guest_phone_verification".isNull,value=null).otherwise(value=$"require_guest_phone_verification")))


    println("now back after schema step2")

    formatted_df2.printSchema()

    // With all the newly formatted column names forming a List, so that only those will be used to form  a final dataframe.
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
        "host_verifications_formatted",
        "host_has_profile_pic_b",
        "host_identity_verified_b",
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
        "is_location_exact_b",
        "property_type",
        "room_type",
        "accommodates_formatted",
        "bathrooms_formatted",
        "bedrooms_formatted",
        "beds_formatted",
        "bed_type",
        "amenities_formatted",
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
        "has_availability_b",
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
        "requires_license_b",
        "license",
        "jurisdiction_names_formatted",
        "instant_bookable_b",
        "is_business_travel_ready_b",
        "cancellation_policy",
        "require_guest_profile_picture_b",
        "require_guest_phone_verification_b",
        "calculated_host_listings_count_formatted",
        "reviews_per_month_formatted"
    )
    //val final_df=formatted_df2.select(col("abc"))
    val final_df=formatted_df2.select(AllcolumnNameSeq.map(x=>col(x)):_* )

    final_df.cache() //caching this resultant dataframe as my next transformations/actions extensively use this.

    final_df.show(100)

    final_df.select("id_formatted").show(100)

  // creating a case class with primitive types  to convert above dataframe to dataset for better compile time safety.

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
                               host_verifications_formatted:String,
                               host_has_profile_pic_b:String,
                               host_identity_verified_b:String,
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
                               is_location_exact_b:String,
                               property_type:String,
                               room_type:String,
                               accommodates_formatted:Int,
                               bathrooms_formatted:Double,
                               bedrooms_formatted:Int,
                               beds_formatted:Int,
                               bed_type:String,
                               amenities_formatted:String,
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
                               has_availability_b:String,
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
                               requires_license_b:String,
                               license:String,
                               jurisdiction_names_formatted:String,
                               instant_bookable_b:String,
                               is_business_travel_ready_b:String,
                               cancellation_policy:String,
                               require_guest_profile_picture_b:String,
                               require_guest_phone_verification:String,
                               calculated_host_listings_count_formatted:Int,
                               reviews_per_month_formatted:Double)

    println("num of columns are :",final_df.columns.size) //check if the count matches with the actual dataframe size.

    val original_df_size=original_df.columns.size
    val final_df_size=final_df.columns.size

    if (original_df_size==final_df_size)
      println("--------->total columms count match after format")
    else
      println("--------->columms missing after format")



  final_df.printSchema()

    //val final_ds=final_df.as[airbnb_data]
    // val final_dataset =final_df.as[airbnb_data]

    //final_dataset.select("host_acceptance_rate_formatted").show(100)


    // Creating a staging table in Hive (airbnb_database) which is a truncate and load on everyday/file arriving pattern basis.

    spark.sql("DROP TABLE IF EXISTS airbnb_database.airbnb_staging_table")

    final_df.write.mode(saveMode="OverWrite").saveAsTable("airbnb_database.airbnb_staging_table")

    //as amenities is a string of comma seperated values, need to explode this with comma seperated
    val amenities_df=final_df.select($"id_formatted".as("host_id"), $"name".as("host_name"), explode(split($"amenities_formatted", ",")).as("individual_amenities"))

    //use a seperate hive table to load this exploded details so that it can be joined with base table and grouped by host_id to get the amenities count individually.
    spark.sql("DROP TABLE IF EXISTS airbnb_database.amenities_staging")

    println("-------------------------------->"+amenities_df.getClass)

    amenities_df.write.mode(saveMode = "OverWrite").saveAsTable("airbnb_database.airbnb_amenities_mapping_table")

    spark.sql("select * from airbnb_database.airbnb_amenities_mapping_table")

    spark.stop()
  }
}

