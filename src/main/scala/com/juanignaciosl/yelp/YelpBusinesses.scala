package com.juanignaciosl.yelp

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object YelpBusinessesRunner extends YelpBusinesses with App {
  lazy val conf: SparkConf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("Yelp Spark")
  lazy val ss: SparkSession =
    SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

  override def main(args: Array[String]): Unit = {
    val file = args(0)
    import ss.implicits._
    val businesses = readBusinesses(file)
      .groupByKey(b => (b.stateAbbr, b.city, b.postalCode))
    import org.apache.spark.sql.functions.count
    val businessCounts = businesses.agg(count("*"))
    businessCounts.collect().foreach(println)
  }
}

trait YelpBusinesses {
  val ss: SparkSession

  def readBusinesses(path: String): Dataset[Business] = {
    import ss.implicits._

    ss.read
      .json(path)
      .withColumnRenamed("business_id", "id")
      .withColumnRenamed("is_open", "isOpen")
      .withColumnRenamed("postal_code", "postalCode")
      .withColumnRenamed("state", "stateAbbr")
      .as[Business]
  }
}

case class Business(id: String,
                    isOpen: Boolean,
                    postalCode: String,
                    city: String,
                    stateAbbr: String)
