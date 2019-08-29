package com.juanignaciosl.yelp

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, SparkSession}

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
    val groupedBusinesses = groupByStateCityAndPostalCode(filterOpen(readBusinesses(file)))
    import org.apache.spark.sql.functions.count
    val businessCounts = groupedBusinesses.agg(count("*"))
    businessCounts.explain()
  }
}

trait YelpBusinesses {
  val ss: SparkSession

  /**
   * Reads business with valid data (drops those without hours).
   *
   * @param path Path to a business.json file
   * @return Dataset with the valid businesses
   */
  def readBusinesses(path: String): Dataset[Business] = {
    import ss.implicits._

    ss.read
      .json(path)
      .withColumnRenamed("business_id", "id")
      .withColumnRenamed("is_open", "isOpen")
      .withColumnRenamed("postal_code", "postalCode")
      .withColumnRenamed("state", "stateAbbr")
      .na.drop(Seq("hours"))
      .as[Business]
  }

  def filterOpen(businesses: Dataset[Business]): Dataset[Business] = businesses.filter(_.isOpen)

  def groupByStateCityAndPostalCode(businesses: Dataset[Business]): KeyValueGroupedDataset[(StateAbbr, City, PostalCode), Business] = {
    {
      import ss.implicits._
      businesses.groupByKey(b => (b.stateAbbr, b.city, b.postalCode))
    }
  }
}

case class WeekHours(monday: Option[BusinessSchedule] = None,
                     tuesday: Option[BusinessSchedule] = None,
                     wednesday: Option[BusinessSchedule] = None,
                     thursday: Option[BusinessSchedule] = None,
                     friday: Option[BusinessSchedule] = None,
                     saturday: Option[BusinessSchedule] = None,
                     sunday: Option[BusinessSchedule] = None) {

  def isEmpty: Boolean =
    List(monday, tuesday, wednesday, thursday, friday, saturday, sunday).flatten.isEmpty
}

case class Business(id: BusinessId,
                    isOpen: Boolean,
                    postalCode: PostalCode,
                    city: City,
                    stateAbbr: StateAbbr,
                    hours: WeekHours)
