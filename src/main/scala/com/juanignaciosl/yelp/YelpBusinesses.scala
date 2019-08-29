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

// INFO: a Map[String, String] might've been simpler, but Spark json read by default required
// a case class instead of a Map to parse the json object, so I kept it and leveraged the extra
// typing benefits, such as ensuring days.
case class WeekHours[T](monday: Option[T] = None,
                        tuesday: Option[T] = None,
                        wednesday: Option[T] = None,
                        thursday: Option[T] = None,
                        friday: Option[T] = None,
                        saturday: Option[T] = None,
                        sunday: Option[T] = None) {

  def isEmpty: Boolean =
    List(monday, tuesday, wednesday, thursday, friday, saturday, sunday).flatten.isEmpty
}

case class Business(id: BusinessId,
                    isOpen: Boolean,
                    postalCode: PostalCode,
                    city: City,
                    stateAbbr: StateAbbr,
                    hours: WeekHours[BusinessSchedule]) {

  // INFO: arguably, cleaning of the hours so they're right and sortable should
  // be done as soon as possible. We've deferred it for simplicity and performance reasons.
  lazy val openingHours: WeekHours[BusinessTime] = WeekHours(
    hours.monday.flatMap(openingHour),
    hours.tuesday.flatMap(openingHour),
    hours.wednesday.flatMap(openingHour),
    hours.thursday.flatMap(openingHour),
    hours.friday.flatMap(openingHour),
    hours.saturday.flatMap(openingHour),
    hours.sunday.flatMap(openingHour)
  )

  private def clean(time: BusinessTime): Option[BusinessTime] = {
    time.split(':') match {
      // INFO: this is a simplistic approach to ease flatmapping with other Option monads
      case Array(hh, mm) => Some(f"${hh.toInt}%02d:${mm.toInt}%02d")
      case _ => None
    }
  }

  private def openingHour(h: BusinessSchedule) = clean(h.split('-')(0))
}
