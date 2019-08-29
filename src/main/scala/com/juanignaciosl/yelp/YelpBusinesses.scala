package com.juanignaciosl.yelp

import java.io.{BufferedWriter, File, FileWriter}

import com.juanignaciosl.utils.MathUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Dataset, Encoder, Encoders, KeyValueGroupedDataset, SparkSession}

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
    val outputDir = if (args.length > 1) args(1) else "/tmp"
    val groupedBusinesses = groupByStateCityAndPostalCode(filterOpen(readBusinesses(file)))
    val ps = Seq(.5, .95)
    val openingHoursPercentiles = percentiles(groupedBusinesses, ps, _.openingHours)
    openingHoursPercentiles.explain()
    dump(openingHoursPercentiles, ps, s"$outputDir/opening")
  }

  /**
   *
   * @param percentilesByGroup Percentile computation
   * @param ps Percentiles corresponding to the generation
   * @param filePathPrefix CSV files will be created for each percentile
   */
  def dump(percentilesByGroup: Dataset[(BusinessGrouping, Seq[WeekHours[BusinessTime]])],
           ps: Seq[Double],
           filePathPrefix: String) = {
    val files = ps.map { p => new BufferedWriter(new FileWriter(new File(s"$filePathPrefix-$p.csv"))) }
    try {
      files.foreach(_.write("state,city,postal_code,monday,tuesday,wednesday,thursday,friday,saturday,sunday\n"))
      percentilesByGroup.collect().foreach { case (group, percentiles) =>
        percentiles.zipWithIndex.foreach { case (percentile, i) =>
          val values = WeekHours.unapply(percentile).get.productIterator.toList.map {
            case Some(v: String) => v
            case _ => ""
          }
          files(i).write(s"${group._1},${group._2},${group._3},${values.mkString(",")}\n")
        }
      }
    } finally {
      files.map(_.close())
    }
  }
}

class PercentileAggregator(f: Business => WeekHours[BusinessTime], ps: Seq[Double]) extends Aggregator[
  Business,
  Vector[WeekHours[BusinessTime]],
  Seq[WeekHours[BusinessTime]]
] with MathUtils {
  override def zero: Vector[WeekHours[BusinessTime]] = Vector()

  override def reduce(v: Vector[WeekHours[BusinessTime]], b: Business): Vector[WeekHours[BusinessTime]] = v :+ f(b)

  override def merge(b1: Vector[WeekHours[BusinessTime]], b2: Vector[WeekHours[BusinessTime]]): Vector[WeekHours[BusinessTime]] = b1 ++ b2

  private def flattenWeek(w: ((((((Option[BusinessTime], Option[BusinessTime]), Option[BusinessTime]), Option[BusinessTime]), Option[BusinessTime]), Option[BusinessTime]), Option[BusinessTime])) = {
    (w._1._1._1._1._1._1, w._1._1._1._1._1._2, w._1._1._1._1._2, w._1._1._1._2, w._1._1._2, w._1._2, w._2)
  }

  override def finish(reduction: Vector[WeekHours[BusinessTime]]): Seq[WeekHours[BusinessTime]] = {
    def prepareForPercentile(f: WeekHours[BusinessTime] => Option[BusinessTime]) = {
      reduction.map(f).flatten
    }

    def prepareForZip(times: Seq[BusinessTime], doubles: Seq[Double]) = {
      if (times.isEmpty) doubles.map(_ => None)
      else times.map(Some(_))
    }

    val monday = prepareForZip(percentile(prepareForPercentile(_.monday), ps), ps)
    val tuesday = prepareForZip(percentile(prepareForPercentile(_.tuesday), ps), ps)
    val wednesday = prepareForZip(percentile(prepareForPercentile(_.wednesday), ps), ps)
    val thursday = prepareForZip(percentile(prepareForPercentile(_.thursday), ps), ps)
    val friday = prepareForZip(percentile(prepareForPercentile(_.friday), ps), ps)
    val saturday = prepareForZip(percentile(prepareForPercentile(_.saturday), ps), ps)
    val sunday = prepareForZip(percentile(prepareForPercentile(_.sunday), ps), ps)

    val zippedWeek = monday zip tuesday zip wednesday zip thursday zip friday zip saturday zip sunday
    zippedWeek.map(flattenWeek).map(t => WeekHours(t._1, t._2, t._3, t._4, t._5, t._6, t._7))
  }

  override def bufferEncoder: Encoder[Vector[WeekHours[BusinessTime]]] = Encoders.kryo

  override def outputEncoder: Encoder[Seq[WeekHours[BusinessTime]]] = Encoders.kryo
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

  def groupByStateCityAndPostalCode(businesses: Dataset[Business]): KeyValueGroupedDataset[BusinessGrouping, Business] = {
    {
      import ss.implicits._
      businesses.groupByKey(b => (b.stateAbbr, b.city, b.postalCode))
    }
  }

  def percentiles(businesses: KeyValueGroupedDataset[BusinessGrouping, Business],
                  ps: Seq[Double],
                  f: Business => WeekHours[BusinessTime]): Dataset[(BusinessGrouping, Seq[WeekHours[BusinessTime]])] = {
    val aggregator = new PercentileAggregator(f, ps).toColumn
    implicit val weekHoursEncoder: Encoder[Seq[WeekHours[BusinessTime]]] = Encoders.kryo //Encoders.product[WeekHours[BusinessTime]]
    businesses.agg(aggregator.as[Seq[WeekHours[BusinessTime]]])
  }
}

// INFO: a Map[String, String] might've been simpler, but Spark json read by default required
// a case class instead of a Map to parse the json object, so I kept it and leveraged the extra
// typing benefits, such as ensuring days.
// PS: I've regretted this decision, especially because of the tedious Aggregator :_)
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
