package com.juanignaciosl.yelp

import java.io.{BufferedWriter, File, FileWriter, Writer}

import com.juanignaciosl.utils.MathUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Dataset, Encoder, Encoders, KeyValueGroupedDataset, SparkSession}

object YelpBusinessesLocalRunner extends App with YelpDataProcessor {
  override def main(args: Array[String]): Unit = super.main(args)

  lazy val conf: SparkConf = new SparkConf().setMaster("local[4]")
}

object YelpBusinessesRunner extends App with YelpDataProcessor {
  override def main(args: Array[String]): Unit = super.main(args)

  lazy val conf: SparkConf = new SparkConf()
}

trait YelpDataProcessor extends YelpBusinesses {
  val conf: SparkConf

  lazy val ss: SparkSession =
    SparkSession
      .builder()
      .config(conf.setAppName("Yelp Spark"))
      .getOrCreate()

  def main(args: Array[String]): Unit = {
    val inputDir = args(0)
    val outputDir = if (args.length > 1) args(1) else "/tmp"

    val openBusinesses = filterOpen(readBusinesses(s"$inputDir/business.json")).persist()
    val groupedBusinesses = groupByStateCityAndPostalCode(openBusinesses)
    val ps = Seq(.5, .95)
    dump(percentiles(groupedBusinesses, ps, _.openingHours), ps, s"$outputDir/opening")
    dump(percentiles(groupedBusinesses, ps, _.closingHours), ps, s"$outputDir/closing")

    dump(countOpenPastTime(groupByStateAndCity(openBusinesses), "21:00"), s"$outputDir/openpast-2100.csv")

    val coolestBusinessesByPostalCode = getCoolestBusiness(
      openBusinesses.filter(_.hours.sunday.isEmpty),
      readReviews(s"$inputDir/review.json"))
    dumpCount(coolestBusinessesByPostalCode, s"$outputDir/coolestBusinessNotOpenOnSunday.csv")
  }

  /**
   *
   * @param percentilesByGroup Percentile computation
   * @param ps                 Percentiles corresponding to the generation
   * @param filePathPrefix     CSV files will be created for each percentile
   */
  def dump(percentilesByGroup: Dataset[(BusinessGrouping, Seq[WeekHours[BusinessTime]])],
           ps: Seq[Double],
           filePathPrefix: String): Unit = {
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

  def dump(informationByStateAndCity: Dataset[(BusinessCityGrouping, SevenLongs)],
           filePath: String): Unit = {
    copyToWriter(filePath, { file =>
      file.write("state,city,monday,tuesday,wednesday,thursday,friday,saturday,sunday\n")
      informationByStateAndCity.collect().foreach {
        case ((state, city), (m, t, w, th, f, sa, su)) => file.write(s"$state,$city,$m,$t,$w,$th,$f,$sa,$su\n")
      }
    })
  }

  def dumpCount(businessWithCount: Dataset[(BusinessGrouping, (BusinessId, CoolnessCount))], filePath: String): Unit = {
    copyToWriter(filePath, { file =>
      file.write("state,city,monday,business_id,cool_count\n")
      businessWithCount.collect().foreach {
        case ((state, city, postalCode), (businessId, count)) => file.write(s"$state,$city,$postalCode,$businessId,$count\n")
      }
    })
  }

  private def copyToWriter(filePath: String, f: Writer => Unit): Unit = {
    val file = new BufferedWriter(new FileWriter(new File(filePath)))
    try {
      f(file)
    } finally {
      file.close()
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

class CountOpenPastTimeAggregator(time: BusinessTime) extends Aggregator[Business, SevenLongs, SevenLongs] {
  override def zero: SevenLongs = (0L, 0L, 0L, 0L, 0L, 0L, 0L)

  override def reduce(c1: SevenLongs, b: Business): SevenLongs = {
    val c2 = b.hours.days.map(d => d.map(WeekHours.isOpen(time, _))).map(open => if (open.getOrElse(false)) 1L else 0L) match {
      case m :: t :: w :: th :: f :: s :: su :: Nil => (m, t, w, th, f, s, su)
      case _ => throw new Exception(s"Error processing open day count for business ${b.id}")
    }
    merge(c1, c2)
  }

  override def merge(c1: SevenLongs, c2: SevenLongs): SevenLongs =
    (c1._1 + c2._1, c1._2 + c2._2, c1._3 + c2._3, c1._4 + c2._4, c1._5 + c2._5, c1._6 + c2._6, c1._7 + c2._7)

  override def finish(reduction: SevenLongs): SevenLongs = reduction

  override def bufferEncoder: Encoder[SevenLongs] = Encoders.kryo

  override def outputEncoder: Encoder[SevenLongs] = Encoders.kryo
}

class CoolestBusinessAggregator() extends Aggregator[(BusinessId, CoolnessCount), (BusinessId, CoolnessCount), (BusinessId, CoolnessCount)] {
  override def zero: (BusinessId, CoolnessCount) = ("", 0)

  override def reduce(b: (BusinessId, CoolnessCount), a: (BusinessId, CoolnessCount)): (BusinessId, CoolnessCount) = {
    if (a._1 == b._1) (a._1, a._2 + b._2)
    else if (b._2 > a._2) b else a
  }

  override def merge(b1: (BusinessId, CoolnessCount), b2: (BusinessId, CoolnessCount)): (BusinessId, CoolnessCount) = reduce(b1, b2)

  override def finish(reduction: (BusinessId, CoolnessCount)): (BusinessId, CoolnessCount) = reduction

  override def bufferEncoder: Encoder[(BusinessId, CoolnessCount)] = Encoders.tuple(Encoders.STRING, Encoders.scalaLong)

  override def outputEncoder: Encoder[(BusinessId, CoolnessCount)] = Encoders.tuple(Encoders.STRING, Encoders.scalaLong)
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

  def readReviews(path: String): Dataset[Review] = {
    import ss.implicits._
    ss.read
      .json(path)
      .withColumnRenamed("review_id", "id")
      .withColumnRenamed("business_id", "businessId")
      .as[Review]
  }

  def filterOpen(businesses: Dataset[Business]): Dataset[Business] = businesses.filter(_.isOpen)

  def groupByStateCityAndPostalCode(businesses: Dataset[Business]): KeyValueGroupedDataset[BusinessGrouping, Business] = {
    {
      import ss.implicits._
      businesses.groupByKey(b => (b.stateAbbr, b.city, b.postalCode))
    }
  }

  def groupByStateAndCity(businesses: Dataset[Business]): KeyValueGroupedDataset[BusinessCityGrouping, Business] = {
    {
      import ss.implicits._
      businesses.groupByKey(b => (b.stateAbbr, b.city))
    }
  }

  def percentiles(businesses: KeyValueGroupedDataset[BusinessGrouping, Business],
                  ps: Seq[Double],
                  f: Business => WeekHours[BusinessTime]): Dataset[(BusinessGrouping, Seq[WeekHours[BusinessTime]])] = {
    val aggregator = new PercentileAggregator(f, ps).toColumn
    implicit val weekHoursEncoder: Encoder[Seq[WeekHours[BusinessTime]]] = Encoders.kryo
    businesses.agg(aggregator.as[Seq[WeekHours[BusinessTime]]])
  }

  def countOpenPastTime[K](businesses: KeyValueGroupedDataset[K, Business], time: BusinessTime): Dataset[(K, SevenLongs)] = {
    val aggregator = new CountOpenPastTimeAggregator(time).toColumn
    implicit val sevenLongsEncoder: Encoder[SevenLongs] = Encoders.kryo
    businesses.agg(aggregator.as[SevenLongs])
  }

  def getCoolestBusiness(businesses: Dataset[Business], reviews: Dataset[Review]): Dataset[(BusinessGrouping, (BusinessId, CoolnessCount))] = {
    import ss.implicits._
    import org.apache.spark.sql.functions.sum
    val businessesIdsCoolness = reviews
      .filter(_.cool > 0)
      .groupByKey(_.businessId)
      .agg(sum("cool").as[CoolnessCount])

    val businessesWithCoolness = businesses
      .joinWith(businessesIdsCoolness, businesses.col("id") === businessesIdsCoolness.col("value"))
      .map {
        case (b, bWithCoolness) => (b, bWithCoolness._2)
      }
    val groupedIds = businessesWithCoolness
      .groupByKey(x => (x._1.stateAbbr, x._1.city, x._1.postalCode))
      .mapValues(bc => (bc._1.id, bc._2))

    groupedIds.agg(new CoolestBusinessAggregator().toColumn.as[(BusinessId, CoolnessCount)])
  }
}

// INFO: a Map[String, String] might've been simpler, but Spark json read by default required
// a case class instead of a Map to parse the json object, so I kept it and leveraged the extra
// typing benefits, such as ensuring days.
// PS: I've regretted this decision, especially because of the tedious Aggregator :_)
case class WeekHours[T <: String](monday: Option[T] = None,
                                  tuesday: Option[T] = None,
                                  wednesday: Option[T] = None,
                                  thursday: Option[T] = None,
                                  friday: Option[T] = None,
                                  saturday: Option[T] = None,
                                  sunday: Option[T] = None) {
  val days = List(monday, tuesday, wednesday, thursday, friday, saturday, sunday)

  val isEmpty: Boolean = days.flatten.isEmpty
}

object WeekHours {
  def isOpen(time: BusinessTime, day: BusinessSchedule): Boolean = {
    import Business.toHHMM
    toHHMM(time).exists(cleanedTime =>
      day.split('-') match {
        case Array(open, close) => (toHHMM(open), toHHMM(close)) match {
          // If closing time is before open time it must be because it spans to the next day
          case (Some(o), Some(c)) => o <= cleanedTime && (c > cleanedTime || c < o)
          case _ => false
        }
        case _ => false
      })
  }

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

  lazy val closingHours: WeekHours[BusinessTime] = WeekHours(
    hours.monday.flatMap(closingHour),
    hours.tuesday.flatMap(closingHour),
    hours.wednesday.flatMap(closingHour),
    hours.thursday.flatMap(closingHour),
    hours.friday.flatMap(closingHour),
    hours.saturday.flatMap(closingHour),
    hours.sunday.flatMap(closingHour)
  )

  import Business.toHHMM

  private def openingHour(h: BusinessSchedule) = toHHMM(h.split('-')(0))

  private def closingHour(h: BusinessSchedule) = toHHMM(h.split('-')(1))
}

object Business {

  def toHHMM(time: BusinessTime): Option[BusinessTime] = {
    time.split(':') match {
      // INFO: this is a simplistic approach to ease flatmapping with other Option monads
      case Array(hh, mm) => Some(f"${hh.toInt}%02d:${mm.toInt}%02d")
      case _ => None
    }
  }

}

case class Review(id: ReviewId, businessId: BusinessId, cool: CoolnessCount)
