package com.juanignaciosl.yelp

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest._
import com.juanignaciosl.utils.{FileUtils, MathUtils}

import scala.util.Random

class YelpBusinessesSpec extends FunSpec with Matchers with YelpBusinesses {

  lazy val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("Yelp Spark test")
  lazy val ss: SparkSession =
    SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
  lazy val sc = ss.sparkContext

  describe("readBusinesses") {
    it("should parse business json files into Business case class") {
      import ss.implicits._
      val ds = readBusinesses(FileUtils.resourcePath("business.head.json")).as[Business]
      ds.count() shouldEqual 8
      val knownId = "gbQN7vr_caG_A1ugSmGhWg"
      ds.map(_.id).collect should contain(knownId)
      ds.collect().foreach { b =>
        b.id.length should be > 0
        b.postalCode.length should be > 0
        b.stateAbbr.length should be > 0
        b.hours.isEmpty should be(false)
      }
    }
  }

  def BusinessTemplate = Business(
    Random.nextString(4),
    isOpen = true,
    "Z1PC0D3",
    "My City",
    "My State",
    WeekHours(monday = Some("9:0-0:0"))
  )

  describe("filterOpen") {
    it("should remove closed businesses") {
      import ss.implicits._
      val openBusiness = BusinessTemplate.copy(isOpen = true)
      val closedBusiness = BusinessTemplate.copy(isOpen = false)
      val businesses = sc.parallelize(Seq(openBusiness, closedBusiness)).toDS()

      val result = filterOpen(businesses).collect().map(_.id)
      result shouldEqual Array(openBusiness.id)
    }
  }

  describe("percentiles") {
    it("should compute .5 and .95 percentiles when there's only one matching business") {
      import ss.implicits._
      val businesses = Seq(
        BusinessTemplate.copy(hours = WeekHours(monday = Some("9:00-10:00"))),
        BusinessTemplate.copy(hours = WeekHours(tuesday = Some("10:00-11:00")))
      )
      val ds = groupByStateCityAndPostalCode(sc.parallelize(businesses).toDS)

      val expected = Seq(
        WeekHours(monday = Some("09:00"), tuesday = Some("10:00")),
        WeekHours(monday = Some("09:00"), tuesday = Some("10:00"))
      )
      val result = percentiles(ds, Seq(.5, .95), _.openingHours).collect()(0)._2
      result shouldEqual expected
    }
    it("should compute .5 and .95 percentiles") {
      import ss.implicits._
      val businesses = (8 to 11).map(h =>
        BusinessTemplate.copy(hours = WeekHours(
          monday = Some(s"$h:0-0:0"),
          tuesday = Some(s"${h + 1}:0-0:0")
        ))
      )
      val ds = groupByStateCityAndPostalCode(sc.parallelize(businesses).toDS)

      val expected = Seq(
        WeekHours(monday = Some("09:00"), tuesday = Some("10:00")),
        WeekHours(monday = Some("11:00"), tuesday = Some("12:00"))
      )
      val result = percentiles(ds, Seq(.5, .95), _.openingHours).collect()(0)._2
      result shouldEqual expected
    }
  }
}

class MathUtilsSpec extends FunSpec with Matchers with MathUtils {
  describe("percentile") {
    it("should return percentiles of strings") {
      percentile(Vector("08:00", "10:00", "09:00", "11:00"), Seq(.5, .95)) shouldEqual Seq("09:00", "11:00")
    }

    it("should return percentiles of ints") {
      percentile(Vector(15, 20, 35, 40, 50), Seq(.05, .30, .40, .50, 1)) shouldEqual Seq(15, 20, 20, 35, 50)
    }

    it("should return percentiles of ints for vectors with only one value") {
      percentile(Vector(15), Seq(.05, .30, .40, .50, 1)) shouldEqual Seq(15, 15, 15, 15, 15)
    }
  }
}

class WeekHoursSpec extends FunSpec with Matchers {
  describe("isEmpty") {
    it("should return true if no day has a value") {
      WeekHours().isEmpty should be(true)
    }
    it("should return false if any day has a value") {
      WeekHours(monday = Some("9:0-0:0")).isEmpty should be(false)
    }
  }
}

class BusinessSpec extends FunSpec with Matchers {
  describe("openingHours") {
    it("should return opening hours, adding leading zeroes if needed") {
      val business = Business(
        Random.nextString(4),
        isOpen = true,
        "Z1PC0D3",
        "My City",
        "My State",
        WeekHours(
          monday = Some("9:0-0:0"),
          tuesday = Some("10:0-11:0")
        )
      )

      val expected = WeekHours[BusinessTime](
        monday = Some("09:00"),
        tuesday = Some("10:00")
      )
      business.openingHours shouldEqual expected
    }
  }
}

class PercentileAggregatorSpec extends FunSpec with Matchers {
  describe("finish") {
    it("should compute percentile over the aggregated values") {
      val aggregator = new PercentileAggregator(_.openingHours, Seq(.5, .95))
      val reduction = Vector(
        WeekHours(monday = Some("09:00")),
        WeekHours(tuesday = Some("10:00")),
      )
      val expected = Seq(
        WeekHours(monday = Some("09:00"), tuesday = Some ("10:00")),
        WeekHours(monday = Some("09:00"), tuesday = Some ("10:00"))
      )
      aggregator.finish(reduction) shouldEqual (expected)
    }
  }
}
