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
      val ds = readBusinesses(FileUtils.resourcePath("business.head.json"))
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

  describe("readReviews") {
    it("should parse review json files into Review case class") {
      import ss.implicits._
      val ds = readReviews(FileUtils.resourcePath("review.head.json")).as[Review]
      ds.count() shouldEqual 10
      val knownId = "8e9HxxLjjqc9ez5ezzN7iQ"
      ds.map(_.id).collect should contain(knownId)
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

  def CoolReviewTemplate = Review(
    Random.nextString(4),
    Random.nextString(4),
    cool = 1
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

  describe("countOpenByDay") {
    it("should count open businesses by day") {
      import ss.implicits._
      def buildBusiness(mondayOpeningTime: String) = {
        BusinessTemplate.copy(hours = WeekHours(monday = Some(mondayOpeningTime)))
      }

      val from09to20No = buildBusiness("9:00-20:00")
      val from09to21No = buildBusiness("9:00-21:00")
      val from09to22Yes = buildBusiness("9:00-22:00")
      val from22to23YNo = buildBusiness("22:00-23:00")
      val businesses = Seq(from09to20No, from09to21No, from09to22Yes, from22to23YNo)
      val groupedBusinesses = groupByStateAndCity(sc.parallelize(businesses).toDS)

      val openBusinessesCount = countOpenPastTime(groupedBusinesses, "21:00").collect().map(_._2)

      openBusinessesCount shouldEqual Array((1L, 0L, 0L, 0L, 0L, 0L, 0L))
    }
    it("should count open businesses by day for businesses spanning to the next day") {
      import ss.implicits._
      def buildBusiness(mondayOpeningTime: String) = {
        BusinessTemplate.copy(hours = WeekHours(monday = Some(mondayOpeningTime)))
      }

      val from09to20No = buildBusiness("9:00-20:00")
      val from09to21No = buildBusiness("9:00-21:00")
      val from09to01Yes = buildBusiness("9:00-1:00")
      val from22to23YNo = buildBusiness("22:00-23:00")
      val businesses = Seq(from09to20No, from09to21No, from09to01Yes, from22to23YNo)
      val groupedBusinesses = groupByStateAndCity(sc.parallelize(businesses).toDS)

      val openBusinessesCount = countOpenPastTime(groupedBusinesses, "21:00").collect().map(_._2)

      openBusinessesCount shouldEqual Array((1L, 0L, 0L, 0L, 0L, 0L, 0L))
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

  describe("getCoolestBusiness") {
    // 4 groups with businesses (3, 2, 1, 1)
    val b111a = BusinessTemplate.copy(id="111a", stateAbbr = "s1", city = "c1", postalCode = "pc1")
    val b111b = BusinessTemplate.copy(id="111b", stateAbbr = "s1", city = "c1", postalCode = "pc1")
    val b111c = BusinessTemplate.copy(id="111c", stateAbbr = "s1", city = "c1", postalCode = "pc1")

    val b112a = BusinessTemplate.copy(id="112a", stateAbbr = "s1", city = "c1", postalCode = "pc2")
    val b112b = BusinessTemplate.copy(id="112b", stateAbbr = "s1", city = "c1", postalCode = "pc2")

    val b113a = BusinessTemplate.copy(id="113a", stateAbbr = "s1", city = "c1", postalCode = "pc3")

    val b114a = BusinessTemplate.copy(id="114a", stateAbbr = "s1", city = "c1", postalCode = "pc4")

    // In groups 1 and 2 the 2nd will win, in 3 the first one, in 4, none
    val r1b111a = CoolReviewTemplate.copy(businessId = b111a.id)
    val r1b111b = CoolReviewTemplate.copy(businessId = b111b.id)
    val r2b111b = CoolReviewTemplate.copy(businessId = b111b.id)

    val r1b112b = CoolReviewTemplate.copy(businessId = b112b.id)

    val r1b113a = CoolReviewTemplate.copy(businessId = b113a.id)

    import ss.implicits._
    val businesses = sc.parallelize(Seq(
      b111a, b111b, b111c, b112a, b112b, b113a, b114a
    )).toDS

    val reviews = sc.parallelize(Seq(
      r1b111a, r1b111b, r2b111b, r1b112b, r1b113a
    )).toDS

    val result = getCoolestBusiness(businesses, reviews)
    val values = result.collect().map {
      case (_, (b: BusinessId, coolness: Long)) => (b, coolness)
    }.sortBy(_._1)

    val expected = Array((b111b.id, 2L), (b112b.id, 1L), (b113a.id, 1L)).sortBy(_._1)
    values shouldEqual expected
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

  describe("isOpen") {
    it("should return true if the schedule spans until the next day") {
      WeekHours.isOpen("20:00", "10:0-1:0") shouldEqual true
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
        WeekHours(monday = Some("09:00"), tuesday = Some("10:00")),
        WeekHours(monday = Some("09:00"), tuesday = Some("10:00"))
      )
      aggregator.finish(reduction) shouldEqual expected
    }
  }
}
