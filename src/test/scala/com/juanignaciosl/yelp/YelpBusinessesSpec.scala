package com.juanignaciosl.yelp

import com.juanignaciosl.utils.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest._

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
      }
    }
  }

  def BusinessTemplate = Business(
    Random.nextString(4),
    isOpen = true,
    "Z1PC0D3",
    "My City",
    "My State"
  )

  describe("filterOpen") {
    it("should remove closed businesses") {
      import ss.implicits._
      val openBusiness = BusinessTemplate.copy(isOpen = true)
      val closedBusiness = BusinessTemplate.copy(isOpen = false)
      val businesses = sc.parallelize(Seq(openBusiness, closedBusiness)).toDS()

      filterOpen(businesses).collect().map(_.id) shouldEqual Seq(openBusiness.id)
    }
  }
}
