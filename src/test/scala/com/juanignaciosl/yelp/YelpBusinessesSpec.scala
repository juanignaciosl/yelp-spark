package com.juanignaciosl.yelp

import com.juanignaciosl.utils.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest._

class YelpBusinessesSpec extends FunSpec with Matchers with YelpBusinesses {
  lazy val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("Yelp Spark test")
  lazy val ss: SparkSession =
    SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

  describe("readBusinesses") {
    it("should parse business json files into Business case class") {
      import ss.implicits._
      val ds = readBusinesses(FileUtils.resourcePath("business.head.json"))
      ds.count() shouldEqual 10
      val knownId = "gbQN7vr_caG_A1ugSmGhWg"
      ds.map(_.id).collect should contain(knownId)
      ds.collect().foreach { b =>
        b.id.length should be > 0
        b.postalCode.length should be > 0
        b.stateAbbr.length should be > 0
      }
    }
  }

}
