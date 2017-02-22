package com.exmind.spark

import java.util

import com.exmind.algorithm.domain.{Circle, CircleFencePoint, ElectronicFencePoint}
import com.exmind.algorithm.function.{CircleFence, ElectronicFence}
import org.apache.spark._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable
import scala.util.control._


object UpdateUserPlate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark for Hive")
    val spark = new SparkContext(conf)

    val hiveContext = new HiveContext(spark)

    val plateInfo = new PlateLocate(hiveContext, "data_center")
    val sqlFunc = udf(plateInfo.getUpdateIdUDF)

    hiveContext.sql("use data_center")

    val userPosition = hiveContext.sql("from st_visitor_map " +
                                       "select type, user_mac, longitude, latitude, " +
                                       "case_id, city, create_time, plate_id")
    val updatedPosition = userPosition.withColumn("plate_id", sqlFunc(col("longitude"), col("latitude")))

    hiveContext.sql("SET spark.sql.hive.convertMetastoreParquet=false")
    updatedPosition.write.mode(SaveMode.Overwrite).saveAsTable("st_plate_visitor_map")

    spark.stop()
  }
}
