package com.exmind.spark

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by 38376 on 2017/2/22.
  */
object UpdateMacHomeConsumeCycle {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark for Hive")
    val spark = new SparkContext(conf)
    val hiveContext = new HiveContext(spark)

    hiveContext.sql("SET spark.sql.hive.convertMetastoreParquet=false")
    hiveContext.sql("SET spark.sql.parquet.mergeSchema=true")

    val cycleLocate= new ConsumeCycleLocate(hiveContext, "masa_td")

    val sqlFunc = udf(cycleLocate.getNearestColUDF[String])

    hiveContext.sql("use masa_td")
    // update home consume cycle
    val homeMac = hiveContext.sql(" from td_home_geospatial_new " +
                  " select mac, home_longitude, home_latitude, type, '' consume_cycle ")
    val homeUpdate = homeMac.withColumn("consume_cycle", sqlFunc(col("home_longitude"), col("home_latitude"), col("consume_cycle")))
    homeUpdate.write.mode(SaveMode.Overwrite).saveAsTable("td_home_geospatial_consume_cycle")

    // update work consume cycle
    val workMac = hiveContext.sql(" from td_work_geospatial_new " +
      " select mac, work_longitude, work_latitude, type, '' consume_cycle ")
    val workUpdate = workMac.withColumn("consume_cycle", sqlFunc(col("work_longitude"), col("work_latitude"), col("consume_cycle")))
    workUpdate.write.mode(SaveMode.Overwrite).saveAsTable("td_work_geospatial_consume_cycle")


    spark.stop()
  }
}
