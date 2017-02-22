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
    val sqlFunc = udf(cycleLocate.getUpdateNameUDF)

    hiveContext.sql("use masa_td")

    val macInfo = hiveContext.sql(" from td_home_geospatial_new " +
                  " select mac, home_longitude, home_latitude, type, '' consume_cycle ")

    val updateInfo = macInfo.withColumn("consume_cycle", sqlFunc(col("home_longitude"), col("home_latitude")))

    updateInfo.write.mode(SaveMode.Overwrite).saveAsTable("td_home_geospatial_test")

    spark.stop()
  }
}
