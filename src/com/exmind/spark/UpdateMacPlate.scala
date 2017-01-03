package com.exmind.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

import java.util.Properties
import java.sql.DriverManager
import java.sql.Connection

/**
  * Created by 38376 on 2016/12/19.
  */
object UpdateMacPlate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark for Hive")
    val spark = new SparkContext(conf)
    val hiveContext = new HiveContext(spark)
    hiveContext.sql("use data_center")
    hiveContext.sql("SET spark.sql.hive.convertMetastoreParquet=false")
    hiveContext.sql("SET spark.sql.parquet.mergeSchema=true")

    val areaPolygonMap = PlateInfo.getAreaPolygonMap(hiveContext)
    val polygonMaxMinMap = PlateInfo.getPolygonMaxMinMap(hiveContext)

    val updatePlateFun= PlateInfo.getUpdatePlateIdUDF(areaPolygonMap, polygonMaxMinMap)
    val sqlFunc = udf(updatePlateFun)

    // 查找mac对应的坐标
    val macInfo = hiveContext.sql(" from td_geo_info_final " +
      " select mac, work_longitude, work_latitude, type, 0 plate_id ")
    val updateInfo = macInfo.withColumn("plate_id", sqlFunc(col("work_longitude"), col("work_latitude")))


    val plateDistrict = hiveContext.sql(" from base_plate " +
                                        " select plate_id as p_id, district_id")

    val result = updateInfo
                    .join(plateDistrict, updateInfo("plate_id") === plateDistrict("p_id"), "left_outer")
                    .select("mac", "work_longitude", "work_latitude", "type", "plate_id", "district_id")

    // save to mysql
    if (args.length > 1 ){
      // args(0) = host, args(1) = db, args(2) = user, args(3) = password
      val jdbcURL = s"jdbc:mysql://${args(0)}:3306/${args(1)}"
      val prop =  new Properties()
      prop.setProperty("user",args(2))
      prop.setProperty("password",args(3))
      prop.setProperty("characterEncoding","utf-8")
      prop.setProperty("driver","com.mysql.jdbc.Driver")

      result.write.mode(SaveMode.Overwrite).jdbc(jdbcURL, "td_geo_info_spark", prop)
    } else {
      // save to hive
      result.write.mode(SaveMode.Overwrite).saveAsTable("td_geo_info_spark")
    }

    // 172.18.84.81 spark用户下执行
    // /usr/bin/spark-submit --jars /usr/hdp/current/spark-client/lib/mysql-connector-java-5.1.30.jar
    // --master yarn-client --class com.exmind.spark.UpdateMacPlate /home/spark/SparkProject.jar
    // host db username password

    spark.stop()
  }
}
