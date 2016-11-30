package com.exmind.spark

import java.util

import com.exmind.algorithm.domain.{Circle, CircleFencePoint, ElectronicFencePoint}
import com.exmind.algorithm.function.{CircleFence, ElectronicFence}
import org.apache.spark._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext

import scala.util.control._
import java.util.Properties

import scala.collection.mutable

/**
  * Created by 38376 on 2016/11/30.
  */
object UpdateProjectPlate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark for Hive")
    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use data_center")

    val areaPolygonMap = PlateInfo.getAreaPolygonMap(hiveContext)
    val polygonMaxMinMap = PlateInfo.getPolygonMaxMinMap(hiveContext)

    val updatePlateFun= PlateInfo.getUpdatePlateIdUDF(areaPolygonMap, polygonMaxMinMap)
    val sqlFunc = udf(updatePlateFun)

    val sqlContext = new SQLContext(sc)
    val projectInfoDF = sqlContext.read.format("jdbc")
      .options(Map("url" -> "jdbc:mysql://172.18.84.66:3306/masa?user=masa&password=masa",
                   "dbtable" -> "(select * from bl_project_info_tmp limit 1000) as tbl",
                   "driver" -> "com.mysql.jdbc.Driver")).load()

    val updatedProject = projectInfoDF.withColumn("plate_id", sqlFunc(col("lng"), col("lat")))

    val prop =  new Properties()
    prop.setProperty("user","masa")
    prop.setProperty("password","masa")
    prop.setProperty("characterEncoding","utf-8")

    updatedProject.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://172.18.84.66:3306/masa", "bl_project_info_zp", prop);

    sc.stop()

    // 172.18.84.81 spark用户下执行
    // /usr/bin/spark-submit --jars $SPARK_HOME/lib/mysql-connector-java-5.1.30.jar
    // --master yarn-client --class com.exmind.spark.UpdateProjectPlate /home/spark/SparkProject.jar
  }
}
