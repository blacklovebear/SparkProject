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
import java.sql.DriverManager
import java.sql.Connection

import scala.collection.mutable

/**
  * Created by 38376 on 2016/11/30.
  */
object UpdateProjectPlate {
  // args(0) mysql数据库IP， args(1) 更新后的目标表
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark for Hive")
    val sc = new SparkContext(conf)

    val jdbcURL = "jdbc:mysql://"+ args(0) +":3306/masa"
    val username = "masa"
    val password = "masa"

    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use data_center")

    val areaPolygonMap = PlateInfo.getAreaPolygonMap(hiveContext)
    val polygonMaxMinMap = PlateInfo.getPolygonMaxMinMap(hiveContext)

    val updatePlateFun= PlateInfo.getUpdatePlateIdUDF(areaPolygonMap, polygonMaxMinMap)
    val sqlFunc = udf(updatePlateFun)

    val sqlContext = new SQLContext(sc)
    val projectInfoDF = sqlContext.read.format("jdbc")
      .options(Map("url" -> jdbcURL,
                   "dbtable" ->
                     """(select * from bl_project_info
                       | where open_date >= date('1970-01-01')
                       | and (plate_id is null OR plate_name is NULL)
                       |) as tbl""".stripMargin,
                   "driver" -> "com.mysql.jdbc.Driver",
                   "user" -> username,
                   "password" -> password)).load()

    val updatedProject = projectInfoDF.withColumn("plate_id", sqlFunc(col("lng"), col("lat")))

    val prop =  new Properties()
    prop.setProperty("user","masa")
    prop.setProperty("password","masa")
    prop.setProperty("characterEncoding","utf-8")

    updatedProject.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://"+ args(0) +":3306/masa", "bl_project_info_spark_tmp", prop)

    var connection:Connection = null
    try {
      connection = DriverManager.getConnection(jdbcURL, username, password)
      val statement = connection.createStatement()
      statement.execute(
        """
          | update bl_project_info a
          |  join (
          |        select a.id, a.plate_id, b.plate_name from bl_project_info_spark_tmp a
          |        join base_plate b
          |        on a.plate_id = b.plate_id
          |       ) b on a.id = b.id
          |    set a.plate_id = b.plate_id,
          |      a.plate_name = b.plate_name
          |where (a.plate_id is null) OR (a.plate_name is NULL )
        """.stripMargin)
    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      connection.close()
    }

    sc.stop()

    // 172.18.84.81 spark用户下执行
    // /usr/bin/spark-submit --jars $SPARK_HOME/lib/mysql-connector-java-5.1.30.jar
    // --master yarn-client --class com.exmind.spark.UpdateProjectPlate /home/spark/SparkProject.jar
  }
}
