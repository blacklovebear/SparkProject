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
    // args = [host db username password]
    val jdbcURL = s"jdbc:mysql://${args(0)}:3306/${args(1)}"
    val username = args(2)
    val password = args(3)

    val hiveContext = new HiveContext(sc)

    val plateInfo = new PlateLocate(hiveContext, "data_center")
    val sqlFunc = udf(plateInfo.getBelongColUDF[Int])

    val sqlContext = new SQLContext(sc)
    val projectInfoDF = sqlContext.read.format("jdbc")
      .options(Map("url" -> jdbcURL,
                   "dbtable" -> """(select id, plate_id, lng, lat from bl_project_info) as tbl""",
                   "driver" -> "com.mysql.jdbc.Driver",
                   "user" -> username,
                   "password" -> password)).load()

    val updatedProject = projectInfoDF.withColumn("plate_id", sqlFunc(col("lng"), col("lat"), col("plate_id")))

    val prop =  new Properties()
    prop.setProperty("user",username)
    prop.setProperty("password",password)
    prop.setProperty("characterEncoding","utf-8")


    var connection:Connection = null
    try {
      connection = DriverManager.getConnection(jdbcURL, username, password)
      val statement = connection.createStatement()
      // 创建临时表
      statement.execute("create table if NOT EXISTS bl_project_info_spark_tmp as select id, plate_id, lng, lat from bl_project_info where 1 > 2 ")
      // 将数据写入临时表
      updatedProject.write.mode(SaveMode.Overwrite).jdbc(jdbcURL, "bl_project_info_spark_tmp", prop)
      // 用临时表的数据更新，结果表
      statement.execute(
        """
          |  update bl_project_info a
          |  join (
          |        select a.id, a.plate_id, b.plate_name, b.district_id, c.district_name
          |        from bl_project_info_spark_tmp a
          |        join base_plate b
          |        on a.plate_id = b.plate_id
          |        join base_district c
          |        on b.district_id = c.district_id
          |       ) b on a.id = b.id
          |    set a.plate_id = b.plate_id,
          |      a.plate_name = b.plate_name,
          |      a.district_id = b.district_id,
          |      a.district_name = b.district_name
          |where (a.plate_id is null) OR (a.plate_name is NULL )
        """.stripMargin)
      // 利用已经更新的bl_project_info表更新 其他的表
      val updateTables = List("bl_deal_info", "bl_marketable_info", "bl_supply_info")
      for(table <- updateTables){
        statement.execute(
          """
            |update %s a
            |   join bl_project_info b
            |   on a.source_project_id = b.source_project_id
            |     set a.plate_id = b.plate_id,
            |      a.plate_name = b.plate_name,
            |      a.district_id = b.district_id,
            |      a.district_name = b.district_name
            | where (a.plate_id is null) OR (a.plate_name is NULL ) or (a.district_id is null) or (a.district_name is NULL );
          """.stripMargin.format(table) )
      }

    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      connection.close()
    }

    sc.stop()

    // 172.18.84.81 spark用户下执行
    // /usr/bin/spark-submit --jars /usr/hdp/current/spark-client/lib/mysql-connector-java-5.1.30.jar
    // --master yarn-client --class com.exmind.spark.UpdateProjectPlate /home/spark/SparkProject.jar
    // host db username password

  }
}
