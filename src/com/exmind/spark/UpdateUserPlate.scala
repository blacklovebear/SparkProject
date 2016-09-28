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
  // 返回模糊区域的圆形
  def vagueCircle(polygonMax: Array[Float]): Circle = {
    val polygon = new util.ArrayList[ElectronicFencePoint]()
    val xMax = polygonMax(0)
    val xMin = polygonMax(1)
    val yMax = polygonMax(2)
    val yMin = polygonMax(3)

    val radius = Math.sqrt((xMax - xMin) * (xMax - xMin) + (yMax - yMin) * (yMax - yMin)) / 2.0
    val centerX = (xMin + xMax) / 2.0
    val centerY = (yMin + yMax) / 2.0
    val centerCircle = new CircleFencePoint(centerX.asInstanceOf[Float],  centerY.asInstanceOf[Float])

    val circle = new Circle(centerCircle, radius.asInstanceOf[Float])
    circle
  }

  // 构建 区域id，到区域坐标列表的 map
  def getAreaPolygonMap(hiveContext: HiveContext): mutable.HashMap[String, util.ArrayList[ElectronicFencePoint]] = {
    val areaPolygonMap = new mutable.HashMap[String, util.ArrayList[ElectronicFencePoint]]()

    hiveContext.sql("from fact_plate_geo " +
                    "select plate_id, coordinates_x, coordinates_y, sort " +
                    "order by plate_id, sort")
    .collect().foreach(row => {
      val plateId = row.get(0).toString
      val point = new ElectronicFencePoint(row.get(1).toString, row.get(2).toString)

      if( areaPolygonMap.contains(plateId) ) {
        areaPolygonMap(plateId).add(point)
      } else {
        val polygon = new util.ArrayList[ElectronicFencePoint]()
        polygon.add(point)
        areaPolygonMap(plateId) =  polygon
      }
    })

    areaPolygonMap
  }

  // 获取板块，坐标极值的 map
  def getPolygonMaxMinMap(hiveContext: HiveContext): mutable.HashMap[String, Array[Float]] = {
    val polygonMaxMinMap = new mutable.HashMap[String, Array[Float]]()
    // 获取区块的四个顶点
    hiveContext.sql("from fact_plate_geo "+
                    "select plate_id, max(coordinates_x) max_x, min(coordinates_x) min_x, " +
                    "max(coordinates_y) max_y,min(coordinates_y) min_y " +
                    "group by plate_id")
    .collect().foreach(row => {
      val plateId = row.get(0).toString
      polygonMaxMinMap(plateId) = Array(row.get(1).asInstanceOf[Float], row.get(2).asInstanceOf[Float],
                                        row.get(3).asInstanceOf[Float], row.get(4).asInstanceOf[Float])
    })

    polygonMaxMinMap
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark for Hive")
    val spark = new SparkContext(conf)

    val hiveContext = new HiveContext(spark)
    hiveContext.sql("use data_center")

    val areaPolygonMap = UpdateUserPlate.getAreaPolygonMap(hiveContext)
    val polygonMaxMinMap = UpdateUserPlate.getPolygonMaxMinMap(hiveContext)

    def updatePlateId(longitude: String, latitude:String): Int = {
      val electronicFence = new ElectronicFence()
      val polygonCheckPoint = new ElectronicFencePoint(longitude, latitude)
      val circleFence = new CircleFence()
      val circleCheckPoint = new CircleFencePoint(longitude, latitude)
      var finalPlateId = 0

      val loop = new Breaks
      loop.breakable {
        for((plateId, polygon) <- areaPolygonMap) {
          // 先模糊匹配，提高效率
          val vagueCircle = UpdateUserPlate.vagueCircle(polygonMaxMinMap(plateId))
          val vagueCheckResult = circleFence.isIn(circleCheckPoint, vagueCircle)

          if (vagueCheckResult) {
            val checkResult = electronicFence.InPolygon(polygon, polygonCheckPoint)
            if (checkResult == 0) {
              finalPlateId = plateId.toInt
              loop.break
            }
          }
        }
      }

      finalPlateId
    }

    val sqlFunc = udf(updatePlateId _)

    // 查找用户位置
    val userPosition = hiveContext.sql("from st_visitor_map_bak " +
                                       "select type, user_mac, longitude, latitude, " +
                                       "case_id, city, create_time, plate_id")
    val updatedPosition = userPosition.withColumn("plate_id", sqlFunc(col("longitude"), col("latitude")))

    hiveContext.sql("SET spark.sql.hive.convertMetastoreParquet=false")
    // "st_plate_visitor_map"
    updatedPosition.write.mode(SaveMode.Overwrite).saveAsTable("st_visitor_map_tmp")

    spark.stop()
  }
}
