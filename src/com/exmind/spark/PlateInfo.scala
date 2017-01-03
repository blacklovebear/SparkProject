package com.exmind.spark

import java.util

import com.exmind.algorithm.domain.{Circle, CircleFencePoint, ElectronicFencePoint}
import com.exmind.algorithm.function.{CircleFence, ElectronicFence}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable
import scala.util.control.Breaks
import scala.util.control.Exception.allCatch

/**
  * Created by 38376 on 2016/11/30.
  */
object PlateInfo {
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

    hiveContext.sql("from base_plate_coordinates " +
      "select plate_id, lng, lat, sort_id " +
      "order by plate_id, sort_id")
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
    hiveContext.sql("from base_plate_coordinates "+
      "select plate_id, max(lng) max_x, min(lng) min_x, " +
      "max(lat) max_y,min(lat) min_y " +
      "group by plate_id")
      .collect().foreach(row => {
      val plateId = row.get(0).toString
      polygonMaxMinMap(plateId) = Array(row.get(1).asInstanceOf[Float], row.get(2).asInstanceOf[Float],
        row.get(3).asInstanceOf[Float], row.get(4).asInstanceOf[Float])
    })

    polygonMaxMinMap
  }

  // 判断坐标是否为数字类型
  def isDoubleNumber(s: String): Boolean = (allCatch opt s.toDouble).isDefined

  // 依据提供的板块信息，返回根据经纬度获得的板块ID
  def getUpdatePlateIdUDF(areaPolygonMap: mutable.HashMap[String, util.ArrayList[ElectronicFencePoint]],
                          polygonMaxMinMap: mutable.HashMap[String, Array[Float]] ): (String, String) => Int = {
    // 返回udf 使用的函数
    (longitude: String, latitude: String) => {
      if ( PlateInfo.isDoubleNumber(longitude) && PlateInfo.isDoubleNumber(latitude) ) {
        val electronicFence = new ElectronicFence()
        val polygonCheckPoint = new ElectronicFencePoint(longitude, latitude)
        val circleFence = new CircleFence()
        val circleCheckPoint = new CircleFencePoint(longitude, latitude)
        var finalPlateId = 0

        val loop = new Breaks
        loop.breakable {
          for ((plateId, polygon) <- areaPolygonMap) {
            // 先模糊匹配，提高效率
            val vagueCircle = PlateInfo.vagueCircle(polygonMaxMinMap(plateId))
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
      else
        0
    }
  }

}
