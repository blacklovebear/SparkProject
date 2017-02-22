package com.exmind.spark

import java._

import com.exmind.algorithm.domain.{Circle, CircleFencePoint, ElectronicFencePoint}
import com.exmind.algorithm.function.{CircleFence, ElectronicFence}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable
import scala.util.control.Breaks
import scala.util.control.Exception.allCatch

/**
  * Created by 38376 on 2017/2/22.
  */
abstract class LongitudeLatitudeLocate (val hContext: HiveContext, val databaseName: String) extends io.Serializable {
  val hiveContext: HiveContext = hContext

  hiveContext.sql( s"use $databaseName" )

  // keep map relation
  val areaPolygonMap: mutable.HashMap[String, util.ArrayList[ElectronicFencePoint]]= _getAreaPolygonMap(hiveContext)
  val polygonMaxMinMap: mutable.HashMap[String, Array[Float]] = _getPolygonMaxMinMap(hiveContext)


  // get fuzzy matching cycle
  def _vagueCircle(polygonMax: Array[Float]): Circle = {
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

  def _isDoubleNumber(s: String): Boolean = (allCatch opt s.toDouble).isDefined

  // overwrite by child
  def _getAreaPolygonMap(hiveContext: HiveContext): mutable.HashMap[String, util.ArrayList[ElectronicFencePoint]]

  // overwrite by child
  def _getPolygonMaxMinMap(hiveContext: HiveContext): mutable.HashMap[String, Array[Float]]

  // use longitude and latitude locate place and return Int
  def getUpdateIdUDF: (String, String) => Int = {
    (longitude: String, latitude: String) => {
      if ( _isDoubleNumber(longitude) && _isDoubleNumber(latitude) ) {
        val electronicFence = new ElectronicFence()
        val polygonCheckPoint = new ElectronicFencePoint(longitude, latitude)
        val circleFence = new CircleFence()
        val circleCheckPoint = new CircleFencePoint(longitude, latitude)
        var returnValue = 0

        val loop = new Breaks
        loop.breakable {
          for ((key, polygon) <- areaPolygonMap) {
            // first use fuzzy matching improve matching efficiency
            val vagueCircle = _vagueCircle(polygonMaxMinMap(key))
            val vagueCheckResult = circleFence.isIn(circleCheckPoint, vagueCircle)

            if (vagueCheckResult) {
              val checkResult = electronicFence.InPolygon(polygon, polygonCheckPoint)
              if (checkResult == 0) {
                returnValue = key.toInt
                loop.break
              }
            }
          }
        }

        returnValue
      }
      else
        0
    }
  }

  // use longitude and latitude locate place and return String
  def getUpdateNameUDF: (String, String) => String = {
    (longitude: String, latitude: String) => {
      if ( _isDoubleNumber(longitude) && _isDoubleNumber(latitude) ) {
        val electronicFence = new ElectronicFence()
        val polygonCheckPoint = new ElectronicFencePoint(longitude, latitude)
        val circleFence = new CircleFence()
        val circleCheckPoint = new CircleFencePoint(longitude, latitude)
        var returnValue = ""

        val loop = new Breaks
        loop.breakable {
          for ((plateId, polygon) <- areaPolygonMap) {
            // first use fuzzy matching improve matching efficiency
            val vagueCircle = _vagueCircle(polygonMaxMinMap(plateId))
            val vagueCheckResult = circleFence.isIn(circleCheckPoint, vagueCircle)

            if (vagueCheckResult) {
              val checkResult = electronicFence.InPolygon(polygon, polygonCheckPoint)
              if (checkResult == 0) {
                returnValue = plateId
                loop.break
              }
            }
          }
        }

        returnValue
      }
      else
        ""
    }
  }

}
