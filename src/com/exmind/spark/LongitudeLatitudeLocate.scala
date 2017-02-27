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
  private val areaPolygonMap: mutable.HashMap[String, util.ArrayList[ElectronicFencePoint]]= _getAreaPolygonMap(hiveContext)
  private val polygonMaxMinMap: mutable.HashMap[String, Array[Float]] = _getPolygonMaxMinMap(hiveContext)


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

  // get nearest location
  def getNearestColUDF[T]: (String, String, T) => T = {
    (longitude: String, latitude: String, returnType: T) => {
      var minDistance: Float = Float.MaxValue
      var minKey = ""

      if ( _isDoubleNumber(longitude) && _isDoubleNumber(latitude) ) {
        val circleFence = new CircleFence()
        val circleCheckPoint = new CircleFencePoint(longitude, latitude)

        val loop = new Breaks
        loop.breakable {
          for ((key, polygon) <- areaPolygonMap) {
            // first use fuzzy matching improve matching efficiency
            val vagueCircle = _vagueCircle(polygonMaxMinMap(key))
            val vagueCheckResult = circleFence.isIn(circleCheckPoint, vagueCircle)

            if (vagueCheckResult) {
              minKey = key
              minDistance = 0
              loop.break
            } else {
              val distance = circleFence.getDistance(circleCheckPoint, vagueCircle.getCenter())
              if (distance < minDistance) {
                minDistance = distance
                minKey = key
              }
            }

          }
        }
      }

      (returnType match {
        case returnType: String => minKey
        case returnType: Int =>
          try {
            minKey.toInt
          } catch {
            case e: NumberFormatException => 0
          }
      }).asInstanceOf[T]

    }
  }

  // use longitude and latitude locate place
  def getBelongColUDF[T]: (String, String, T) => T = {

    (longitude: String, latitude: String, returnType: T) => {
      var returnValue = ""

      if ( _isDoubleNumber(longitude) && _isDoubleNumber(latitude) ) {
        val electronicFence = new ElectronicFence()
        val polygonCheckPoint = new ElectronicFencePoint(longitude, latitude)
        val circleFence = new CircleFence()
        val circleCheckPoint = new CircleFencePoint(longitude, latitude)

        val loop = new Breaks
        loop.breakable {
          for ((key, polygon) <- areaPolygonMap) {
            // first use fuzzy matching improve matching efficiency
            val vagueCircle = _vagueCircle(polygonMaxMinMap(key))
            val vagueCheckResult = circleFence.isIn(circleCheckPoint, vagueCircle)

            if (vagueCheckResult) {
              val checkResult = electronicFence.InPolygon(polygon, polygonCheckPoint)
              if (checkResult == 0) {
                returnValue = key
                loop.break
              }
            }
          }
        }
      }

      (returnType match {
        case returnType: String => returnValue
        case returnType: Int =>
          try {
            returnValue.toInt
          } catch {
            case e: NumberFormatException => 0
          }
      }).asInstanceOf[T]

    }
  }

}
