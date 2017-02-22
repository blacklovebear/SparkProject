package com.exmind.spark

import java.util

import com.exmind.algorithm.domain.ElectronicFencePoint
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable

/**
  * Created by 38376 on 2017/2/22.
  */
class ConsumeCycleLocate (hContext: HiveContext, databaseName: String) extends LongitudeLatitudeLocate(hContext, databaseName) {
  // build key -> ElectronicFencePoint map
  override def _getAreaPolygonMap(hiveContext: HiveContext): mutable.HashMap[String, util.ArrayList[ElectronicFencePoint]] = {
    val areaPolygonMap = new mutable.HashMap[String, util.ArrayList[ElectronicFencePoint]]()

    hiveContext.sql("from base_consume_cycle_coordinates " +
      "select name, lng, lat, sort_id " +
      "order by name, sort_id")
      .collect().foreach(row => {
      val name = row.get(0).toString
      val point = new ElectronicFencePoint(row.get(1).toString, row.get(2).toString)

      if( areaPolygonMap.contains(name) ) {
        areaPolygonMap(name).add(point)
      } else {
        val polygon = new util.ArrayList[ElectronicFencePoint]()
        polygon.add(point)
        areaPolygonMap(name) =  polygon
      }
    })

    areaPolygonMap
  }

  override def _getPolygonMaxMinMap(hiveContext: HiveContext): mutable.HashMap[String, Array[Float]] = {
    val polygonMaxMinMap = new mutable.HashMap[String, Array[Float]]()
    // get four peak point
    hiveContext.sql("from base_consume_cycle_coordinates "+
      "select name, max(cast(lng as float)) max_x, min(cast(lng as float)) min_x, " +
      "max(cast(lat as float)) max_y, min(cast(lat as float)) min_y " +
      "group by name")
      .collect().foreach(row => {
      val name = row.get(0).toString
      polygonMaxMinMap(name) = Array(row.get(1).asInstanceOf[Float], row.get(2).asInstanceOf[Float],
        row.get(3).asInstanceOf[Float], row.get(4).asInstanceOf[Float])
    })

    polygonMaxMinMap
  }
}
