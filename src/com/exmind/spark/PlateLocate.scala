package com.exmind.spark

import java.util

import com.exmind.algorithm.domain.ElectronicFencePoint
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable

/**
  * Created by 38376 on 2017/2/22.
  */
class PlateLocate (hContext: HiveContext, databaseName: String) extends LongitudeLatitudeLocate(hContext, databaseName) {

  // build key -> ElectronicFencePoint map
  override def _getAreaPolygonMap(hiveContext: HiveContext): mutable.HashMap[String, util.ArrayList[ElectronicFencePoint]] = {
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

  // build key -> peak point array
  override def _getPolygonMaxMinMap(hiveContext: HiveContext): mutable.HashMap[String, Array[Float]] = {
    val polygonMaxMinMap = new mutable.HashMap[String, Array[Float]]()
    // get four peak point
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

}
