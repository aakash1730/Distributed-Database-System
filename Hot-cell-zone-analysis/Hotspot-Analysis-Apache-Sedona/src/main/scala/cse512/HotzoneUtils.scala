package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {

    var rectangle = new Array[String](4)
    rectangle = queryRectangle.split(",")
    val rectangle_x1 = rectangle(0).trim.toDouble
    val rectangle_y1 = rectangle(1).trim.toDouble
    val rectangle_x2 = rectangle(2).trim.toDouble
    val rectangle_y2 = rectangle(3).trim.toDouble

    var point = new Array[String](2)
    point = pointString.split(",")
    val point_x = point(0).trim.toDouble
    val point_y = point(1).trim.toDouble


    var lower_x = 0.0
    var higher_x = 0.0

    if (rectangle_x1 < rectangle_x2) {
      lower_x = rectangle_x1
      higher_x = rectangle_x2
    }
    else {
      lower_x = rectangle_x2
      higher_x = rectangle_x1
    }

    val lower_y = math.min(rectangle_y1, rectangle_y2)
    val higher_y = math.max(rectangle_y1, rectangle_y2)

    if (point_y > higher_y || point_x < lower_x || point_x > higher_x || point_y < lower_y)
      false
    else
      true
  }

}
