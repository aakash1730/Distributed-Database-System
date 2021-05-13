package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor(inputString.split(",")(0).replace("(","").toDouble/coordinateStep).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def squared(a:Int):Double=
  {
    return (a*a).toDouble;
  }

  def countOfNeighbouringPoints(minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, inputX: Int, inputY: Int, inputZ: Int): Int =
  {
    var countCells = 0;
    val condition0 = 7
    val condition1 = 11
    val condition2 = 17
    val condition3 = 26

    if (inputX == minX || inputX == maxX) {
      countCells += 1;
    }

    if (inputY == minY || inputY == maxY) {
      countCells += 1;
    }

    if (inputZ == minZ || inputZ == maxZ) {
      countCells += 1;
    }

    if (countCells == 1) {
      return condition2;
    }
    else if (countCells == 2)
    {
      return condition1;
    }
    else if (countCells == 3)
    {
      return condition0;
    }
    else
    {
      return condition3;
    }
  }

  def getStatisticOfData(x: Int, y: Int, z: Int, mean:Double, standardDeviation: Double, count0: Int, sum0: Int, numOfCells: Int): Double =
  {
    val nume = sum0.toDouble - (mean*count0.toDouble)
    val deno = standardDeviation * math.sqrt(numOfCells.toDouble * count0.toDouble) - (count0.toDouble*count0.toDouble) / (numOfCells.toDouble-1.0)
    return nume / deno
  }
}