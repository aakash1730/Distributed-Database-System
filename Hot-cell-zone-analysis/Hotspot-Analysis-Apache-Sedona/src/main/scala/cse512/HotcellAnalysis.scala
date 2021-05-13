package cse512

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.{ DataFrame, SparkSession }

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath)
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    //pickupInfo.show()
    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX", (pickupPoint: String) =>
      HotcellUtils.CalculateCoordinate(pickupPoint, 0))
    spark.udf.register("CalculateY", (pickupPoint: String) =>
      HotcellUtils.CalculateCoordinate(pickupPoint, 1))
    spark.udf.register("CalculateZ", (pickupTime: String) =>
      HotcellUtils.CalculateCoordinate(pickupTime, 2))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    val newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
    pickupInfo.createOrReplaceTempView("pickupinfo")

    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numOfCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

    val temp_point_dataframe = spark.sql("select x,y,z from pickupinfo where x>=" + minX + " and x<= " + maxX + " and y>= " + minY + " and y<= " + maxY + " and z>= " + minZ + " and z<= " + maxZ + " order by z,y,x").persist()
    temp_point_dataframe.createOrReplaceTempView("temp_dataframe_0")

    val temp_point_dataframe_1 = spark.sql("select x,y,z,count(*) as value_of_point from temp_dataframe_0 group by z,y,x order by z,y,x").persist()
    temp_point_dataframe_1.createOrReplaceTempView("temp_dataframe_2")

    spark.udf.register("squared", (inputX: Int) => HotcellUtils.squared(inputX))

    val sumOfAllPointsFromDataFrame = spark.sql("select count(*) as val_of_count, sum(value_of_point) as value_of_sum,sum(squared(value_of_point)) as sum_of_squared from temp_dataframe_2")
    sumOfAllPointsFromDataFrame.createOrReplaceTempView("sumOfAllPointsFromDataFrame")

    val sumOfPoints = sumOfAllPointsFromDataFrame.first().getLong(1)
    val sumOfPoints0 = sumOfAllPointsFromDataFrame.first().getDouble(2)

    val mean = sumOfPoints.toDouble / numOfCells.toDouble
    val standardDeviation = math.sqrt((sumOfPoints0.toDouble / numOfCells.toDouble) - (mean.toDouble * mean.toDouble))

    spark.udf.register("NeighbourCount", (minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, inputX: Int, inputY: Int, inputZ: Int)
    => HotcellUtils.countOfNeighbouringPoints(minX, minY, minZ, maxX, maxY, maxZ, inputX, inputY, inputZ))
    val Neighbours = spark.sql("select NeighbourCount(" + minX + "," + minY + "," + minZ + "," + maxX + "," + maxY + "," + maxZ + "," + "a1.x,a1.y,a1.z) as count, count(*) as count_of_all_value, a1.x as x,a1.y as y,a1.z as z, sum(a2.value_of_point) as total_sum from temp_dataframe_2 as a1, temp_dataframe_2 as a2 where (a2.x = a1.x+1 or a2.x = a1.x or a2.x = a1.x-1) and (a2.y = a1.y+1 or a2.y = a1.y or a2.y =a1.y-1) and (a2.z = a1.z+1 or a2.z = a1.z or a2.z =a1.z-1) group by a1.z,a1.y,a1.x order by a1.z,a1.y,a1.x").persist()
    Neighbours.createOrReplaceTempView("temp_dataframe_3")

    spark.udf.register("GScore", (x: Int, y: Int, z: Int, mean: Double, standardDeviation: Double, count0: Int, sum0: Int, numOfCells: Int) =>
      HotcellUtils.getStatisticOfData(x, y, z, mean,standardDeviation, count0, sum0, numOfCells))

    val Neighbours1 = spark.sql("select GScore(x,y,z," + mean + "," + standardDeviation + ", count, total_sum," + numOfCells + ") as gtstat,x, y, z from temp_dataframe_3 order by gtstat desc")
    Neighbours1.createOrReplaceTempView("temp_dataframe_4")

    val finalresult = spark.sql("select x,y,z from temp_dataframe_4")
    finalresult.createOrReplaceTempView("temp_dataframe_5")
    return finalresult
  }
}
