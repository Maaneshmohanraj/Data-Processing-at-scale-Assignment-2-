package cse511

import org.apache.spark.sql.SparkSession

object SpatialQuery {

  // Parse a point string into (x, y) coordinates
  def parsePoint(pointString: String): (Double, Double) = {
    val coords = pointString.split(",")
    (coords(0).toDouble, coords(1).toDouble)
  }

  // Parse a rectangle string into (x1, y1, x2, y2) coordinates
  def parseRectangle(rectString: String): (Double, Double, Double, Double) = {
    val coords = rectString.split(",")
    (coords(0).toDouble, coords(1).toDouble, coords(2).toDouble, coords(3).toDouble)
  }

  // ST_Contains function: Returns true if the point is inside or on the boundary of the rectangle
  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    val (x1, y1, x2, y2) = parseRectangle(queryRectangle)
    val (px, py) = parsePoint(pointString)
    px >= Math.min(x1, x2) && px <= Math.max(x1, x2) && py >= Math.min(y1, y2) && py <= Math.max(y1, y2)
  }

  // ST_Within function: Returns true if the distance between two points is less than or equal to the given distance
  def ST_Within(pointString1: String, pointString2: String, distance: Double): Boolean = {
    val (x1, y1) = parsePoint(pointString1)
    val (x2, y2) = parsePoint(pointString2)
    val euclideanDistance = Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2))
    euclideanDistance <= distance
  }

  // Range Query: Find all points inside a given rectangle
  def runRangeQuery(spark: SparkSession, pointDataPath: String, queryRectangle: String): Long = {
    val pointDf = spark.read.format("csv").option("delimiter", "\t").option("header", "false").load(pointDataPath)
    pointDf.createOrReplaceTempView("points")
    spark.udf.register("ST_Contains", (rect: String, point: String) => ST_Contains(rect, point))
    val resultDf = spark.sql(s"SELECT * FROM points WHERE ST_Contains('$queryRectangle', points._c0)")
    resultDf.show()
    resultDf.count()
  }

  // Range Join Query: Find all (point, rectangle) pairs such that the point is within the rectangle
  def runRangeJoinQuery(spark: SparkSession, pointDataPath: String, rectangleDataPath: String): Long = {
    val pointDf = spark.read.format("csv").option("delimiter", "\t").option("header", "false").load(pointDataPath)
    val rectangleDf = spark.read.format("csv").option("delimiter", "\t").option("header", "false").load(rectangleDataPath)
    pointDf.createOrReplaceTempView("points")
    rectangleDf.createOrReplaceTempView("rectangles")
    spark.udf.register("ST_Contains", (rect: String, point: String) => ST_Contains(rect, point))
    val resultDf = spark.sql("SELECT * FROM points, rectangles WHERE ST_Contains(rectangles._c0, points._c0)")
    resultDf.show()
    resultDf.count()
  }

  // Distance Query: Find all points within a given distance from a fixed point
  def runDistanceQuery(spark: SparkSession, pointDataPath: String, queryPoint: String, distance: String): Long = {
    val pointDf = spark.read.format("csv").option("delimiter", "\t").option("header", "false").load(pointDataPath)
    pointDf.createOrReplaceTempView("points")
    spark.udf.register("ST_Within", (p1: String, p2: String, dist: Double) => ST_Within(p1, p2, dist))
    val resultDf = spark.sql(s"SELECT * FROM points WHERE ST_Within(points._c0, '$queryPoint', $distance)")
    resultDf.show()
    resultDf.count()
  }

  // Distance Join Query: Find all (p1, p2) pairs such that p1 is within a distance D from p2
  def runDistanceJoinQuery(spark: SparkSession, pointDataPath1: String, pointDataPath2: String, distance: String): Long = {
    val pointDf1 = spark.read.format("csv").option("delimiter", "\t").option("header", "false").load(pointDataPath1)
    val pointDf2 = spark.read.format("csv").option("delimiter", "\t").option("header", "false").load(pointDataPath2)
    pointDf1.createOrReplaceTempView("points1")
    pointDf2.createOrReplaceTempView("points2")
    spark.udf.register("ST_Within", (p1: String, p2: String, dist: Double) => ST_Within(p1, p2, dist))
    val resultDf = spark.sql(s"SELECT * FROM points1 p1, points2 p2 WHERE ST_Within(p1._c0, p2._c0, $distance)")
    resultDf.show()
    resultDf.count()
  }
}
