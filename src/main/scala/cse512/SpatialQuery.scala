package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((st_contains(pointString, queryRectangle))))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def st_contains(pointString : String, queryRectangle : String): Boolean = {

    val rectPoints = queryRectangle.split(",")
    val rec_x1 = rectPoints(0).trim().toDouble
    val rec_y1 = rectPoints(1).trim().toDouble
    val rec_x2 = rectPoints(2).trim().toDouble
    val rec_y2 = rectPoints(3).trim().toDouble

    val testPoints = pointString.split(",")
    val pt_x = testPoints(0).trim().toDouble
    val pt_y = testPoints(1).trim().toDouble

    if(pt_x < rec_x1 || pt_x > rec_x2){
      return false
    }
    if(pt_y < rec_y1 || pt_y > rec_y2){
      return false
    }

    return true
  }

  def euclideanDistance(x1 : Double, y1 : Double, x2 : Double, y2 : Double) : Double = {

    val distance = scala.math.sqrt(scala.math.pow((x1-x2), 2) + scala.math.pow((y1-y2), 2))
    return distance
  }

  def st_within(pointString1 : String, pointString2 : String, distance : Double) : Boolean = {

    val pt1 = pointString1.split(",")
    val pt2 = pointString2.split(",")

    val x1 = pt1(0).trim().toDouble
    val y1 = pt1(1).trim().toDouble

    val x2 = pt2(0).trim().toDouble
    val y2 = pt2(1).trim().toDouble

    return (euclideanDistance(x1,y1,x2,y2) <= distance)

  }


  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((st_contains(pointString, queryRectangle))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((st_within(pointString1, pointString2, distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((st_within(pointString1, pointString2, distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
