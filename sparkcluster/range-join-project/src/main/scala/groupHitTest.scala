/////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Signals for a given observation group are identified as anomalous 
// if they match with signals in all on ON observations but in none of the OFF observations
// and if they exceed thresholds in SNR and drift rate.
/////////////////////////////////////////////////////////////////////////////////////////////////////////////



/////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Imports
/////////////////////////////////////////////////////////////////////////////////////////////////////////////

import scala.util.Random
import java.util.Calendar
import org.apache.spark.sql._
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.{ Try, Success, Failure }
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf

//to run:
//$SPARK_HOME/bin/spark-shell --conf spark.cassandra.connection.host=[insert cassandra host]
// --packages datastax:spark-cassandra-connector:2.0.1-s_2.11 -i ~/range-join-project/src/main/scala/groupHitTest.scala



/////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Range join and anti-join functions
/////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Joins records based on if values in a column are found within a range of any other values in a second column
def range_join_dfs[U,V](df1:DataFrame, rangeField1:Column, df2:DataFrame, rangeField2:Column, rangeBack:Any):Try[DataFrame] = {
    // check that both fields are from the same (and the correct) type
    (df1.schema(rangeField1.toString).dataType, df2.schema(rangeField2.toString).dataType, rangeBack) match {
        case (x1: TimestampType, x2: TimestampType, rb:String) => true
        case (x1: NumericType, x2: NumericType, rb:Number) => true
        case _ => return Failure(new IllegalArgumentException("rangeField1 and rangeField2 must both be either numeric or timestamps. If they are timestamps, rangeBack must be a string, if numerics, rangeBack must be numeric"))
    }

    // returns the "window grouping" function for timestamp/numeric.
    // Timestamps will return the start of the grouping window
    // Numeric will do integers division
    def getWindowStartFunction(df:DataFrame, field:Column) = {
        df.schema(field.toString).dataType match {
            case d: TimestampType => window(field, rangeBack.asInstanceOf[String])("start")
            case d: NumericType => floor(field / lit(rangeBack))
            case _ => throw new IllegalArgumentException("field must be either of NumericType or TimestampType")
        }
    }

    // returns the difference between windows and a numeric representation of "rangeBack"
    // if rangeBack is numeric - the window diff is 1 and the numeric representation is rangeBack itself
    // if it's timestamp - the CalendarInterval can be used for both jumping between windows and filtering at the end
    def getPrevWindowDiffAndRangeBackNumeric(rangeBack:Any) = rangeBack match {
        case rb:Number => (1, rangeBack)
        case rb:String => {
            val interval = rb match {
                case rb if rb.startsWith("interval") => org.apache.spark.unsafe.types.CalendarInterval.fromString(rb)
                case _ => org.apache.spark.unsafe.types.CalendarInterval.fromString("interval " + rb)
            }
            //( interval.months * (60*60*24*31) ) + ( interval.microseconds / 1000000 )
            (interval, interval)
        }
        case _ => throw new IllegalArgumentException("rangeBack must be either of NumericType or TimestampType")
    }


    // get windowstart functions for rangeField1 and rangeField2
    val rf1WindowStart = getWindowStartFunction(df1, rangeField1)
    val rf2WindowStart = getWindowStartFunction(df2, rangeField2)
    val (prevWindowDiff, rangeBackNumeric) = getPrevWindowDiffAndRangeBackNumeric(rangeBack)


    // actual joining logic starts here
    val windowedDf1 = df1.withColumn("windowStart", rf1WindowStart)
    val windowedDf2 = df2.withColumn("windowStart", rf2WindowStart)
        .union( df2.withColumn("windowStart", rf2WindowStart + lit(prevWindowDiff)) )
        .union( df2.withColumn("windowStart", rf2WindowStart - lit(prevWindowDiff)) )

    val res = windowedDf1.join(windowedDf2, "windowStart")
          .filter( (rangeField2 > rangeField1-lit(rangeBackNumeric)) && (rangeField2 <= rangeField1 + lit(rangeBackNumeric)) )
          .drop(windowedDf1("windowStart"))
          .drop(windowedDf2("windowStart"))
          .drop(windowedDf2("observationgroup"))
          .drop(windowedDf2("observationorder"))
          .drop(windowedDf2("frequency"))
          .drop(windowedDf2("snr"))
          .drop(windowedDf2("driftrate"))
          .drop(windowedDf2("uncorrectedfrequency"))
          .distinct()

    Success(res)
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Anti-joins records based on if values in a column are found within a range of any of the values in a second column
def range_antijoin_dfs[U,V](df1:DataFrame, rangeField1:Column, df2:DataFrame, rangeField2:Column, rangeBack:Any):Try[DataFrame] = {
    // check that both fields are from the same (and the correct) type
    (df1.schema(rangeField1.toString).dataType, df2.schema(rangeField2.toString).dataType, rangeBack) match {
        case (x1: TimestampType, x2: TimestampType, rb:String) => true
        case (x1: NumericType, x2: NumericType, rb:Number) => true
        case _ => return Failure(new IllegalArgumentException("rangeField1 and rangeField2 must both be either numeric or timestamps. If they are timestamps, rangeBack must be numeric"))
    }

    // returns the "window grouping" function for timestamp/numeric.
    // Timestamps will return the start of the grouping window
    // Numeric will do integers division
    def getWindowStartFunction(df:DataFrame, field:Column) = {
        df.schema(field.toString).dataType match {
            case d: TimestampType => window(field, rangeBack.asInstanceOf[String])("start")
            case d: NumericType => floor(field / lit(rangeBack))
            case _ => throw new IllegalArgumentException("field must be either of NumericType or TimestampType")
        }
    }

    // returns the difference between windows and a numeric representation of "rangeBack"
    // if rangeBack is numeric - the window diff is 1 and the numeric representation is rangeBack itself
    // if it's timestamp - the CalendarInterval can be used for both jumping between windows and filtering at the end
    def getPrevWindowDiffAndRangeBackNumeric(rangeBack:Any) = rangeBack match {
        case rb:Number => (1, rangeBack)
        case rb:String => {
            val interval = rb match {
                case rb if rb.startsWith("interval") => org.apache.spark.unsafe.types.CalendarInterval.fromString(rb)
                case _ => org.apache.spark.unsafe.types.CalendarInterval.fromString("interval " + rb)
            }
            //( interval.months * (60*60*24*31) ) + ( interval.microseconds / 1000000 )
            (interval, interval)
        }
        case _ => throw new IllegalArgumentException("rangeBack must be either of NumericType or TimestampType")
    }


    // get windowstart functions for rangeField1 and rangeField2
    val rf1WindowStart = getWindowStartFunction(df1, rangeField1)
    val rf2WindowStart = getWindowStartFunction(df2, rangeField2)
    val (prevWindowDiff, rangeBackNumeric) = getPrevWindowDiffAndRangeBackNumeric(rangeBack)


    // actual joining logic starts here
    val windowedDf1 = df1.withColumn("windowStart", rf1WindowStart)
    val windowedDf2 = df2.withColumn("windowStart", rf2WindowStart)
        .union( df2.withColumn("windowStart", rf2WindowStart + lit(prevWindowDiff)) )
        .union( df2.withColumn("windowStart", rf2WindowStart - lit(prevWindowDiff)) )

    // The logic for this anti-join could be optimized further beyond using a range-join and except
    val resjoin = windowedDf1.join(windowedDf2, "windowStart")
          .filter( (rangeField2 > rangeField1-lit(rangeBackNumeric)) && (rangeField2 <= rangeField1 + lit(rangeBackNumeric)))
          .drop(windowedDf1("windowStart"))
          .drop(windowedDf2("windowStart"))
          .drop(windowedDf2("observationgroup"))
          .drop(windowedDf2("observationorder"))
          .drop(windowedDf2("frequency"))
          .drop(windowedDf2("snr"))
          .drop(windowedDf2("driftrate"))
          .drop(windowedDf2("uncorrectedfrequency"))
          .distinct()
    val res = df1.except(resjoin)

    Success(res)
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Define the criteria for anomalous signals for a given observation group
/////////////////////////////////////////////////////////////////////////////////////////////////////////////

println("Observation group number:")
val og = 101
println(og)

// Plus or minus range of frequency in Hz for ON and OFF observations
val pmrange = 100
val pmrangeoff = 100
// The sigal to noise ratio (SNR) and drift rate threshholds
val snron = 25
val snroff = 20
val driftrate = 0.0001


/////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Load signal data for each observation in this observation group and apply SNR and drift rate threshold filtering
/////////////////////////////////////////////////////////////////////////////////////////////////////////////

println("All six of the observations for this observation group:")
var measurements1 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +" and observationorder = 1").filter("snr > " + snron.toString + " and driftrate > " + driftrate.toString)
var measurements2 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +"  and observationorder = 2").filter("snr > " + snroff.toString + " and driftrate > " + driftrate.toString)
var measurements3 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +"  and observationorder = 3").filter("snr > " + snron.toString + " and driftrate > " + driftrate.toString)
var measurements4 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +" and observationorder = 4").filter("snr > " + snroff.toString + " and driftrate > " + driftrate.toString)
var measurements5 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +" and observationorder = 5").filter("snr > " + snron.toString + " and driftrate > " + driftrate.toString)
var measurements6 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +"  and observationorder = 6").filter("snr > " + snroff.toString + " and driftrate > " + driftrate.toString)
measurements1.show
measurements2.show
measurements3.show
measurements4.show
measurements5.show
measurements6.show

println("The number of signals in each of the six observations in the observation group after threshold filtering:")
println(measurements1.count)
println(measurements2.count)
println(measurements3.count)
println(measurements4.count)
println(measurements5.count)
println(measurements6.count)


/////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Range-join ON observations and range-anti-join OFF observations and print to screen at each stage
/////////////////////////////////////////////////////////////////////////////////////////////////////////////

var res1 = range_join_dfs(measurements1, measurements1("frequency"), measurements3, measurements3("frequency"), pmrange)
var res2 = range_join_dfs(res1.get, res1.get("frequency"), measurements5, measurements5("frequency"), pmrange)
var res3 = range_antijoin_dfs(res2.get, res2.get("frequency"), measurements2, measurements2("frequency"), pmrangeoff)
var res4 = range_antijoin_dfs(res3.get, res3.get("frequency"), measurements4, measurements4("frequency"), pmrangeoff)
var res5 = range_antijoin_dfs(res4.get, res4.get("frequency"), measurements6, measurements6("frequency"), pmrangeoff)

println("The signals after joining observations 1 and 3:")
res1 match {
    case Failure(ex) => print(ex)
    case Success(df) => df.show
}
println("The number of signals after joining observations 1 and 3:")
println(res1.get.count())


println("The signals after joining observations 1 and 3 and 5:")
res2 match {
    case Failure(ex) => print(ex)
    case Success(df) => df.show
}
println("The number of signals after joining observations 1 and 3 and 5:")
println(res2.get.count())

println("The signals after joining all ON observations and anti-joining with observation 2:")
res3 match {
    case Failure(ex) => print(ex)
    case Success(df) => df.show
}
println("The number of signals after joining all ON observations and anti-joining with observations 2:")
println(res3.get.count())

println("The signals after joining all ON observations and anti-joining with observations 2 and 4:")
res4 match {
    case Failure(ex) => print(ex)
    case Success(df) => df.show
}
println("The number of signals after joining all ON observations and anti-joining with observations 2 and 4:")
println(res4.get.count())

println("The signals after joining all ON observations and anti-joining with all OFF observations:")
res5 match {
    case Failure(ex) => print(ex)
    case Success(df) => df.show
}
println("The number of signals after joining all ON observations and anti-joining with all OFF observations:")
println(res5.get.count())


/////////////////////////////////////////////////////////////////////////////////////////////////////////////
