///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// For a series of observation groups (with increasing numbers of signals), the time it takes to join two
// observations is timed to benchmark against the default join (groupHitTestdefaultjoin.scala).
// The range-join employed here uses a bucketing technique that avoids a full-cartesian N^2 pairwise join for large
// element sets.
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Imports
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
// --packages datastax:spark-cassandra-connector:2.0.1-s_2.11 -i ~/range-join-project/src/main/scala/groupHitTestEffJoin.scala


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Join and anti-join methods
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Join data based on if a column value is within a range of a value in another column using a bucketing technique
// to avoid a full cartesian join for large element sets
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
    // add elements from df2 to three buckets each to guarentee each element will be joined with any existing matches in df1
    val windowedDf2 = df2.withColumn("windowStart", rf2WindowStart)
        .union( df2.withColumn("windowStart", rf2WindowStart + lit(prevWindowDiff)) )
        .union( df2.withColumn("windowStart", rf2WindowStart - lit(prevWindowDiff)) )

    // Join within each bucket and drop windows.
    // Dropping the other columns should probably be done outside this method.
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

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Anti-join based on if a value of one column is not found within a given range of any value in another column
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

    // The anti-join logic likely can be improved beyond performing a bucketing-range-join and except.
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


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// For each observation group, time how long it takes to perform the bucketing range-join operation
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Benchmarking time variables (in milliseconds)
var beforeload = 0.0
var afterload = 0.0
var afterjoin = 0.0
var aftercount = 0.0

//The plus or minus range within which two signals will be matched and joined
val pmrange = 10

// For a series of observation groups (with an increasing number of elements), perform and time a range-join  
for (og <- 100 to 114) {
println("Observation group number:")
println(og)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Retrieve two observations from the observation group from the database
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

println("before read measurements")
beforeload = System.currentTimeMillis()
println(beforeload)

var measurements1 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +" and observationorder = 1")
var measurements3 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +"  and observationorder = 3")

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Call for the range-join with the customly defined bucket method
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

println("before rangejoin")
afterload = System.currentTimeMillis()
println(afterload)
println(beforeload-afterload)

var res1 = range_join_dfs(measurements1, measurements1("frequency"), measurements3, measurements3("frequency"), pmrange)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Force the join operation to compute by counting the number of signals that were joined
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

println("before count")
afterjoin = System.currentTimeMillis()
println(afterjoin)
println(afterjoin-afterload)

println("Number of joined signals:")
println(res1.get.count())

println("after count")
aftercount = System.currentTimeMillis()
println(aftercount)
println(aftercount-afterload)

} //end for loop
