import scala.util.Random
import java.util.Calendar
import org.apache.spark.sql._
//import org.apache.spark.sql.{Dataset, DataFrame, Column}
// val sc: SparkContext // An existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.{ Try, Success, Failure }
//import com.datastax.spark.connector.cql.CassandraConnector
//import com.datastax.spark.connector._
//import com.datastax.spark.connector.{SomeColumns, _}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf

//to run:
//$SPARK_HOME/bin/spark-shell --conf spark.cassandra.connection.host=[insert my cassandra hostname here]
// --packages datastax:spark-cassandra-connector:2.0.1-s_2.11 -i ~/range-join-project/src/main/scala/groupHitTest.scala



//to read:
//val df = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load()
//df.filter("observationgroup = 1 and observationorder = 1").show

/***

case class MeasurementHits(observationgroup:Int, observationorder:Int, frequency:Double, snr:Double, driftrate:Double, uncorrectedfrequency:Double)

def generateMeasurementsHits(n:Long,groupnum:Int,groupord:Int):Dataset[MeasurementHits] = {
    val measurementsHits = sqlContext.range(0,n).select(
        (lit(groupnum)).as("observationgroup"),
        (lit(groupord)).as("observationorder"),
        (lit(10000)*rand(1*groupord)).as("frequency"),
        (lit(100)*rand(2*groupord)).as("snr"),
        (lit(1)*rand(3*groupord)).as("driftrate"),
        (lit(10000)*rand(1*groupord)+rand(4*groupord)-rand(5*groupord)).as("uncorrectedfrequency")
    ).as[MeasurementHits]
    
    measurementsHits   
}


//to write:
for (a <- 1 to 6) {

   val res = generateMeasurementsHits(10,10,a).toDF
   //res.show

   res.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "hitinfo","keyspace" -> "hitplayground")).mode(SaveMode.Append).save()
}

val df2 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load()

df2.show

***/

/***

case class Measurement(mid:Long, measurementTime:java.sql.Timestamp, hitCenterFrequency:Double)

def generateMeasurements(n:Long):Dataset[Measurement] = {
    val measurements = sqlContext.range(0,n).select(
        col("id").as("mid"),
        // measurementTime is more random, but generally every 10 seconds
        (unix_timestamp(current_timestamp()) - lit(10)*col("id") + lit(5)*rand()).cast(TimestampType).as("measurementTime"),
        (lit(100)*rand(10)).as("hitCenterFrequency")
    ).as[Measurement]

    measurements
}

generateMeasurements(5).toDF.show


case class Measurement2(mid:Long, measurementTime:java.sql.Timestamp, hitCenterFrequency:Double)

def generateMeasurements2(n:Long):Dataset[Measurement2] = {
    val measurements2 = sqlContext.range(0,n).select(
        col("id").as("mid"),
        // measurementTime is more random, but generally every 10 seconds
        (unix_timestamp(current_timestamp()) - lit(10)*col("id") + lit(5)*rand()).cast(TimestampType).as("measurementTime"),
        (lit(100)*rand(20)).as("hitCenterFrequency")
    ).as[Measurement2]

    measurements2
}






var events = generateEvents(100)
var measurements = generateMeasurements(100)

// An example with a timestamp field would look like this:
val res = events.join(measurements,
   (measurements("measurementTime") > events("eventTime") - CalendarInterval.fromString("interval 30 seconds") ) &&
   (measurements("measurementTime") <= events("eventTime"))
)

// With a numeric field (took the id as an example, this is obviously useless):
val res = events.join(measurements,
    (measurements("mid") > events("eid") - lit(2)) &&
    (measurements("mid") <= events("eid"))
)

res.explain

// run something like `res.count` to make Spark actually perform the join.
res.count
res.show
events.show
measurements.show


***/

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

//    df1.show
//    df2.show
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
//    res.show
//    res.distinct().show
    Success(res)
}




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
    //df1.show
    //resjoin.show

    val res = df1.except(resjoin)
    //res.show
    Success(res)
}


//var events2 = generateEvents(1000000).toDF
//var measurements2 = generateMeasurements(15).toDF
//var measurements3 = generateMeasurements2(15).toDF


// you can either join by timestamp fields
//var res2 = range_join_dfs(events2, events2("eventTime"), measurements2, measurements2("measurementTime"), "60 minutes")
// or by numeric fields (again, id was taken here just for the purpose of the example)
//var res2 = range_join_dfs(events2, events2("eid"), measurements2, measurements2("mid"), 10)



var beforeload = 0.0
var afterload = 0.0
var afterjoin = 0.0
var aftercount = 0.0
val og = 52

//for (og <- 100 to 114) {
println("group number")
println(og)

//val og = 12
val pmrange = 100
val pmrangeoff = 100
val snron = 25
val snroff = 20
val driftrate = 0.01

println("before read measurements")
//println(Calendar.getInstance().getTime())
beforeload = System.currentTimeMillis()
println(beforeload)


var measurements1 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +" and observationorder = 1")
var measurements2 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +"  and observationorder = 2")

measurements1.show
measurements2.show
var measurements3 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +"  and observationorder = 3")
var measurements4 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +" and observationorder = 4")
measurements3.show
measurements4.show
var measurements5 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +" and observationorder = 5")
var measurements6 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +"  and observationorder = 6")
measurements5.show
measurements6.show
println(measurements1.count)
println(measurements2.count)
println(measurements3.count)
println(measurements4.count)
println(measurements5.count)
println(measurements6.count)



println("before rangejoin")
//println(Calendar.getInstance().getTime())
afterload = System.currentTimeMillis()
println(afterload)
println(beforeload-afterload)

var res1 = range_join_dfs(measurements1, measurements1("frequency"), measurements3, measurements3("frequency"), pmrange)


res1 match {
    case Failure(ex) => print(ex)
    //case Success(df) => df.explain
    case Success(df) => df.show                                                                                             
}


println("before count")
//println(Calendar.getInstance().getTime())
afterjoin = System.currentTimeMillis()
println(afterjoin)
println(afterjoin-afterload)

res1.get.count()
println("after count")
//println(Calendar.getInstance().getTime())
aftercount = System.currentTimeMillis()
println(aftercount)
println(aftercount-afterload)



//res1.get.show


var res2 = range_join_dfs(res1.get, res1.get("frequency"), measurements5, measurements5("frequency"), pmrange)
var res3 = range_antijoin_dfs(res2.get, res2.get("frequency"), measurements2, measurements2("frequency"), pmrangeoff)
var res4 = range_antijoin_dfs(res3.get, res3.get("frequency"), measurements4, measurements4("frequency"), pmrangeoff)
var res5 = range_antijoin_dfs(res4.get, res4.get("frequency"), measurements6, measurements6("frequency"), pmrangeoff)

res2 match {
    case Failure(ex) => print(ex)
    //case Success(df) => df.explain
    case Success(df) => df.show
}
res2.get.count

res3 match {
    case Failure(ex) => print(ex)
    //case Success(df) => df.explain
    case Success(df) => df.show
}
res3.get.count
res4 match {
    case Failure(ex) => print(ex)
    //case Success(df) => df.explain
    case Success(df) => df.show
}
res4.get.count
res5 match {
    case Failure(ex) => print(ex)
    //case Success(df) => df.explain
    case Success(df) => df.show
}
res5.get.count


// and run something like `res.count` to actually perform anything.
//res3.count
//res3.show



