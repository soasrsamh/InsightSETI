/////////////////////////////////////////////////////////////////////////////////////////////////////////
// For a series of observation groups (with increasing number of signals), test how long a default
// join takes. These benchmark time values are compared to the values from a bucketing range-join 
// in groupHitTestEffjoin.scala.
/////////////////////////////////////////////////////////////////////////////////////////////////////////



/////////////////////////////////////////////////////////////////////////////////////////////////////////
// Imports
/////////////////////////////////////////////////////////////////////////////////////////////////////////

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
// --packages datastax:spark-cassandra-connector:2.0.1-s_2.11 -i ~/range-join-project/src/main/scala/groupHitTestdefaultjoin.scala

/////////////////////////////////////////////////////////////////////////////////////////////////////////
// For each observation group time how long a default join takes
/////////////////////////////////////////////////////////////////////////////////////////////////////////

// Time benchmarking variables
var beforeload = 0.0
var afterload = 0.0
var afterjoin = 0.0
var aftercount = 0.0

// The plus and minus range (in Hz) within which two signals will be matched.
val pmrange = 10

//For each observation group (with increasing numbers of signals), test how long a default join takes:
for (og <- 100 to 114) {
println("Observation group number:")
println(og)

/////////////////////////////////////////////////////////////////////////////////////////////////////////
// Read in the data for the observation group from the database
/////////////////////////////////////////////////////////////////////////////////////////////////////////

println("before read measurements")
beforeload = System.currentTimeMillis()
println(beforeload)

var measurements1 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +" and observationorder = 1")
var measurements3 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +"  and observationorder = 3")
println(measurements1.count)
println(measurements3.count)

/////////////////////////////////////////////////////////////////////////////////////////////////////////
// Define the default join for the two observations
/////////////////////////////////////////////////////////////////////////////////////////////////////////

println("before rangejoin")
afterload = System.currentTimeMillis()
println(afterload)
println(beforeload-afterload)

val res1 = measurements1.join(measurements3,
    (measurements1("frequency") > measurements3("frequency") - lit(pmrange)) &&
    (measurements1("frequency") <= measurements3("frequency") + lit(pmrange))
)


/////////////////////////////////////////////////////////////////////////////////////////////////////////
// Force the join to be performed by counting the number of signals that were joined
/////////////////////////////////////////////////////////////////////////////////////////////////////////

println("before count")
//println(Calendar.getInstance().getTime())
afterjoin = System.currentTimeMillis()
println(afterjoin)
println(afterjoin-afterload)

println("Count of joined signals:")
println(res1.count)

println("after count")
//println(Calendar.getInstance().getTime())
aftercount = System.currentTimeMillis()
println(aftercount)
println(aftercount-afterload)

} //end for loop
