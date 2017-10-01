///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Default Euclidian distance join for a benchmarking set of observation groups.
// Performs a cartesian pairwise join operation.
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
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors


//to run:
//$SPARK_HOME/bin/spark-shell --conf spark.cassandra.connection.host=[insert cassandra host]
// --packages datastax:spark-cassandra-connector:2.0.1-s_2.11 -i ~/range-join-project/src/main/scala/groupHitTestsimilarityjoindefault.scala


//Time benchmarking variables
var aftermeas = 0.0
var aftervectorize = 0.0
var aftertrain = 0.0
var beforeload = 0.0
var afterload = 0.0
var afterjoin = 0.0
var aftercount = 0.0

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// For each observation group time how long it takes to load and join two observations with the default distance join
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

for (og <- 100 to 114) {
println("group number")
println(og)


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//Load observations fromt the database
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

println("before load measurements")
beforeload = System.currentTimeMillis()
println(beforeload)

var measurements1 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +" and observationorder = 1")
var measurements3 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +"  and observationorder = 3")
println("counts of elements in each observation")
println(measurements1.count)
println(measurements3.count)

measurements1.show
measurements3.show

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Use a default join based on euclidian distance
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

println("after read measurments")
afterload = System.currentTimeMillis()
println(aftermeas)
println(beforeload-afterload)

//The distance between two normalized signal feature sets must be within this range to match
val pmrange = 0.0001

val res1 = measurements1.join(measurements3,
    ( lit(pmrange*pmrange) >= 
      (measurements1("frequency")-measurements3("frequency"))*(measurements1("frequency")-measurements3("frequency"))*(0.00000001) +
      (measurements1("snr")-measurements3("snr"))*(measurements1("snr")-measurements3("snr"))*(0.0001) +
      (measurements1("driftrate")-measurements3("driftrate"))*(measurements1("driftrate")-measurements3("driftrate")) )
    )
//res1.show

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Count the number of joined signals
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

println("before count")
afterjoin = System.currentTimeMillis()
println(afterjoin)
println(afterjoin-afterload)

println(res1.count)

println("after count")
aftercount = System.currentTimeMillis()
println(aftercount)
println(aftercount-afterload)

} //end for loop



