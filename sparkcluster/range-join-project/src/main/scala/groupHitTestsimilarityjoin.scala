////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// This benchmarks a similarity join using LSH (Locality Sensitive Hashing) Euclidian distance probabalistic
// matching with bucketed random projection by joining a series of observations with N signals each.
//
////////////////////////////////////////////////////////////////////////////////////////////////////////////////



////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Imports
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

import scala.util.Random
import java.util.Calendar
import org.apache.spark.sql.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.{ Try, Success, Failure 
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
//$SPARK_HOME/bin/spark-shell --conf spark.cassandra.connection.host=ec2-35-165-134-68.us-west-2.compute.amazonaws.com
// --packages datastax:spark-cassandra-connector:2.0.1-s_2.11 -i ~/range-join-project/src/main/scala/groupHitTestsimilarityjoin.scala

// Time tracking variables (in milliseconds)
var aftermeas = 0.0
var aftervectorize = 0.0
var aftertrain = 0.0
var beforeload = 0.0
var afterload = 0.0
var afterjoin = 0.0
var aftercount = 0.0


////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Apply LSH similarity join for a series of observation groups to benchmark the time it takes
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Join observations in a series of observations (with an increasing number of elements each)
for (og <- 100 to 114) {

println("Observation group number:")
println(og)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Load observations for a given observation group
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

println("before load measurements")
beforeload = System.currentTimeMillis()
println(beforeload)

var measurements1 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +" and observationorder = 1")
var measurements3 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +"  and observationorder = 3")
println("Counts of elements in each observation")
println(measurements1.count)
println(measurements3.count)
measurements1.show
measurements3.show

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Convert features of both observations to new columns of normalized feature vectors (each feature ranging from 0 to 1).
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

println("after read measurments")
afterload = System.currentTimeMillis()
println(aftermeas)
println(beforeload-afterload)

val assembler1 = new VectorAssembler()
  .setInputCols(Array("nfrequency", "nsnr", "ndriftrate"))
  .setOutputCol("features")
println("Observation 1:")
val output1 = assembler1.transform(measurements1.withColumn("nfrequency", measurements1("frequency")*0.0001).withColumn("nsnr", measurements1("snr")*0.01).withColumn("ndriftrate", measurements1("driftrate")*1.0))
output1.show


//measurement3 convert to [id, feature vector] format
val assembler3 = new VectorAssembler()
  .setInputCols(Array("nfrequency", "nsnr", "ndriftrate"))
  .setOutputCol("features")
println("Observation 2:")
val output3 = assembler3.transform(measurements3.withColumn("nfrequency", measurements3("frequency")*.0001).withColumn("nsnr", measurements3("snr")*.01).withColumn("ndriftrate", measurements3("driftrate")*1))
output3.show


////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Train the Bucketed Random Projection LSH model for Euclidean distance.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

println("after vectorize")
aftervectorize = System.currentTimeMillis()
println(aftervectorize)

//Bucketed Random Projection is an LSH family for Euclidean distance.
//The LSH projects feature vectors onto a set of random unit vectors and portions the projected results into hash buckets.
//The bucket length can be used to control the average size of hash buckets (and thus the number of buckets).
//A larger bucket length (i.e., fewer buckets) increases the probability of features being hashed to the same bucket (increasing the numbers of true and false positives).
//Similar feature vectors will be bucketed in the same bucket with high probability and only joined with vectors within the same bucket, avoiding a full cartesian pairwize computation at the cost of some accuracy.
//Bucketed Random Projection accepts arbitrary vectors as input features, and supports both sparse and dense vectors.
//Choose bucket length and number of hashes for the BRP to trade-off speed and accuracy. (e.g. a bucket length, A times larger than the matching distance means a probabiliity of at least (1-1/A) that two matching features will fall in the same bucket.)
val brp2 = new BucketedRandomProjectionLSH()
  .setBucketLength(.001)
  .setNumHashTables(4)
  .setInputCol("features")
  .setOutputCol("hashes")

val model2 = brp2.fit(output1)

// Feature Transformation
//println("The hashed dataset where hashed values are stored in the column 'hashes':")
model2.transform(output1)   
//.show


////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Use the LSH distance-join on the two observations
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

println("after train")
aftertrain = System.currentTimeMillis()
println(aftertrain)
println(aftertrain-aftervectorize)

//The distance between two signal feature sets must be within this range to match
val pmrange = 0.0001

// Compute the locality sensitive hashes for the input rows, then perform approximate similarity join.
println("Approximately joining dfA and dfB on Euclidean distance smaller than threshhold:")
var res1 = model2.approxSimilarityJoin(output1, output3, pmrange, "EuclideanDistance")
  .select(col("datasetA.observationgroup").alias("observationgroup"),col("datasetA.observationorder").alias("observationorder"),col("datasetA.frequency").alias("frequency"),
    col("datasetA.snr").alias("snr"),col("datasetA.driftrate").alias("driftrate"),col("datasetA.uncorrectedfrequency").alias("uncorrectedfrequency")).distinct()

//res1.show()

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Count the number of signals in the resulting distance-join
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

println("before count")
afterjoin = System.currentTimeMillis()
println(afterjoin)
println(afterjoin-aftervectorize)

println(res1.count)

println("after count")
aftercount = System.currentTimeMillis()
println(aftercount)
println(aftercount-aftervectorize)

} //end for loop (computing the probabalistic distance-join for each observation group)
