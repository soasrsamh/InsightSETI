/////////////////////////////////////////////////////////////////////////////////////////////// 
// Generates random signals for benchmarking tests. 
// Saves the signals to a cassandra table.
/////////////////////////////////////////////////////////////////////////////////////////////// 


/////////////////////////////////////////////////////////////////////////////////////////////// 
// Imports
/////////////////////////////////////////////////////////////////////////////////////////////// 

import scala.util.Random
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
//$SPARK_HOME/bin/spark-shell --conf spark.cassandra.connection.host=[insert cassandra ip] --packages datastax:spark-cassandra-connector:2.0.1-s_2.11 -i ~/range-join-project/src/main/scala/generateHits.scala



/////////////////////////////////////////////////////////////////////////////////////////////// 
// Format of a signal
/////////////////////////////////////////////////////////////////////////////////////////////// 

case class MeasurementHits(observationgroup:Int, observationorder:Int, frequency:Double, snr:Double, driftrate:Double, uncorrectedfrequency:Double)




/////////////////////////////////////////////////////////////////////////////////////////////// 
// Function to generate n signals for a given observation
/////////////////////////////////////////////////////////////////////////////////////////////// 

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



/////////////////////////////////////////////////////////////////////////////////////////////// 
// Generate some observation groups where each observation has N signals for benchmarking
/////////////////////////////////////////////////////////////////////////////////////////////// 
for (b <- 0 to 14) {
   println(100*scala.math.pow(2,b))
   println(100+b)
   for (a <- 1 to 6) {

      // Generate signal data
      val res = generateMeasurementsHits((100*scala.math.pow(2,b)).toInt,(100+b).toInt,a).toDF
      println("Count of generated signals:")
      println(res.count)

      // Save signals to cassandra database table
      res.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "hitinfo","keyspace" -> "hitplayground")).mode(SaveMode.Append).save()
   }
}

