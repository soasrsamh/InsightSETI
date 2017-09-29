///////////////////////////////////////////////////////////////////////////////////////////////
// This Spark Streaming script processes signal data from Kafka and saves to a Cassandra database.
// It also computes if anomalous signals exist for each incoming observation 
// groups and saves the results to another Cassandra table.
/////////////////////////////////////////////////////////////////////////////////////////////// 


/////////////////////////////////////////////////////////////////////////////////////////////// 
// Imports
/////////////////////////////////////////////////////////////////////////////////////////////// 

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import scala.util.{ Try, Success, Failure }
import scala.util.Random
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.streaming._
import org.apache.spark.unsafe.types.CalendarInterval
 
//to run:
//sbt assembly
//sbt package
//spark-submit --class PriceDataStreaming --master spark://[insert spark ip]:7077 --jars target/scala-2.11/streamhits-assembly-1.0.jar target/scala-2.11/streamhits-assembly-1.0.jar


/////////////////////////////////////////////////////////////////////////////////////////////// 
// Main 
/////////////////////////////////////////////////////////////////////////////////////////////// 

object PriceDataStreaming {

  def main(args: Array[String]) {

    // Define Kafka brokers and topics 
    val brokers = "ec2-35-163-47-103.us-west-2.compute.amazonaws.com:9092"
    val topics = "gbthits"
    val topicsSet = topics.split(",").toSet

    // Create context with batch interval
    val sparkConf = new SparkConf().setAppName("streamhits").set("spark.cassandra.connection.host", "35.165.134.68")
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    // Reduce the amount of log information printed to the screen (only warning level and above)
    import org.apache.log4j.Logger
    import org.apache.log4j.Level
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    

  /////////////////////////////////////////////////////////////////////////////////////////////// 
  // Join and anti-join functions
  /////////////////////////////////////////////////////////////////////////////////////////////// 


  // Implements faster range-joins than .join (for large sets) using a bucketing approach
  // so that only close items are compared with one another. 
  def range_join_dfs[U,V](df1:DataFrame, rangeField1:Column, df2:DataFrame, rangeField2:Column, rangeBack:Any):Try[DataFrame] = {
    // check that both fields are from the same (and the correct) type
    (df1.schema(rangeField1.toString).dataType, df2.schema(rangeField2.toString).dataType, rangeBack) match {
        case (x1: TimestampType, x2: TimestampType, rb:String) => true
        case (x1: NumericType, x2: NumericType, rb:Number) => true
        case _ => return Failure(new IllegalArgumentException("rangeField1 and rangeField2 must both be either numeric or timestamps. If they are timestamps, rangeBack must be a string, if numerics, rangeBack must be numeric"))
    }

    // Returns the "window grouping" function for timestamp/numeric.
    // Timestamps will return the start of the grouping window
    // Numeric will do integers division
    def getWindowStartFunction(df:DataFrame, field:Column) = {
        df.schema(field.toString).dataType match {
            case d: TimestampType => window(field, rangeBack.asInstanceOf[String])("start")
            case d: NumericType => floor(field / lit(rangeBack))
            case _ => throw new IllegalArgumentException("field must be either of NumericType or TimestampType")
        }
    }

    // Returns the difference between windows and a numeric representation of "rangeBack"
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
  
  /////////////////////////////////////////////////////////////////////////////////////////////// 

  //Function finds the anti-range join
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
    
    //actual joining logic starts here
    val windowedDf1 = df1.withColumn("windowStart", rf1WindowStart)
    val windowedDf2 = df2.withColumn("windowStart", rf2WindowStart)
        .union( df2.withColumn("windowStart", rf2WindowStart + lit(prevWindowDiff)) )
        .union( df2.withColumn("windowStart", rf2WindowStart - lit(prevWindowDiff)) )

    //The logic to perform an anti-join might be changed to be more efficent than simply just taking the join and then using except.
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

  ///////////////////////////////////////////////////////////////////////////////////////////////
  // For each minibatch logic
  /////////////////////////////////////////////////////////////////////////////////////////////// 
    
    // Get the lines and show results
    messages.foreachRDD { rdd =>

        val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
        import sqlContext.implicits._

        // ticksDF contains the new signal data from Kafka
        val lines = rdd.map(_._2)
        val ticksDF = lines.map( x => {
                                  val tokens = x.split(";")                        
                                  Tick(tokens(1).toInt, tokens(2).toInt, tokens(3).toDouble, tokens(4).toDouble, tokens(5).toDouble,tokens(6).toDouble )}).toDF()
        println("The signals that were read in this interval cycle:")
        ticksDF.show()
        println("Count of how many signals were read in this interval cycle:")
        println(ticksDF.count())

        // Save new signals to Cassandra database table
        ticksDF.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "hitinfo","keyspace" -> "hitplayground")).mode(SaveMode.Append).save()
        
        // Identify which observation groups that produced signal data this cycle.
        // Assumes observation groups come in in numerical order, one after the other.
        val groups = ticksDF.select(ticksDF("observationgroup")).distinct.orderBy("observationgroup")
        println("Observation groups that produced signal data this cycle and their count:")
        groups.show
        val numgroups = groups.count


        if (numgroups > 1) {
          // If true means an observation group has finished sending signals and a new observation group has started sending signals.
          // So anomalous signals can be calculated for the observation group that has just finished sending signals. 
          
          // Get the first observation group that has finished sending signals.
          val finishedgroup = groups.first().getInt(0)
          println("Finished observation group:")
          println(finishedgroup)
          
          // These are the parameters for the filtering and range-join matching as in the lastest Breakthrough Listen publication. 
          val og = finishedgroup
          val pmrange = 600
          val pmrangeoff = 600
          val snron = 25
          val snroff = 20
          val driftrate = 0.0001

          // Read in the hits from the 6 observations in this observation group and do initial filtering based on SNR and Dopplar drift rate.
          var measurements1 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +" and observationorder = 1").filter("snr > " + snron.toString + " and driftrate > " + driftrate.toString)
          var measurements2 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +" and observationorder = 2").filter("snr > " + snroff.toString + " and driftrate > " + driftrate.toString)
          var measurements3 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +" and observationorder = 3").filter("snr > " + snron.toString + " and driftrate > " + driftrate.toString)
          var measurements4 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +" and observationorder = 4").filter("snr > " + snroff.toString + " and driftrate > " + driftrate.toString)
          var measurements5 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +" and observationorder = 5").filter("snr > " + snron.toString + " and driftrate > " + driftrate.toString)
          var measurements6 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hitinfo", "keyspace" -> "hitplayground")).load().filter("observationgroup = " + og.toString +" and observationorder = 6").filter("snr > " + snroff.toString + " and driftrate > " + driftrate.toString)
          println("The six observations in the finished observation group:")
          measurements1.show
          measurements2.show
          measurements3.show
          measurements4.show
          measurements5.show
          measurements6.show
          println("The number of signals in each of the six finished observations:")
          println(measurements1.count)
          println(measurements2.count)
          println(measurements3.count)
          println(measurements4.count)
          println(measurements5.count)
          println(measurements6.count)

          // Perform the series of range-joins to identify signal hits in all observations ON source and that are not in any observations OFF source.
          // Observations 1, 3, and 5 are ON and 2, 4, and 6 are OFF.
          var res1 = range_join_dfs(measurements1, measurements1("frequency"), measurements3, measurements3("frequency"), pmrange)
          var res2 = range_join_dfs(res1.get, res1.get("frequency"), measurements5, measurements5("frequency"), pmrange)
          var res3 = range_antijoin_dfs(res2.get, res2.get("frequency"), measurements2, measurements2("frequency"), pmrangeoff)
          var res4 = range_antijoin_dfs(res3.get, res3.get("frequency"), measurements4, measurements4("frequency"), pmrangeoff)
          var res5 = range_antijoin_dfs(res4.get, res4.get("frequency"), measurements6, measurements6("frequency"), pmrangeoff)
          
          println("After joining observations 1 and 3:")
          res1.get.show
          println("Count after joining observations 1 and 3:")
          println(res1.get.count)

          println("After joining observations 1 and 3 and 6:")
          res2.get.show
          println("Count after joining observations 1 and 3 and 6:")
          println(res2.get.count)

          println("After joining observations 1 and 3 and 6 and anti-joining with 2:")
          res3.get.show
          println("Count after joining observations 1 and 3 and 6 and anti-joining with 2:")
          println(res3.get.count)

          println("After joining observations 1 and 3 and 6 and anti-joining with 2 and 4:")
          res4.get.show
          println("Count after joining observations 1 and 3 and 6 and anti-joining with 2 and 4:")
          println(res4.get.count)

          println("After joining observations 1 and 3 and 6 and anti-joining with 2 and 4 and 6:")
          res5.get.show
          println("Count after joining observations 1 and 3 and 6 and anti-joining with 2 and 4 and 6 and the final number of anomoulous hits in this group:")
          val numGroupHits = res5.get.count
          println(numGroupHits)

   
          // Save anomalous signal results for the finished group to Cassandra table
          val groupresults = Seq(((finishedgroup).toInt, (numGroupHits).toInt,"Random Source", 100.5, "Random RA", "Random DEC","f1","f2","f3","f4","f5","f6")).toDF("observationgroup", "grouphit", "source", "mjd", "ra", "dec", "filename1", "filename2", "filename3", "filename4", "filename5", "filename6")
          println("The anomalous signal results for the finished group:")
          groupresults.show()
          groupresults.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "groupinfo","keyspace" -> "hitplayground")).mode(SaveMode.Append).save()

        
        } else {
          println("An observation group has not finished this cycle.");
        }
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}


// Format for the incoming signal data.
case class Tick(observationgroup: Int, observationorder: Int, frequency: Double, snr:Double, driftrate:Double, uncorrectedfrequency:Double)

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}


