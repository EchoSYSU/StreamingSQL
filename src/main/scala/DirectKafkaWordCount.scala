/**
 * Created by Thinkpad on 2015/7/16.
 */
import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: DirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
 *    topic1,topic2
 */
object DirectKafkaWordCount {

  def main(args: Array[String]) {

    val brokers = ""
    val topics = "test"

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(":"))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print(10000)

    //这个方法是通过spark sql把json 文件解析出来
    lines.foreachRDD((rdd: RDD[String])=> {
      val sqlContext = new SQLContext(rdd.sparkContext)
      val appRecord = sqlContext.read.json(rdd)
      appRecord.printSchema()
      //appRecord.show
      appRecord.registerTempTable("app")
      appRecord.select("id").show
      sqlContext.sql("select event_list.e_time from app").show
    })


     //这个方法是传入一个普通文件，并定义模式类，把文件解析出来
     //Convert RDDs of the words DStream to DataFrame and run SQL query
//      lines.foreachRDD((rdd: RDD[String]) => {
//      // Get the singleton instance of SQLContext
//      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
//      import sqlContext.implicits._
//
//      // Convert RDD[String] to RDD[case class] to DataFrame
//      val transRecord = rdd.map(r => {
//        val splits = r.split(" ")
//        val record = Record(splits(0),splits(1).toLong,splits(2).toInt,
//          splits(3).toDouble,splits(4).toDouble)
//        record
//      }).toDF()
//      // Register as table
//         transRecord.registerTempTable("transportation")
//
//      // Do word count on table using SQL and print it
//      //sqlContext.sql("select * form transportation as trans where trans.name = 'HaiShangShiJiei'").collect().foreach(println)
//      sqlContext.sql("select count(*) from transportation").show() // the path of test text is  /data/traffic/taxi/original_data/merge/area_merge
//      println("================ time ==================")
//  })
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}

  /** Case class for converting RDD to DataFrame */
case class Record(name:String,id:Long,num:Int,longitude:Double,latitude:Double)


  /** Lazily instantiated singleton instance of SQLContext */
  object SQLContextSingleton {

    @transient private var instance: SQLContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
  }
