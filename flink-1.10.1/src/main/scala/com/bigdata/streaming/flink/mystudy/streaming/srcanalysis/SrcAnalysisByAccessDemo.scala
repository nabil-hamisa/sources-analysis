package com.bigdata.streaming.flink.mystudy.streaming.srcanalysis

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Properties

import com.bigdata.streaming.flink.mystudy.pojo.KeyValueTime
import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, MapFunction, ReduceFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment, WindowedStream, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.srcutils.SrcCount
import org.apache.flink.util.Collector
import org.junit.{Before, Test}

class SrcAnalysisByAccessDemo {

  @Before
  def setUp(): Unit ={
    SrcCount.initCountArray(
      SrcCount.Key.Index_0,
      SrcCount.Key.Index_1,
      SrcCount.Key.Index_2
    )

  }

  @Test
  def testWordCount() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val text = env.readTextFile("input/wc")
//    val text = env.fromElements("data spark flin", "Hello Spark Hello Scala")
    println(text.parallelism)

    val map: DataStream[String] = text.flatMap(x=>{
      x.toLowerCase.split("\\W+")
    })
    val filter: DataStream[String] = map.filter(_.nonEmpty)
    println(filter.parallelism)

    val keyed: KeyedStream[(String, Int), Tuple] = filter
      .map(x=>{
        (x, 1)
      })
      .keyBy(0)
    println(keyed.parallelism)

    val sum: DataStream[(String, Int)] = keyed.reduce(new WCReduceFunc()).setParallelism(2)
    val json: DataStream[String] = sum.map(x => {
      s"{key=${x._1}, value=${x._2}}"
    })

    println(json.parallelism)
    json.print()

    SrcCount.initCountArray(
      SrcCount.Key.Index_0,
      SrcCount.Key.Index_1,
      SrcCount.Key.Index_2
    )

    env.execute("Streaming WordCount")
  }


  @Test
  def testWordCount_notBuild() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val text = env.readTextFile("input/wc")
    //    val text = env.fromElements("data spark flin", "Hello Spark Hello Scala")
    println(text.parallelism)

    val map: DataStream[String] = text.flatMap(x=>{
      x.toLowerCase.split("\\W+")
    })
    val filter: DataStream[String] = map.filter(_.nonEmpty)
    println(filter.parallelism)

    val keyed: KeyedStream[(String, Int), Tuple] = filter
      .map(x=>{
        (x, 1)
      })
      .keyBy(0)
    println(keyed.parallelism)

    val sum: DataStream[(String,Int)] = keyed.reduce(new WCReduceFunc())
    val json: DataStream[String] = sum.map(x => {
      s"{key=${x._1}, value=${x._2}}"
    })
    println(json.parallelism)
    json.print()

    env.execute("Streaming WordCount")
  }

  @Test
  def testSocketSrc_tumblingWin_keyBy_sideTag_myWatermark(): Unit ={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val outputTag = new OutputTag[(String, Double,String)]("late-data")

    /*测试数据:
      2020-11-02 21:01:05.998,a,1
      2020-11-02 21:01:05.999,a,1
      2020-11-02 21:01:06.998,a,1
      2020-11-02 21:01:06.999,a,1
      2020-11-02 21:01:08.999,a,1
     */

    val line: DataStream[String] = env.socketTextStream("ldsver50",9099)
    val etled: DataStream[KeyValueTime] = line
      .map(x => {
        //        println(x)
        KeyValueTime(x)
      })
    val maxOutOfOrderness:Long = 2000
    etled.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[KeyValueTime] {
      var lastCommittedWatermark:Long = Long.MinValue
      var currentMaxTimestamp:Long = _

      override def getCurrentWatermark: Watermark = {
        if(currentMaxTimestamp - maxOutOfOrderness >= lastCommittedWatermark){
          lastCommittedWatermark = currentMaxTimestamp
        }
        new Watermark(lastCommittedWatermark)
      }

      val timeFormat = new SimpleDateFormat("HH:mm:ss.SSS")
      override def extractTimestamp(element: KeyValueTime, previousElementTimestamp: Long): Long = {
        val currentEventTime = element.ts
        if(currentEventTime > currentMaxTimestamp){
          currentMaxTimestamp = currentEventTime
        }
        val currentWM = getCurrentWatermark.getTimestamp
        val curTimeStr: String = timeFormat.format(new Date(currentEventTime))
        val curWatermarkStr: String = timeFormat.format(new Date(currentWM))
        println(s"Event( time=${curTimeStr},value=${element.value} ); currentWatermark=${curWatermarkStr} ")
        currentEventTime
      }
    })
    val event: DataStream[(String, Double,String)] = etled
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[KeyValueTime](Time.seconds(0)) {
          override def extractTimestamp(element: KeyValueTime): Long = element.getTime
        })
      .map(x => x.asTimeStrKV())

    val timeFormat = new SimpleDateFormat("HH:mm:ss")

    val keyed: KeyedStream[(String, Double,String), String] = event.keyBy(_._1)
    val windowed: WindowedStream[(String, Double,String), String, TimeWindow] = keyed
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .sideOutputLateData(outputTag)

    val result: DataStream[String] = windowed.reduce(
      new ReduceFunction[(String, Double, String)] {
        override def reduce(value1: (String, Double, String), value2: (String, Double, String)): (String, Double, String) = {
          (value1._1, value1._2 + value2._2, value1._3)
        }
      },
      new ProcessWindowFunction[(String, Double, String), String, String, TimeWindow]() {
        override def process(key: String, context: Context, elements: Iterable[(String, Double, String)], out: Collector[String]): Unit = {
          val it  = elements.map(_._2).toList
          val sum: Double = it.sum
          val count: Int = it.size //前面进行了预聚合了, 因为只有1个key, 所以这里只有1条;
          val start: String = timeFormat.format(new Date(context.window.getStart))
          val end: String = timeFormat.format(new Date(context.window.getEnd))
          out.collect(s"窗口:[${start} - ${end}]  count=${count}   sum=${sum}")
        }
      })

    result.print("reduceWin-sum")
    result.getSideOutput(outputTag ).print("lateData")

    env.execute(this.getClass.getSimpleName)

  }


  @Test
  def testKafkaSourceWordCount() {
    val  topic = "testWordCount" // testWordCount testEnosRecords
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "ldsver53:9092")
    properties.setProperty("group.id", "testFlinkStreaming")

    val kafkaSource = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema, properties)
    kafkaSource.setStartFromLatest
    kafkaSource.setCommitOffsetsOnCheckpoints(false)

    val stream: DataStream[String] = env.addSource(kafkaSource)
    stream.flatMap(new MySplitFlatMap)
      .filter(new MyFilter)
      .map(new MyMapFunc)
      .keyBy(new MyKeySelector)
      .reduce(new MyReduceFunc).setParallelism(1)
      .print().setParallelism(1)


    env.execute("Streaming WordCount")
  }

  @Test
  def testKafkaSourceWordCount_noBuild() {
    val  topic = "testWordCount" // testWordCount testEnosRecords
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "ldsver53:9092")
    properties.setProperty("group.id", "testFlinkStreaming")

    val kafkaSource = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema, properties)
    kafkaSource.setStartFromLatest
    kafkaSource.setCommitOffsetsOnCheckpoints(false)

    val stream: DataStream[String] = env.addSource(kafkaSource)
    stream.flatMap(new MySplitFlatMap)
      .filter(new MyFilter)
      .map(new MyMapFunc)
      .keyBy(new MyKeySelector)
      .reduce(new MyReduceFunc).setParallelism(1)
      .print().setParallelism(1)


    env.execute("Streaming WordCount")
  }



  @Test
  def testStringToBytes(): Unit ={
    def toBytes(str:String):Array[Byte]={
      str.getBytes
    }
    val str = "3"
    toBytes(str).foreach(println)
    toBytes(",").foreach(println)
    toBytes("1").foreach(println)

  /*
     3    51
     ,    44
     1    49
   */


  }



}


class WCReduceFunc extends ReduceFunction[(String,Int)]{
  override def reduce(value1: (String,Int), value2: (String,Int)): (String,Int) = {
    (value1._1,value1._2+value2._2)
  }
}

class MySplitFlatMap extends FlatMapFunction[String, String] {
  override def flatMap(value: String, out: Collector[String]): Unit = {
    val split = value.split(" ")
    for(ele <- split){
      out.collect(ele)
    }
  }
}


class MyFilter extends FilterFunction[String] {
  override def filter(value: String): Boolean = {
    !value.isEmpty
  }
}

class MyMapFunc extends MapFunction[String,String] {
  override def map(value: String): String = {
    val tuple: (String, Int) = (value.trim.toLowerCase(),1)
    tuple._1+","+tuple._2
  }
}

class MyKeySelector extends KeySelector[String,String]{
  override def getKey(value: String): String = {
    value.split(",")(0).trim
  }
}

class MyReduceFunc extends ReduceFunction[String]{
  override def reduce(value1: String, value2: String): String = {
    val arr1: Array[String] = value1.split(",")
    val arr2: Array[String] = value2.split(",")
    val tuple: (String, Int) = (arr1(0).trim,arr1(1).trim.toInt+ arr2(1).trim.toInt)
    tuple._1+","+tuple._2
  }
}
