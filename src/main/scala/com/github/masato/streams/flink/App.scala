package com.github.masato.streams.flink

import java.util.Properties
import java.time.ZoneId;
import java.time.format.DateTimeFormatter
import java.util.Date

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010,FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization.{JSONDeserializationSchema,SimpleStringSchema}
import org.apache.flink.api.common.functions.AggregateFunction

import org.apache.flink.util.Collector
import com.fasterxml.jackson.databind.node.ObjectNode
import scala.util.parsing.json.JSONObject
import com.typesafe.config.ConfigFactory

case class Accumulator(time: Long, bid: String, var sum: Double, var count: Int)

class Aggregate extends AggregateFunction[(String, Double), Accumulator,Accumulator] {

  override def createAccumulator(): Accumulator = {
    return Accumulator(0L, "", 0.0, 0)
  }

  override def merge(a: Accumulator, b: Accumulator): Accumulator = {
    a.sum += b.sum
    a.count += b.count
    return a
  }

  override def add(value: (String, Double), acc: Accumulator): Unit = {
    acc.sum += value._2
    acc.count += 1
  }

  override def getResult(acc: Accumulator): Accumulator = {
    return acc
  }
}

object App {
  val fmt = DateTimeFormatter.ISO_OFFSET_DATE_TIME

  val conf = ConfigFactory.load()
  val bootstrapServers = conf.getString("app.bootstrap-servers")
  val groupId = conf.getString("app.group-id")
  val sourceTopic = conf.getString("app.source-topic")
  val sinkTopic = conf.getString("app.sink-topic")

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", bootstrapServers)
    props.setProperty("group.id", groupId)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val source = new FlinkKafkaConsumer010[ObjectNode](
      sourceTopic, new JSONDeserializationSchema(), props)

    val events = env.addSource(source).name("events")

    val timestamped = events.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[ObjectNode](Time.seconds(10)) {
        override def extractTimestamp(element: ObjectNode): Long = element.get("time").asLong * 1000
      })

    timestamped
      .map { v =>
        val key =  v.get("bid").asText
        val ambient = v.get("ambient").asDouble
        (key, ambient)
      }
      .keyBy(v => v._1)
      .timeWindow(Time.seconds(60))
      .aggregate(new Aggregate(),
        ( key: String,
          window: TimeWindow,
          input: Iterable[Accumulator],
          out: Collector[Accumulator] ) => {
            var in = input.iterator.next()
            out.collect(Accumulator(window.getEnd, key, in.sum/in.count, in.count))
          }
      )
      .map { v =>
        val zdt = new Date(v.time).toInstant().atZone(ZoneId.systemDefault())
        val time = fmt.format(zdt)
        val json = Map("time" -> time, "bid" -> v.bid, "ambient" -> v.sum)
        val retval = JSONObject(json).toString()
        println(retval)
        retval
      }
      .addSink(new FlinkKafkaProducer010[String](
        bootstrapServers,
        sinkTopic,
        new SimpleStringSchema)
      ).name("kafka")
    env.execute()
  }
}
