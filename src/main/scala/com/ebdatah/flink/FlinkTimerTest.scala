package com.ebdatah.flink

import java.time.Duration
import java.util
import java.util.Properties

import com.ebdatah.pojo.Order
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, TimestampAssigner, TimestampAssignerSupplier, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema

import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object FlinkTimerTest {
  def main(args: Array[String]): Unit = {

    val outPutTag = new OutputTag[String]("he")
    //获取环境变量
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStateBackend(new FsStateBackend("hdfs://"))
    env.enableCheckpointing(1000)
    val checkpointConfig: CheckpointConfig = env.getCheckpointConfig


    //读取Kafka的数据
    val kafkaPro = new Properties()
    kafkaPro.put("bootstrap.servers", "node02:9092,node04:9092,node03:9092")
    kafkaPro.put("group.id", "flink")
    kafkaPro.put("key.serializer", "org.apache.kafka.common.StringSerializer")
    kafkaPro.put("value.serializer", "org.apache.kafka.common.StringSerializer")
    //    kafkaPro.put("auto.offset.reset","earliest")
    kafkaPro.put("enable.auto.commit", "false")
    //重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 2000))
    //waterMarker的时间间隔
    //    env.getConfig.setAutoWatermarkInterval()
    //     env.setStreamTimeCharacteristic()  //不需要设置 默认EventTime
    val kafkaSource: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("", new SimpleStringSchema(), kafkaPro).setStartFromLatest())
    kafkaSource
      .map((line: String) => {
        val split: Array[String] = line.split(",")
        Order(split(0).toLong, split(1).toLong, split(2).toLong)
      }).assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(Duration.ofSeconds(1L))
        .withTimestampAssigner(new SerializableTimestampAssigner[Order] {
          override def extractTimestamp(element: Order, recordTimestamp: Long): Long = {
            //事件事件
            element.orderTime
          }
        })
    ).keyBy((_: Order).orderId)
      .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
      .aggregate(
        new AggregateFunction[Order, List[Order], String] {
          //初始化累加器
          override def createAccumulator(): List[Order] = {
            List[Order]()
          }

          //单分区累加
          override def add(value: Order, accumulator: List[Order]): List[Order] = ???

          //获取累加器的结果 返回最总的结果
          override def getResult(accumulator: List[Order]): String = {
            accumulator.head.orderId.toString
          }

          //多分区累加器merge
          override def merge(a: List[Order], b: List[Order]): List[Order] = ???

        },
        //        (l: Long, w: TimeWindow, iter: Iterable[String], out: Collector[Order])=>{
        //
        //        }

        //
        new ProcessWindowFunction[String, Order, Long, TimeWindow] {
          override def process(key: Long, context: Context, elements: Iterable[String], out: Collector[Order]): Unit = {
            if (123 == key) {
              //侧输出流
              context.output(outPutTag, "")
            } else {
              out.collect(Order(1, 2, 3))
            }
          }
        }


        //
      ).print()

    env.execute()

    //    .reduce()
    //.process()
    //.apply()
    /**
     * * @tparam IN The type of the input value.
     * * @tparam OUT The type of the output value.
     * * @tparam KEY The type of the key.
     * * @tparam W The type of the window.
     */

    /**
     * * @param <IN>  The type of the values that are aggregated (input values)
     * * @param <ACC> The type of the accumulator (intermediate aggregate state).
     * * @param <OUT> The type of the aggregated result
     */
  }
}

//class TestProcess extends KeyedProcessFunction[String,String,String] {
//
//  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, String, String]#OnTimerContext, out: Collector[String]): Unit = super.onTimer(timestamp, ctx, out)
//
//  override def processElement(value: String, ctx: KeyedProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
//
//  }
//}