package com.cebdata.flink


import java.sql.Timestamp
import java.time.Duration

import akka.serialization.SerializerWithStringManifest
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkStrategy}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.flink.api.common.time.Time


object FlinkTimerTest {
  def main(args: Array[String]): Unit = {
    //获取环境变量
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //全局的状态管理器
    env.getConfig.setAutoWatermarkInterval(2000)
    val ds01: DataStream[(String, Int)] = env.socketTextStream("node01", 8888)
      .flatMap((_: String).split(","))
      .map(((_: String), 1))
      .keyBy((_: (String, Int))._1)
      .sum(1)


    val ds02: DataStream[(String, Int)] = env.socketTextStream("node01", 9999)
      .flatMap((_: String).split(","))
      .map(((_: String), 1))
      .keyBy((_: (String, Int))._1)
      .sum(1).disableChaining()


    //定义侧输出流
    val outTag = new OutputTag[String]("tag")


    val ds04: DataStream[(String, Int)] = ds01.connect(ds02).keyBy(a => a._1, b => b._1)
      .process(new CoProcessFunction[(String, Int), (String, Int), (String, Int)] {

        var map1: MapState[String, (String, Int)] = _
        var map2: MapState[String, (String, Int)] = _


        override def open(parameters: Configuration): Unit = {

          val ttlConfig: StateTtlConfig = StateTtlConfig
            .newBuilder(Time.minutes(1)) //存活时间
            .cleanupFullSnapshot()
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) //不返回过期数据
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) //更新
            .build()

          val mysqlStat = new MapStateDescriptor("s1", classOf[String], classOf[(String, Int)])
          mysqlStat.enableTimeToLive(ttlConfig)
          map1 = getRuntimeContext.getMapState(mysqlStat)

          val ds1Stat = new MapStateDescriptor("s2", classOf[String], classOf[(String, Int)])
          ds1Stat.enableTimeToLive(ttlConfig)
          map2 = getRuntimeContext.getMapState(ds1Stat)

        }


        override def processElement1(value: (String, Int), ctx: CoProcessFunction[(String, Int), (String, Int), (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {



          //注册定时触发器
//          print(ctx.timestamp())
          ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+ 2000 * 30)
          val t1: (String, Int) = map1.get(value._1)
          //先做word count
          if (t1 == null)
            map1.put(value._1, value)
          else
            map1.put(value._1, (value._1, value._2 + t1._2))

          //关联数据

          val t2: (String, Int) = map2.get(value._1)

          if (t2 == null)
            ctx.output[String](outTag, value._1 + "未关联到数据。。。。。当前的数量:" + value._2)
          else
            out.collect(("关联到数据" + value._1, map1.get(value._1)._2 + t2._2))

        }

        override def processElement2(value: (String, Int), ctx: CoProcessFunction[(String, Int), (String, Int), (String, Int)]#Context, out: Collector[(String, Int)]): Unit

        = {
          val t1: (String, Int) = map2.get(value._1)
          //先做word count
          if (t1 == null)
            map2.put(value._1, value)
          else
            map2.put(value._1, (value._1, value._2 + t1._2))

          //关联数据

          val t2: (String, Int) = map1.get(value._1)

          if (t2 == null)
            ctx.output[String](outTag, value._1 + "未关联到数据。。。。。当前的数量:" + value._2)
          else
            out.collect(("关联到数据" + value._1, map2.get(value._1)._2 + t2._2))
        }


        //        //时间触发器
        override def onTimer(timestamp: Long, ctx: CoProcessFunction[(String, Int), (String, Int), (String, Int)]#OnTimerContext, out: Collector[(String, Int)]): Unit = {
          println("出发了定时器：" + "当前的watermaker" + ctx.timerService().currentWatermark() + ":当前的处理时间" + ctx.timerService().currentProcessingTime())
        }


      }

      ).disableChaining()

    ds04.print("主流")

    ds04.getSideOutput(outTag).print("侧输出流")


    env.execute()


  }

}