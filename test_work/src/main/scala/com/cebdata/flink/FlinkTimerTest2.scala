package com.cebdata.flink

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector


object FlinkTimerTest2 {
  def main(args: Array[String]): Unit = {
    //获取环境变量
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    
    //全局的状态管理器
    val ds01: DataStream[(String, Long)] = env.socketTextStream("node01", 8888)
      .map(line => (line.split(",")(0), line.split(",")(1).toLong))
            .assignTimestampsAndWatermarks(WatermarkStrategy
              .forBoundedOutOfOrderness(Duration.ofMillis(2000))
              .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
                override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = {
                  element._2
                }
              })
            )
      .setParallelism(1).slotSharingGroup("red")


    /*
    WatermarkStrategy
              .forBoundedOutOfOrderness(Duration.ofMillis(3000))
              .createWatermarkGenerator()
              */

    val ds02: DataStream[(String, Long)] = env.socketTextStream("node01", 9999)
      .map(line => (line.split(",")(0), line.split(",")(1).toLong))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forGenerator(new WatermarkGeneratorSupplier[(String, Long)] {
          override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[(String, Long)] = {

            new WatermarkGenerator[(String, Long)]() {
              val per = 2000L
              var maxTimestamp = 0L

              override def onEvent(event: (String, Long), eventTimestamp: Long, output: WatermarkOutput): Unit = {
                //每条数据过来调用一次
                //                if (event._2 > maxTimestamp)
                //                  output.emitWatermark(new Watermark(maxTimestamp - per))

                maxTimestamp = Math.max(event._2, maxTimestamp)

              }

              override def onPeriodicEmit(output: WatermarkOutput): Unit = {
                output.emitWatermark(new Watermark(maxTimestamp - per))
              }
            }

          }
        })
      )
      .setParallelism(1).slotSharingGroup("green")

    //定义侧输出流
    val outTag = new OutputTag[String]("tag")


    val ds04 = ds01.connect(ds02).keyBy(a => a._1, b => b._1)
      .process(new CoProcessFunction[(String, Long), (String, Long), (String, String)] {

        var map1: MapState[String, (String, Long)] = _
        var map2: MapState[String, (String, Long)] = _


        override def open(parameters: Configuration): Unit = {

          val ttlConfig: StateTtlConfig = StateTtlConfig
            .newBuilder(Time.minutes(1)) //存活时间
            .cleanupFullSnapshot()
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) //不返回过期数据
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) //更新
            .build()

          val mysqlStat = new MapStateDescriptor("s1", classOf[String], classOf[(String, Long)])
          mysqlStat.enableTimeToLive(ttlConfig)
          map1 = getRuntimeContext.getMapState(mysqlStat)

          val ds1Stat = new MapStateDescriptor("s2", classOf[String], classOf[(String, Long)])
          ds1Stat.enableTimeToLive(ttlConfig)
          map2 = getRuntimeContext.getMapState(ds1Stat)

        }


        override def processElement1(value: (String, Long), ctx: CoProcessFunction[(String, Long), (String, Long), (String, String)]#Context, out: Collector[(String, String)]): Unit = {
          //注册定时触发器
          ctx.timerService().registerEventTimeTimer(value._2 + 1000 * 2)

          //          println("当前的事件事件" + ctx.timerService)

          val t1: (String, Long) = map1.get(value._1)
          //先做word count
          if (t1 == null)
            map1.put(value._1, value)
          else
            map1.put(value._1, (value._1, value._2 + t1._2))

          //关联数据

          val t2: (String, Long) = map2.get(value._1)

          if (t2 == null)
            ctx.output[String](outTag, value._1 + "未关联到数据。。。。。当前的数量:" + value._2)
          else
            out.collect(("关联到数据" + value._1, map1.get(value._1)._2 + "---" + t2._2))

        }

        override def processElement2(value: (String, Long), ctx: CoProcessFunction[(String, Long), (String, Long), (String, String)]#Context, out: Collector[(String, String)]): Unit

        = {

          println("当前的事件事件" + ctx.timerService().currentWatermark())
          val t1 = map2.get(value._1)
          //先做word count
          if (t1 == null)
            map2.put(value._1, value)
          else
            map2.put(value._1, (value._1, value._2 + t1._2))

          //关联数据

          val t2 = map1.get(value._1)

          if (t2 == null)
            ctx.output[String](outTag, value._1 + "未关联到数据。。。。。当前的数量:" + value._2)
          else
            out.collect(("关联到数据" + value._1, map2.get(value._1)._2 + "----" + t2._2))
        }


        //        //时间触发器
        override def onTimer(timestamp: Long, ctx: CoProcessFunction[(String, Long), (String, Long), (String, String)]#OnTimerContext, out: Collector[(String, String)]): Unit = {
          println("出发了定时器：" + "当前的watermaker" + ctx.timerService().currentWatermark() + ":当前的处理时间" + ctx.timerService().currentProcessingTime())
        }


      }

      ).slotSharingGroup("black")

    ds04.print("主流")

    ds04.getSideOutput(outTag).print("侧输出流")


    env.execute()


  }


}