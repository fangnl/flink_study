package com.ebdatah.sql

import java.sql.Timestamp
import java.time.Duration

import apple.laf.JRSUIState.ValueState
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, TimestampAssigner, TimestampAssignerSupplier, WatermarkStrategy}
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows

import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.util.Collector


object TestJoin {


  val KAFKA_TABLE_SOURCE_DDL: String =
    """
      |CREATE TABLE user_behavior(
      |  name STRING,
      |  id INT,
      |  aptime STRING,
      | `offset` BIGINT METADATA VIRTUAL,
      | `rowtime` TIMESTAMP(3) METADATA FROM 'timestamp',
      | --`headers`	MAP<STRING, BYTES> METADATA VIRTUAL,
      | --`partition` BIGINT METADATA VIRTUAL,
      | --ts as TO_TIMESTAMP(FROM_UNIXTIME(`rowtime` / 1000, 'yyyy-MM-dd HH:mm:ss')),
      | WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND
      |
      |)with(
      |  'connector'='kafka',
      |  'topic'='flink',
      |  'properties.group.id'='flink4',
      |  'scan.startup.mode' = 'latest-offset',
      |  'properties.bootstrap.servers' = 'node05:9092,node03:9092,node04:9092',
      |  'format' = 'json',
      |  'is_generic'='true',
      |   'json.ignore-parse-errors' = 'true'
      |)
      |""".stripMargin

  val MYSQL_CDC_TABLE =
    """
      |create table testflink (
      |  id int,
      |  name STRING,
      |  proctime as proctime()
      |) with (
      | 'connector' = 'mysql-cdc',
      | 'hostname' = 'localhost',
      | 'port' = '3306',
      | 'username' = 'root',
      | 'password' = '12345678',
      | 'database-name' = 'traffic',
      | 'table-name' = 'testflink',
      | 'server-time-zone' = 'Asia/Shanghai'
      |)
    """.stripMargin

  val MYSQL_DDL_SINK_TABLE: String =
    """
      |create table if not exists sinkMysqlTable (
      |  `name` String,
      |  `id` BIGINT,
      |   PRIMARY KEY (`name`) NOT ENFORCED
      |) with (
      | 'connector' = 'jdbc',
      | 'driver'='com.mysql.cj.jdbc.Driver',
      | 'url'='jdbc:mysql://172.20.10.6:3306/traffic',
      | 'username' = 'root',
      | 'password' = '12345678',
      | 'table-name' = 'test_flink_sink',
      | 'is_generic'='true'
      |)
      |""".stripMargin

  //-- 使用 partition 中抽取时间，加上 watermark 决定 partiton commit 的时机
  //-- 配置hour级别的partition时间抽取策略这个例子中dt字段是yyyy-MM-dd格式的天，hour 是 0-23 的小时，timestamp-pattern 定义了如何从这两个 partition 字段推出完整的 timestamp
  //-- 配置 dalay 为小时级，当 watermark > partition 时间 + 1 小时，会 commit 这个 partition
  //-- partitiion commit 的策略是：先更新 metastore(addPartition)，再写 SUCCESS 文件
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //开启checkpoint
    val envSetting: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    //设置table环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, envSetting)

    import org.apache.flink.api.scala._
    tableEnv.executeSql(MYSQL_CDC_TABLE)


    val table: Table = tableEnv.sqlQuery(
      """
        |select id,name,proctime from testflink
        |""".stripMargin)

    val ds2: DataStream[(Int, String)] = env.socketTextStream("node01", 9999).map(line => (line.split(",")(0).toInt, line.split(",")(1)))


    val ds1: DataStream[(Boolean, (Int, String, Timestamp))] = tableEnv.toRetractStream[(Int, String, Timestamp)](table)


    import org.apache.flink.api.common.time.Time;
    //intervalJoin
    //    ds1.keyBy(_._1).intervalJoin(ds2.keyBy(_._1)).between(Time.days(1),Time.days(-1)).

    ds1.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(1000))
      .withTimestampAssigner(
        new SerializableTimestampAssigner[(Boolean, (Int, String, Timestamp))] {
          override def extractTimestamp(element: (Boolean, (Int, String, Timestamp)), recordTimestamp: Long): Long = {
            0L
          }
        })

    )

    //GlobalWindows.create()
    //TumblingEventTimeWindows.of(Time.minutes(2))

    //re join


    ds1.join(ds2).where(_._2._1).equalTo(_._1).window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.days(1))).apply(
      new JoinFunction[(Boolean, (Int, String, Timestamp)), (Int, String), String]() {
        override def join(first: (Boolean, (Int, String, Timestamp)), second: (Int, String)): String = {
          ""
        }
      }
    )






    //        ds1.join(ds2).where(_._2._1).equalTo(_._1).window()

    ds2.keyBy(_._1).connect(ds1.keyBy(_._2._1)).process(new CoProcessFunction[(Int, String), (Boolean, (Int, String, Timestamp)), (Int, String, Timestamp)] {
      var map1: MapState[Int, (String, Timestamp)] = _
      var map2: MapState[Int, String] = _

      override def open(parameters: Configuration): Unit = {
        val ttlConfig: StateTtlConfig = StateTtlConfig
          .newBuilder(Time.minutes(1)) //存活时间
          .cleanupFullSnapshot()
          .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) //不返回过期数据
          .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) //更新
          .build()

        val mysqlStat = new MapStateDescriptor[Int, (String, Timestamp)]("mysqlStat", classOf[Int], classOf[(String, Timestamp)])
        mysqlStat.enableTimeToLive(ttlConfig)
        map1 = getRuntimeContext.getMapState(mysqlStat)

        val ds1Stat = new MapStateDescriptor[Int, String]("mysqlStat2", classOf[Int], classOf[String])
        ds1Stat.enableTimeToLive(ttlConfig)
        map2 = getRuntimeContext.getMapState(ds1Stat)

      }

      override def processElement1(value: (Int, String),
                                   ctx: CoProcessFunction[(Int, String), (Boolean, (Int, String, Timestamp)), (Int, String, Timestamp)]#Context,
                                   out: Collector[(Int, String, Timestamp)]): Unit = {
        //事实表数据
        val tuple: (String, Timestamp) = map1.get(value._1)
        println(tuple)
        if (tuple == null) {
          map2.put(value._1, value._2)
        } else {
          out.collect((value._1, tuple._1, tuple._2))
        }


      }

      override def processElement2(value: (Boolean, (Int, String, Timestamp)),
                                   ctx: CoProcessFunction[(Int, String),
                                     (Boolean, (Int, String, Timestamp)),
                                     (Int, String, Timestamp)]#Context,
                                   out: Collector[(Int, String, Timestamp)]): Unit = {
        //mysql数据
        val str: String = map2.get(value._2._1)
        println(str)
        if (str == null) {
          map1.put(value._2._1, (value._2._2, value._2._3))
        } else {
          out.collect((value._2._1, value._2._2, value._2._3))
        }

      }
    }
    ).print()


    //    ds1.coGroup(ds2)
    //      .where((key: (Boolean, (Int, String, Timestamp))) =>key._2._2)
    //        .equalTo((key: (Int, String)) =>key._2)
    //      .window(TumblingEventTimeWindows)


    env.execute()


  }


}
