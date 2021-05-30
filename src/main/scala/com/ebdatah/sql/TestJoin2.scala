package com.ebdatah.sql

import java.sql.Timestamp
import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.util.Collector

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._


object TestJoin2 {


  val KAFKA_TABLE_SOURCE_DDL: String =
    """
      |CREATE TABLE order_info(
      |  orderName STRING,
      |  id INT,
      | `offset` BIGINT METADATA VIRTUAL,
      | `rowtime` TIMESTAMP(3) METADATA FROM 'timestamp',
      | --`headers`	MAP<STRING, BYTES> METADATA VIRTUAL,
      | --`partition` BIGINT METADATA VIRTUAL,
      | --ts as TO_TIMESTAMP(FROM_UNIXTIME(`rowtime` / 1000, 'yyyy-MM-dd HH:mm:ss')),
      | WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND
      |
      |)with(
      |  'connector'='kafka',
      |  'topic'='flink2',
      |  'properties.group.id'='flink4',
      |  'scan.startup.mode' = 'earliest-offset',
      |  'properties.bootstrap.servers' = 'node05:9092,node03:9092,node04:9092',
      |  'format' = 'json',
      |  -- 'is_generic'='true'
      | 'json.ignore-parse-errors' = 'true'
      |)
      |""".stripMargin

  val MYSQL_CDC_TABLE =
    """
      |create table user_info (
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
    tableEnv.executeSql(KAFKA_TABLE_SOURCE_DDL)

//    StateTtlConfig.newBuilder(Time.milliseconds(1000))



    //waterMark对齐
    val ds2: DataStream[(Int, Int)] = env.socketTextStream("node01", 9999).map((line: String) => (line.split(",")(0).toInt, line.split(",")(1).toInt))


    tableEnv.createTemporaryView("be_table", ds2, $"userId", $"orderId")


    val table: Table = tableEnv.sqlQuery(
      """
        |select
        |   userId,
        |   orderId,
        |   name,
        |   orderName
        |from be_table b
        |join
        |user_info t on b.userId=t.id
        |join
        |order_info o on b.orderId=o.id
        |
        |""".stripMargin)

    tableEnv.toRetractStream[(Int, Int, String, String)](table).print()


    env.execute()


  }


}
