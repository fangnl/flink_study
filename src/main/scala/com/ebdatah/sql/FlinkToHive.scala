package com.ebdatah.sql

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{SqlDialect, Table}
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.util.Collector


object FlinkToHive {


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
      |  'scan.startup.mode' = 'earliest-offset',
      |  'properties.bootstrap.servers' = 'node05:9092,node03:9092,node04:9092',
      |  'format' = 'json',
      |  'is_generic'='true',
      |   'json.ignore-parse-errors' = 'true'
      |)
      |""".stripMargin
  val MYSQL_DDL_SINK_TABLE: String =
    """
      |create table if not exists sinkMysqlTable (
      |  `name` String,
      |   `id` BIGINT,
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

  val hive_table_test =
    """
      |create table hive_table(
      |name string,
      |id int
      |)partitioned by(dt string ,tim String )
      |tblproperties(
      |'sink.partition-commit.trigger'='partition-time',
      |'partition.time-extractor.timestamp-pattern'='$dt $tim:00:00',
      |'sink.partition-commit.delay'='1 h',
      |'sink.partition-commit.policy.kind' = 'metastore,success-file',
      |'auto-compaction' = 'true',
      |'compaction.file-size'='128KB'
      |)
      |""".stripMargin

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //开启checkpoint


    env.setStateBackend(new FsStateBackend("hdfs://node01:8020/conf/checkpoint"))
    env.enableCheckpointing(60000*60, CheckpointingMode.EXACTLY_ONCE)

    val checkpointConfig: CheckpointConfig = env.getCheckpointConfig

    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //    checkpointConfig.setCheckpointInterval(5000)
    //默认10分钟的时间间隔
    checkpointConfig.setCheckpointTimeout(60000)
    checkpointConfig.setMaxConcurrentCheckpoints(1)
    checkpointConfig.setMinPauseBetweenCheckpoints(5000)
    // 外部检查点
    // 不会在任务正常停止的过程中清理掉检查点数据，而是会一直保存在外部系统介质中，另外也可以通过从外部检查点中对任务进行恢复
    checkpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //// 设置可以允许的checkpoint失败数
    checkpointConfig.setTolerableCheckpointFailureNumber(0)

    val envSetting: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    //设置table环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, envSetting)
    //连接hive
    val name = "myhive"
    val defaultDatabase = "default"
    val hiveConfDir = "hdfs://node01:8020/conf"
    //    val hiveConfDir = "src/main/resources"
    val hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir)
    tableEnv.registerCatalog("myhive", hiveCatalog)
    tableEnv.useCatalog("myhive")
    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
    tableEnv.executeSql(
      """
        |drop table IF exists hive_table
        |""".stripMargin)
    tableEnv.executeSql(hive_table_test)
    tableEnv.getConfig.setSqlDialect(SqlDialect.DEFAULT)
    //    tableEnv.executeSql(
    //      """
    //        |drop table IF exists sinkMysqlTable
    //        |""".stripMargin)
    //    tableEnv.executeSql(MYSQL_DDL_SINK_TABLE)
    tableEnv.executeSql(
      """
        |drop table IF exists user_behavior
        |""".stripMargin)
    tableEnv.executeSql(KAFKA_TABLE_SOURCE_DDL)
//    val table: Table = tableEnv.sqlQuery(
//      """
//        |select
//        |name,id,aptime
//        |from user_behavior
//        |""".stripMargin)

    tableEnv.executeSql(
      """
        |insert into  hive_table
        |select name ,id ,DATE_FORMAT(rowtime ,'yyyy-MM-dd'),DATE_FORMAT(rowtime ,'HH')
        |from user_behavior
        |""".stripMargin)


//    val ds1: DataStream[(String, Long, String)] = tableEnv.toAppendStream[(String, Long, String)](table)

//    ds1.print()
    //    val ds2: KeyedStream[String, Char] = env.readTextFile("").keyBy(_.charAt(1))
    //    env.readTextFile("").keyBy(_.charAt(1)).intervalJoin(ds2)
    //      .between(Time.hours(-1),Time.hours(1))
    //        .process(new ProcessJoinFunction[String,String,String] {
    //
    //
    //          override def processElement(left: String, right: String, ctx: ProcessJoinFunction[String, String, String]#Context, out: Collector[String]): Unit = ???
    //        })

//    env.execute()


  }


}
