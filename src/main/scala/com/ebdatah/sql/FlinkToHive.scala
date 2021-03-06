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
  //-- ?????? partition ???????????????????????? watermark ?????? partiton commit ?????????
  //-- ??????hour?????????partition?????????????????????????????????dt?????????yyyy-MM-dd???????????????hour ??? 0-23 ????????????timestamp-pattern ??????????????????????????? partition ????????????????????? timestamp
  //-- ?????? dalay ?????????????????? watermark > partition ?????? + 1 ???????????? commit ?????? partition
  //-- partitiion commit ???????????????????????? metastore(addPartition)????????? SUCCESS ??????

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
    //??????checkpoint


    env.setStateBackend(new FsStateBackend("hdfs://node01:8020/conf/checkpoint"))
    env.enableCheckpointing(60000*60, CheckpointingMode.EXACTLY_ONCE)

    val checkpointConfig: CheckpointConfig = env.getCheckpointConfig

    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //    checkpointConfig.setCheckpointInterval(5000)
    //??????10?????????????????????
    checkpointConfig.setCheckpointTimeout(60000)
    checkpointConfig.setMaxConcurrentCheckpoints(1)
    checkpointConfig.setMinPauseBetweenCheckpoints(5000)
    // ???????????????
    // ?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
    checkpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //// ?????????????????????checkpoint?????????
    checkpointConfig.setTolerableCheckpointFailureNumber(0)

    val envSetting: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    //??????table??????
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, envSetting)
    //??????hive
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
