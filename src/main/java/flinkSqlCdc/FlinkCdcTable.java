package flinkSqlCdc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

/**
 * @author ：why
 * note: 这是一个练习
 * 他的流程是：kafka -->kafkaTable-->hiveTable-->hive
 * 他的目的是：不使用DataStream API  使用table实现kafka to hive  结果没有实现，hiveSql 与flinkSQL有冲突
 * @description：TODO
 * @date ：2020/11/28 18:16
 */


public class FlinkCdcTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, bsSettings);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(10000);
      //  bsTableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
                String ddlSource = "CREATE TABLE mysqlT001 (\n" +
                "    id BIGINT,\n" +
                "    commodity_id STRING,\n" +
                "    order_num INT,\n" +
                "    usercode STRING,\n" +
                "    status STRING,\n" +
                "    amount DOUBLE,\n" +
                "    phone_num STRING,\n" +
                "    create_time TIMESTAMP(3),\n" +
                        "    addr STRING,\n" +
                "    order_flag STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                 "    'hostname' = '192.168.140.146',\n" +
                 "    'port' = '3306',\n" +
                 "    'username' = 'root',\n" +
                  "    'password' = 'WHYwhy@@@123',\n" +
                  "    'database-name' = 'whyt',\n" +
                  "    'table-name' = 'order'\n" +
                ")";

        bsTableEnv.sqlUpdate(ddlSource);
        //hive catalog
        String name = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = "/workspace/Configuration/flinkhive"; // a local path
        String version = "3.1.2";
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        bsTableEnv.registerCatalog("myhive", hive);
        bsTableEnv.useCatalog("myhive");
        bsTableEnv.useDatabase("myhive");
        bsTableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        String queryDdl = " SELECT id,commodity_id,order_num,usercode,status,amount,phone_num,create_time," +
                "addr,order_flag FROM mysqlT001";
        Table table = bsTableEnv.sqlQuery(queryDdl);
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = bsTableEnv.toRetractStream(table, Row.class);
        tuple2DataStream.print().setParallelism(1);
        env.execute("090");


//
//
//        String insertSql = "insert into  order118 SELECT id,commodity_id,order_num,usercode,status,amount,phone_num,create_time," +
//                "addr,order_flag,DATE_FORMAT(create_time, 'yyyy-MM-dd'), DATE_FORMAT(create_time, 'HH'), DATE_FORMAT(create_time, 'mm')" +
//                " FROM mysqlT001";
//        bsTableEnv.executeSql(insertSql);




    }

}


