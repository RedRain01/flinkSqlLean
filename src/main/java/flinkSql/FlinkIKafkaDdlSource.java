package flinkSql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author ：why
 * note：这是一个练习
 * 他的流程是kafka -->kafkaTable -->DataStream
 * 他的目的是kafka到kafkaTable的验证
 * @description：TODO
 * @date ：2020/11/29 9:36
 */


public class FlinkIKafkaDdlSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, bsSettings);
        String ddlSource = "CREATE TABLE user_behavior (\n" +
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
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'orderflinktopic',\n" +
                "    'properties.bootstrap.servers' = '192.168.140.145:9092,192.168.140.146:9092,192.168.140.147:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'format' = 'csv',\n" +
                "    'scan.startup.mode' = 'earliest-offset'\n" +
                ")";
        bsTableEnv.sqlUpdate(ddlSource);
        String queryDdl="SELECT usercode,order_flag FROM user_behavior";
//        table.printSchema();
        Table table = bsTableEnv.sqlQuery(queryDdl);
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = bsTableEnv.toRetractStream(table, Row.class);
        tuple2DataStream.print().setParallelism(1);
        env.execute("090");
//        bsTableEnv.sqlUpdate(ddlSource);
    }

}
