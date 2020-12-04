package flinkSqlCdc;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/12/4 15:10
 */


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import
        org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import
        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.Duration;
import java.util.Properties;

public class MysqlCDC2Hive {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv =
                StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        streamEnv.setParallelism(3);
        streamEnv.enableCheckpointing(60000);

        EnvironmentSettings tableEnvSettings =
                EnvironmentSettings.newInstance()
                        .useBlinkPlanner()
                        .inStreamingMode()
                        .build();
        StreamTableEnvironment tableEnv =
                StreamTableEnvironment.create(streamEnv, tableEnvSettings);

        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE,
                CheckpointingMode.EXACTLY_ONCE);

        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,
                Duration.ofMinutes(1));

        String catalogName = "hive_catalog";
        HiveCatalog catalog = new HiveCatalog(
                catalogName,
                "default",
                "/Users/chenshuai/dev/apache-hive-2.3.4-bin/conf",
                "2.3.4"
        );
        tableEnv.registerCatalog(catalogName, catalog);
        tableEnv.useCatalog(catalogName);

//        MyDateFormat2 myDateFormat = new MyDateFormat2();
//        tableEnv.registerFunction("my_date_format", myDateFormat);

        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS cdc");
        tableEnv.executeSql("DROP TABLE IF EXISTS cdc.team");
        tableEnv.executeSql("CREATE TABLE cdc.team(\n" +
                "    team_id INT,\n" +
                "    team_name STRING,\n" +
                "    create_time TIMESTAMP,\n" +
                "    update_time TIMESTAMP,\n" +
                "    proctime as proctime()\n" +
                ") WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = 'localhost',\n" +
                "  'port' = '3306',\n" +
                "  'username' = 'root',\n" +
                "  'password' = 'root',\n" +
                "  'database-name' = 'test',\n" +
                "  'table-name' = 'team'\n" +
                ")");

        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS kafka");
        tableEnv.executeSql("DROP TABLE IF EXISTS kafka.team");
        tableEnv.executeSql("CREATE TABLE kafka.team (\n" +
                "  team_id INT,\n" +
                "  team_name STRING,\n" +
                "  create_time TIMESTAMP,\n" +
                "  update_time TIMESTAMP\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'team',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'format' = 'changelog-json'\n" +
                ")");

        tableEnv.executeSql("INSERT INTO kafka.team \n" +
                "SELECT team_id, team_name, create_time, update_time \n" +
                "FROM cdc.team");

        // 定义带op字段的stream
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumerBase<String> consumer = new FlinkKafkaConsumer<>(
                "team",
                new SimpleStringSchema(),
                properties
        ).setStartFromEarliest();

        DataStream<String> ds = streamEnv.addSource(consumer);

        String[] fieldNames = {"team_id", "team_name", "create_time","update_time", "op"};
        TypeInformation[] types = {Types.INT, Types.STRING, Types.STRING,
                Types.STRING, Types.STRING};
        DataStream<Row> ds2 = ds.map(str -> {
            JSONObject jsonObject = JSON.parseObject(str);
            String op = jsonObject.getString("op");
            JSONObject data = jsonObject.getJSONObject("data");
            int arity = fieldNames.length;
            Row row = new Row(arity);
            row.setField(0, data.get("team_id"));
            row.setField(1, data.get("team_name"));
            row.setField(2, data.get("create_time"));
            row.setField(3, data.get("update_time"));
            String operation = getOperation(op);
            row.setField(4, operation);

            return row;
        }, new RowTypeInfo(types, fieldNames));

        tableEnv.registerDataStream("merged_team", ds2);

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS ods");
        tableEnv.executeSql("DROP TABLE IF EXISTS ods.team");

        tableEnv.executeSql("CREATE TABLE ods.team (\n" +
                "  team_id INT,\n" +
                "  team_name STRING,\n" +
                "  create_time STRING,\n" +
                "  update_time STRING,\n" +
                "  op STRING\n" +
                ") PARTITIONED BY (\n" +
                "    ts_date STRING,\n" +
                "    ts_hour STRING,\n" +
                "    ts_minute STRING\n" +
                ") STORED AS PARQUET TBLPROPERTIES (\n" +
                "  'sink.partition-commit.trigger' = 'partition-time',\n" +
                "  'sink.partition-commit.delay' = '1 min',\n" +
                "  'sink.partition-commit.policy.kind' ='metastore,success-file',\n" +
        "  'partition.time-extractor.timestamp-pattern' = '$ts_date$ts_hour:$ts_minute:00'\n" +
        ")");

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql("INSERT INTO ods.team \n" +
                        "SELECT team_id, team_name, create_time, update_time, op\n" +
                " DATE_FORMAT(TO_TIMESTAMP(create_time, 'yyyy-MM-ddHH:mm:ss'), 'yyyyMMdd') as ts_date, \n" +
        " DATE_FORMAT(TO_TIMESTAMP(create_time, 'yyyy-MM-ddHH:mm:ss'), 'HH') as ts_hour, \n" +
        " DATE_FORMAT(TO_TIMESTAMP(create_time, 'yyyy-MM-ddHH:mm:ss'), 'mm') as ts_minute \n" +
        "FROM merged_team");
        tableEnv.execute("MysqlCDC2Hive2");

        streamEnv.execute("");
    }

    private static String getOperation(String op) {
        String operation = "INSERT";
        for (RowKind rk : RowKind.values()) {
            if (rk.shortString().equals(op)) {
                switch (rk) {
                    case UPDATE_BEFORE:
                    case UPDATE_AFTER:
                        operation = "UPDATE";
                        break;
                    case DELETE:
                        operation = "DELETE";
                        break;
                    case INSERT:
                    default:
                        operation = "INSERT";
                        break;
                }
                break;
            }
        }
        return operation;
    }
}
