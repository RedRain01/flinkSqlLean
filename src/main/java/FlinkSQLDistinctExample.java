import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Desc: flink sql distinct & count demo
 * Created by zhisheng on 2020-03-18 23:41
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class FlinkSQLDistinctExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment blinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        blinkStreamEnv.setParallelism(1);
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(blinkStreamEnv, blinkStreamSettings);


        String ddlSource = "CREATE TABLE user_behavior (\n" +
                "    id BIGINT,\n" +
                "    order_flag STRING,\n" +
                "    create_time TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka',\n" +
                "    'connector.version' = '0.11',\n" +
                "    'connector.topic' = 'orderflinktopic',\n" +
                "    'connector.startup-mode' = 'latest-offset',\n" +
                "    'connector.properties.zookeeper.connect' = '192.168.140.145:2181,192.168.140.146:2181,192.168.140.147:2181',\n" +
                "    'connector.properties.bootstrap.servers' = '192.168.140.145:9092,192.168.140.146:9092,192.168.140.147:9092',\n" +
                "    'format.type' = 'json'\n" +
                ")";
//        String ddlSource =
//                "CREATE TABLE order11 (\n" +
//                "  id BIGINT,\n" +
//                "  commodity_id STRING,\n" +
//                "  order_num INT,\n" +
//                "  usercode STRING,\n" +
//                "  status STRING,\n" +
//                "  phone_num STRING,\n" +
//                "  create_time TIMESTAMP(3),\n" +
//                "  addr STRING,\n" +
//                "  order_flag STRING,) WITH (" +
//                "    'connector.type' = 'kafka',\n" +
//                "    'connector.version' = '0.11',\n" +
//                "    'connector.topic' = 'orderflinktopic',\n" +
//                "    'connector.startup-mode' = 'latest-offset',\n" +
//                "    'connector.properties.zookeeper.connect' = '192.168.140.145:2181,192.168.140.146:2181,192.168.140.147:2181',\n" +
//                "    'connector.properties.bootstrap.servers' = '192.168.140.145:9092,192.168.140.146:9092,192.168.140.147:9092',\n" +
//                "    'format.type' = 'json'\n" +
//                ")";

        String countSql = "select id from user_behavior ";
        blinkStreamTableEnv.executeSql(ddlSource);
        Table countTable = blinkStreamTableEnv.sqlQuery(countSql);
        blinkStreamTableEnv.toRetractStream(countTable, Row.class).print();

//        String distinctSql = "select distinct(id) from user_behavior";
//        Table distinctTable = blinkStreamTableEnv.sqlQuery(distinctSql);
        blinkStreamTableEnv.toRetractStream(countTable, Row.class).print("==");

        blinkStreamTableEnv.execute("Blink Stream SQL count/distinct demo");
    }
}
