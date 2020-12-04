import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/11/26 14:03
 */


public class TestSqlCreat {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment tenv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(tenv);
        EnvironmentSettings build = EnvironmentSettings.newInstance().useAnyPlanner().inStreamingMode().build();
        StreamTableEnvironment.create(tenv,build);
        String ddl = "CREATE TABLE order (\n" +
                "  id BIGINT,\n" +
                "  commodity_id STRING,\n" +
                "  order_num INT,\n" +
                "  usercode STRING,\n" +
                "  status STRING,\n" +
                "  phone_num STRING,\n" +
                "  create_time TIMESTAMP(3),\n" +
                "  addr STRING,\n" +
                "  order_flag STRING,\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'orderflinktopic'\n" +
                "  'properties.bootstrap.servers' = '192.168.140.145:9092,192.168.140.146:9092,192.168.140.147:9092'\n" +
//                "  'properties.group.id' = '555555'\n" +
                "  'format' = 'json'\n" +
//                "  'scan.startup.mode' = '555555'\n" +
//                "  'scan.startup.specific-offsets' = '555555'\n" +
//                "  'scan.startup.timestamp-millis' = '555555'\n" +
//                "  'sink.partitioner' = '555555'\n" +
                ")";

        String query = "SELECT\n" +
                "  CAST(TUMBLE_START(create_time, INTERVAL '5' SECOND) AS STRING) window_start,\n" +
                "  COUNT(*) order_num,\n" +
                "  SUM(order_num) total_amount,\n" +
                "FROM order\n" +
                "GROUP BY TUMBLE(create_time, INTERVAL '5' SECOND)";
        Table table = streamTableEnvironment.sqlQuery(ddl);
        streamTableEnvironment.toAppendStream(table,Row.class).print();
        tenv.execute("test21");
    }
}
