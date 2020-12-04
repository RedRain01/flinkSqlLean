package flinkSql;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.util.Properties;

/**
 * @author ：why
 * note: 这是一个练习
 * 他的流程是：
 * 他的目的是：
 * @description：TODO
 * @date ：2020/11/28 18:16
 */


public class FlinkKafkaSounrce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        String name            = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir     = "/workspace/Configuration/flinkhive"; // a local path
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);
        // set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");
        String topic = "orderflinktopic";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","192.168.140.145:9092,192.168.140.146:9092,192.168.140.147:9092");
        prop.setProperty("group.id","flinkOrderRorPort");
        FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), prop);
        DataStreamSource<String> stringDataStreamSource = bsEnv.addSource(stringFlinkKafkaConsumer);
        stringDataStreamSource.print().setParallelism(1);
        bsEnv.execute("56465");

    }
}
