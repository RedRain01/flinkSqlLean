package util;

import model.OrderDw;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/11/28 21:55
 */


public class GetKafkaData {
    public static DataStream<String> getOrder(){
        //获取table环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String topic = "orderflinktopic";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","192.168.140.145:9092,192.168.140.146:9092,192.168.140.147:9092");
        prop.setProperty("group.id","flinkOrderRorPort");
        FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), prop);
        DataStreamSource<String> stringDataStreamSource = env.addSource(stringFlinkKafkaConsumer);
        //统计某个端口的出货量，出货金额
        DataStream<OrderDw>  dataStreamTuple3= stringDataStreamSource.map(new MapFunction<String, OrderDw>() {
            @Override
            public OrderDw map(String value) throws Exception {
                String[] orderSplit = value.split("\",\"");
                OrderDw tuple3 = new OrderDw();
                return null;
            }
        });
        return null;
    }
}
