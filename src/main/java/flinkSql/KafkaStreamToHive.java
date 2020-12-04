package flinkSql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.Properties;


/**
 * @author ：why
 * note：这是一个练习 \
 * 他的流程是：kafka -->flinkDataStream -->HiveTable-->hive
 * 他的目的是：练习kafkaTable to hive
 * @description：TODO
 * @date ：2020/11/29 9:36
 */


public class KafkaStreamToHive {
    public static void main(String[] args){
        // EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        //TableEnvironment tEnv = TableEnvironment.create(settings);
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        bsEnv.enableCheckpointing(10000);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(bsEnv);

        String topic = "orderflinktopic";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","192.168.140.145:9092,192.168.140.146:9092,192.168.140.147:9092");
        prop.setProperty("group.id","flinkOrderRorPort");
        FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), prop);
        DataStreamSource<String> stringDataStreamSource = bsEnv.addSource(stringFlinkKafkaConsumer);
        DataStream<Order>  dataStreamOrder= stringDataStreamSource.map(new MapFunction<String, Order>() {
            @Override
            public Order map(String value) throws Exception {
                String[] orderSplit = value.split("\",\"");
                Order order=new Order();
                order.setId(orderSplit[0].substring(1,orderSplit[0].length()));
                order.setCommodity_id(orderSplit[1]);
                order.setOrder_num(orderSplit[2]);
                order.setUsercode(orderSplit[3]);
                order.setStatus(orderSplit[4]);
                order.setAmount(orderSplit[5]);
                order.setPhone_num(orderSplit[6]);
                order.setCreate_time(Timestamp.valueOf(orderSplit[7]));
                order.setAddr(orderSplit[8]);
                order.setOrder_flag(orderSplit[9].substring(0,orderSplit[9].length()-1));
                order.setCreate_time(new Timestamp(System.currentTimeMillis()));
                return order;
            }
        });

        DataStream<Order>  dataStreamOrder001 = dataStreamOrder.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Order>() {
            long water = 0l;
            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(
                    Order lastElement,
                    long extractedTimestamp) {
                return new Watermark(water);
            }
            @Override
            public long extractTimestamp(
                    Order element,
                    long recordTimestamp) {
                water = element.getCreate_time().getTime();
                return water;
            }
        });
//
//        //构造hive catalog
        String name = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = "/workspace/Configuration/flinkhive"; // a local path
        String version = "3.1.2";
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tEnv.registerCatalog("myhive", hive);
        tEnv.useCatalog("myhive");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.useDatabase("myhive");
        Table table = tEnv.fromDataStream(dataStreamOrder);
        table.printSchema();
        tEnv.createTemporaryView("orderView",dataStreamOrder001);
        String insertSql = "insert into  order111 SELECT id,commodity_id,order_num,usercode,status,amount,phone_num,create_time,addr,order_flag,DATE_FORMAT(create_time, 'yyyy-MM-dd'), DATE_FORMAT(create_time, 'HH'), DATE_FORMAT(create_time, 'mm') FROM orderView";
        tEnv.executeSql(insertSql);

    }
    public static class Order implements java.io.Serializable{
        private String id;
        private String commodity_id;
        private String order_num;
        private String usercode;
        private String status;
        private String amount;
        private String phone_num;
        private Timestamp create_time;
        private String addr;
        private String order_flag;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getCommodity_id() {
            return commodity_id;
        }

        public void setCommodity_id(String commodity_id) {
            this.commodity_id = commodity_id;
        }

        public String getOrder_num() {
            return order_num;
        }

        public void setOrder_num(String order_num) {
            this.order_num = order_num;
        }

        public String getUsercode() {
            return usercode;
        }

        public void setUsercode(String usercode) {
            this.usercode = usercode;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getAmount() {
            return amount;
        }

        public void setAmount(String amount) {
            this.amount = amount;
        }

        public String getPhone_num() {
            return phone_num;
        }

        public void setPhone_num(String phone_num) {
            this.phone_num = phone_num;
        }

        public Timestamp getCreate_time() {
            return create_time;
        }

        public void setCreate_time(Timestamp create_time) {
            this.create_time = create_time;
        }

        public String getAddr() {
            return addr;
        }

        public void setAddr(String addr) {
            this.addr = addr;
        }

        public String getOrder_flag() {
            return order_flag;
        }

        public void setOrder_flag(String order_flag) {
            this.order_flag = order_flag;
        }
    }


}
