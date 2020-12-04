package flinkSql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
/**
 * @author ：why
 * note: 这是一个练习
 * 他的流程是：
 * 他的目的是：在hive中建表
 * @description：TODO
 * @date ：2020/11/28 18:16
 */

public class Sql001 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(bsEnv);


        //构造hive catalog
        String name = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = "/workspace/Configuration/flinkhive"; // a local path
        String version = "3.1.2";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tEnv.registerCatalog("myhive", hive);
        tEnv.useCatalog("myhive");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.useDatabase("myhive");

      //如果hive中已经存在了相应的表，则这段代码省略
		String hiveSql = "CREATE external TABLE fs_table111 (\n" +
		                 "  user_id STRING,\n" +
		                 "  order_amount DOUBLE" +
		                 ") partitioned by (dt string,h string,m string) " +
		                 "stored as ORC " +
		                 "TBLPROPERTIES (\n" +
		                 "  'partition.time-extractor.timestamp-pattern'='$dt $h:$m:00',\n" +
		                 "  'sink.partition-commit.delay'='0s',\n" +
		                 "  'sink.partition-commit.trigger'='partition-time',\n" +
		                 "  'sink.partition-commit.policy.kind'='metastore'" +
		                 ")";
		tEnv.executeSql(hiveSql);
    }

}