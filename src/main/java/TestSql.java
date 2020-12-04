//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
//import org.apache.flink.types.Row;
//
//import static org.apache.flink.table.api.Expressions.$;
//
///**
// * @author ：why
// * @description：TODO
// * @date ：2020/11/26 14:03
// */
//
//
//public class TestSql {
//    public static void main(String[] args) throws Exception {
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment tenv = BatchTableEnvironment.create(env);
//        Table orders = tenv.from("Orders");
//        Table count = orders.groupBy($("a"))
//                .select($("a"), $("b").count().as("cnt"));
//        DataSet<Row> rowDataSet = tenv.toDataSet(count, Row.class);
//        rowDataSet.print();
//
//    }
//}
