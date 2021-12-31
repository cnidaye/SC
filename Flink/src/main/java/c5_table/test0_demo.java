package c5_table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class test0_demo {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);
        StreamTableEnvironment table = StreamTableEnvironment.create(env);

        DataStreamSource<String> source = env.socketTextStream("localhost", 1234);

        Table stream = table.fromDataStream(source);

        table.createTemporaryView("InputTable", stream);
        Table table1 = table.sqlQuery("select * from InputTable");

        DataStream<Row> resultStream = table.toDataStream(table1);

        resultStream.print();
        env.execute();
    }
}
