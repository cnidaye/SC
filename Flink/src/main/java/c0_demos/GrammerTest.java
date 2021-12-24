package c0_demos;

import a1_pojo.Item;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class GrammerTest {
    @Test
    public void test0() throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);

        DataStreamSource<String> localhost = env.socketTextStream("localhost", 1234);
        localhost.map(x -> {
            String[] split = x.split(",");
            return new Item(split[0], Integer.valueOf(split[1]));
        }).keyBy(x -> x.name)
                .sum("score")
                .map(x -> x.name +":::" +x.score)
                .print();

        env.execute();
    }

    @Test
    public void test1() throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);

        env.socketTextStream("localhost", 1234)
                .process(
                        new ProcessFunction<String, String>() {
                            @Override
                            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                                System.out.println(value);
                            }
                        }
                );

        env.execute();
    }


}
