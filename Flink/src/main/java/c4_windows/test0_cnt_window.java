package c4_windows;

import a1_pojo.Item;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

public class test0_cnt_window {
    @Test
    public void test0_tumbling_reduce( ) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 1234);
        source.map(x -> {
            String[] s = x.split(" ");
            return new Item(s[0], Integer.valueOf(s[1]));
        })
                .keyBy(x -> x.name)
                .countWindow(3)
                .reduce(new ReduceFunction<Item>() {
                    @Override
                    public Item reduce(Item value1, Item value2) throws Exception {
                        return new Item(value1.name, value1.score + value2.score);
                    }
                })
//                .map()
                .print();
        env.execute();
    }

    @Test
    public void test1_sliding_reduce() throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);
        DataStreamSource<String> source = env.socketTextStream("localhost", 1234);

        source
                .map(x -> {
            String[] s = x.split(" ");
            return new Item(s[0], Integer.valueOf(s[1]));
        })
                .keyBy(x -> x.name)
                .countWindow(4, 2)
                .reduce(new ReduceFunction<Item>() {
                    @Override
                    public Item reduce(Item value1, Item value2) throws Exception {
                        return new Item(value1.name, value1.score + value2.score);
                    }
                })
                .print();

        env.execute();

    }
}
