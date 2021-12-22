package c0_demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.List;
/*
 暂时忽略， withBroadCastSet 适用于 DataSet操作
 */
public class BroadCastDemo {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);
        HashSet<String> strings = new HashSet<>();
        strings.add("Hello");
        strings.add("Bye");
        DataStreamSource<String> banned = env.fromCollection(strings);

        env.socketTextStream("localhost", 1234)
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        String[] s = value.split(" ");
                        for (String s1 : s) {
                            out.collect(s1);
                        }
                    }
                }).map(
                        new RichMapFunction<String, Tuple2<String, Integer>>() {
                            HashSet<String> banned;

                            @Override
                            public Tuple2<String, Integer> map(String value) throws Exception {
                                if (!banned.contains(value)) {
                                    return new Tuple2<>(value, 1);
                                }
                                return null;
                            }

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                List<String> banned = getRuntimeContext().getBroadcastVariable("banned");
                                this.banned = new HashSet<>(banned);
                            }
                        }
                )
                .keyBy(0)
                .sum(1);

        env.execute();

    }
}























