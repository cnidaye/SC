package c4_windows;

import a0_sources.ItemSource;
import a0_sources.ItemSourceWithTime;
import a1_pojo.Item;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.time.Duration;

public class test1_event_time_window {

    @Test
    public void test0_tumbling() throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        DataStreamSource<Item> source = env.addSource(new ItemSourceWithTime());

        source
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Item>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner((a, b) -> a.checkTime))
                .keyBy(x -> x.name)
                .window(TumblingEventTimeWindows.of(Time.seconds(4)))
                .apply(new WindowFunction<Item, Tuple2<String,Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Item> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                        System.out.println(123);
                        int sum = 0;
                        for (Item item : input) {
                           sum += item.score;
                        }
                        out.collect(new Tuple2<>(s, sum));
                    }
                })
                .print();
        env.execute();
    }

    @Test
    public void test1_sliding() {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);
        DataStreamSource<Item> source = env.addSource(new ItemSource());
    }
}
