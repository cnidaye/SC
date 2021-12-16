package c1_time;

import a0_sources.IntSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class Test1EventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);
        DataStreamSource<Tuple2<Integer, Long>> source = env.addSource(new IntSource()).setParallelism(1);

        WatermarkStrategy<Tuple2<Integer, Long>> strategy = WatermarkStrategy.<Tuple2<Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(
                        (a, b) -> a.f1
                );

        SingleOutputStreamOperator<Tuple2<Integer, Long>> src = source.assignTimestampsAndWatermarks(strategy);

        src.windowAll(TumblingEventTimeWindows.of(Time.seconds(4)))
                        .apply(new AllWindowFunction<Tuple2<Integer, Long>, String, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<Tuple2<Integer, Long>> values, Collector<String> out) throws Exception {
                                String s = "";
                                for (Tuple2<Integer, Long> value : values) {
                                    s += String.valueOf(value);
                                    s += ",";
                                }
                                out.collect(s);
                            }
                        })
                                .print();

        env.execute();

    }
}
