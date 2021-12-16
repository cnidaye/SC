package c1_time;

import a0_sources.IntSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class Test0EventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);
        DataStreamSource<Tuple2<Integer, Long>> source = env.addSource(new IntSource()).setParallelism(1);

        WatermarkStrategy<Tuple2<Integer, Long>> strategy = WatermarkStrategy.<Tuple2<Integer, Long>>forBoundedOutOfOrderness(Duration.ofMillis(1500))
                .withTimestampAssigner(
                        (a, b) -> a.f1
                );

        SingleOutputStreamOperator<Tuple2<Integer, Long>> src = source.assignTimestampsAndWatermarks(strategy);

        src.filter(x->true).print();


        env.execute();

    }
}
