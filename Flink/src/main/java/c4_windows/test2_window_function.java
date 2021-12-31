package c4_windows;

import a0_sources.ItemSourceWithTime;
import a1_pojo.Item;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

public class test2_window_function {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env
                .addSource(new ItemSourceWithTime())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Item>forMonotonousTimestamps().withTimestampAssigner((a, b) -> a.checkTime))
                .keyBy(x -> x.name)
                .window(TumblingEventTimeWindows.of(Time.seconds(4)))
                .apply(
                        new WindowFunction<Item, Tuple2<String,Integer>, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow window, Iterable<Item> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                                int cnt = 0;
                                int total = 0;
                                for (Item item : input) {
                                    cnt += 1;
                                    total += item.score;
                                }
                                out.collect(Tuple2.of(s, total/cnt));
                            }
                        }
                )
                .print();
        env.execute();
    }
}
