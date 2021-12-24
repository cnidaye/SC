package c3_stateful;

import a1_pojo.Item;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/*
统计到同id的三个产品才计算平均质量
 */
public class Test3KeyedListState {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);

        DataStreamSource<String> source = env.socketTextStream("localhost", 1234);

        source
                .map(x -> {
            String[] s = x.split(" ");
            return new Item(s[0], Integer.valueOf(s[2]));
        })
                .keyBy(x -> x.name)
                .process(
                        new KeyedProcessFunction<String, Item, String>() {
                            ListState<Long> cache;
                            ValueState<Integer> cnt;
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ListStateDescriptor<Long> cache = new ListStateDescriptor<>("cache", TypeInformation.of(Long.class));
                                this.cache = getRuntimeContext().getListState(cache);
                                this.cnt = getRuntimeContext()
                                        .getState(new ValueStateDescriptor<Integer>("count", Integer.class));
                            }

                            @Override
                            public void processElement(Item value, KeyedProcessFunction<String, Item, String>.Context ctx, Collector<String> out) throws Exception {
                                if (cnt.value() == null) {
                                    cnt.update(1);
                                    cache.add((long) value.score);
                                }else {
                                    if (cnt.value() == 3) {
                                        long total = 0;
                                        for (Long num : cache.get()) {
                                            total += num;
                                        }
                                        out.collect(value.name +":::"+ total/cnt.value());
                                        cache.clear();
                                        cnt.clear();
                                    }else{
                                        cnt.update(cnt.value() + 1);
                                        cache.add((long)value.score);
                                    }
                                }
                            }
                        }
                );
        env.execute();
    }
}
