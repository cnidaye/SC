package c3_stateful;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/*
https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/datastream/fault-tolerance/state/#%E7%8A%B6%E6%80%81%E6%9C%89%E6%95%88%E6%9C%9F-ttl
 */
public class Test4TTL {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(4);

        StateTtlConfig ttlConfig = StateTtlConfig
                //数据的有效期
                .newBuilder(Time.seconds(2))
                //更新策略
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                /*
                过期但还未清理的状态的可见性
                - NeverReturnExpired 不返回过期数据
                - ReturnExpiredIfNotCleanedUp 会返回过期但未清理的数据
                 */
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ListStateDescriptor<Integer> cacheState = new ListStateDescriptor<Integer>("cache",
                Integer.class);
        cacheState.enableTimeToLive(ttlConfig);


        env
                .addSource(new SourceFunction<Tuple2<Integer,Long>>() {
            int i = 0;
            @Override
            public void run(SourceContext<Tuple2<Integer, Long>> ctx) throws Exception {
                while (true) {
                    ctx.collect(Tuple2.of(i,System.currentTimeMillis()));
                    i++;
                    Thread.sleep(1000);
                    if(i == 6) Thread.sleep(5000);
                }
            }

            @Override
            public void cancel() {

            }
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<Integer,Long>>noWatermarks()
                        .withTimestampAssigner((x,y) -> x.f1)
                )
                .keyBy(x -> x.f0 % 2)
                .process(
                        new KeyedProcessFunction<Integer, Tuple2<Integer, Long>, Object>() {
                            ListState<Integer> cache;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<>("cache", Integer.class);
                                this.cache = getRuntimeContext().getListState(descriptor);
                            }

                            @Override
                            public void processElement(Tuple2<Integer, Long> value, KeyedProcessFunction<Integer, Tuple2<Integer, Long>, Object>.Context ctx, Collector<Object> out) throws Exception {
                                System.out.println("=================================");
                                System.out.println("接受元素：" + value);
                                this.cache.add(value.f0);
                                for (Integer integer : cache.get()) {
                                    System.out.println(integer);
                                }
                            }
                        }
                )
                ;

        env.execute();


    }
}
