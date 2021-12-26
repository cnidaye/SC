package c3_stateful;

import a1_pojo.Item;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;


/*
机制参考
https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/broadcast_state/
输出平均质量合格的产品
主流：检测报告
规则流：产品的合格标准
 */
public class Test2Broadcast {
    public static void main(String[] args) throws Exception {
//        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(8);


        DataStreamSource<String> localhost = env.socketTextStream("localhost", 12345);
        DataStreamSource<String> ruleStream = env.socketTextStream("localhost", 12346);
        SingleOutputStreamOperator<Tuple2<String, Long>> resultStream
                = localhost
                .map(x -> {
                    String[] split = x.split(" ");
                    return new Item(split[0], Integer.valueOf(split[1]));
                })
                .keyBy(x -> x.name)
                .process(new KeyedProcessFunction<String, Item, Tuple2<String, Long>>() {
                    ValueState<Integer> cnt;
                    ValueState<Integer> sum;

                    @Override
                    public void open(Configuration parameters) {
                        cnt = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count", Integer.class));
                        sum = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("sum", Integer.class));
                    }
                    @Override
                    public void processElement(Item value, KeyedProcessFunction<String, Item, Tuple2<String, Long>>.Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        if (this.cnt.value() == null) {
                            this.cnt.update(1);
                            this.sum.update(value.score);
                            out.collect(Tuple2.of(value.name, (long) value.score));
                        }else{
                            int count = this.cnt.value() + 1;
                            int total = this.sum.value() + value.score;
                            this.cnt.update(count);
                            this.sum.update(total);
                            out.collect(Tuple2.of(value.name, (long) total / count));
                        }

                    }
                });
//        rules.map(x )
        MapStateDescriptor<String,Item> ruleStateDesp = new MapStateDescriptor<String, Item>(
                "RuleStandard",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Item>() {
                }));
        //todo 这里的传播是什么意思？
        BroadcastStream<String> ruleBroadcastStream = ruleStream.broadcast(ruleStateDesp);

        resultStream
                .connect(ruleBroadcastStream)
                .process(new BroadcastProcessFunction<Tuple2<String, Long>, String, String>() {
                    MapStateDescriptor<String,Item> ruleState = new MapStateDescriptor<String, Item>(
                            "RuleStandard",
                    BasicTypeInfo.STRING_TYPE_INFO,
                            TypeInformation.of(new TypeHint<Item>() {
                    }));





                    new MapStateDescriptor<String,Item>()


                    @Override
                    public void processElement(Tuple2<String, Long> value, BroadcastProcessFunction<Tuple2<String, Long>, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        ReadOnlyBroadcastState<String, Item> broadcastState = ctx.getBroadcastState(this.ruleState);
                        Item standard = broadcastState.get(value.f0);

                        if (standard != null) {
                            System.out.println("mark1");
                            if (value.f1 > standard.score) {
                                out.collect(value.toString());
                            }
                        }else{
                            System.out.println("mark2");
                            out.collect(value.toString());
                        }
                    }
                    @Override
                    public void processBroadcastElement(String value, BroadcastProcessFunction<Tuple2<String, Long>, String, String>.Context ctx, Collector<String> out) throws Exception {
                        BroadcastState<String, Item> broadcastState = ctx.getBroadcastState(this.ruleState);
                        String[] split = value.split(" ");
                        broadcastState.put(split[0], new Item(split[0], Integer.valueOf(split[1])) );
                        System.out.println("update standard of " + split[0]);

                    }
                })
                .print();

        env.execute("quality check");

    }
}


