package c3_stateful;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
/*
模拟滴滴打车， 每有同样的记录id，则输出行进公里数。
 */
public class Test0KeyedState {
    public static void main(String[] args) throws Exception {
//        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);
        env.socketTextStream("localhost",1234)
                .map(new MapFunction<String, Distance>() {
                    @Override
                    public Distance map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Distance(split[0], Integer.parseInt(split[1]));
                    }
                }).keyBy(x -> x.id)
                .flatMap(
                        new RichFlatMapFunction<Distance, String>() {
                            ValueState<Integer> startMills ;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                System.out.println("===OPEN====");
                                ValueStateDescriptor<Integer> disDes = new ValueStateDescriptor<>("disDes", Integer.class);
                                startMills = getRuntimeContext().getState(disDes);
                            }

                            @Override
                            public void flatMap(Distance value, Collector<String> out) throws Exception {
                                Integer start = startMills.value();
                                if (start == null) {
                                    startMills.update(value.miles);
                                }else {
                                    out.collect(value.id + "===" + (value.miles - start));
                                    startMills.clear();
                                }
                            }
                        }
                ).print();

        env.execute();
    }
}
class Distance{
    public String id;
    public int miles;

    public Distance(String id, int miles) {
        this.id = id;
        this.miles = miles;
    }
}