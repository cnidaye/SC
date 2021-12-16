package c0_demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple2;

public class ParallelismTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
//        env.setParallelism(3);
        System.out.println(env.getParallelism());

        DataStreamSource<String> source = env.socketTextStream("localhost", 1234);

        source.flatMap(new MyFlatMapper())
                .setParallelism(2)
                .map(new MyMapper())
                .setParallelism(2)
                .keyBy(x -> x._1)
                .reduce(new MyReducer())
                .setParallelism(3)
                .print()
//                .setParallelism(1)
        ;

        env.execute("socket wc");
    }

    static class MyFlatMapper implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
           for(String s : value.split(" ")){
               out.collect(s);
           }
        }
    }
    static class MyMapper implements MapFunction<String, Tuple2<String,Integer>> {
        @Override
        public Tuple2<String, Integer> map(String value) throws Exception {
           return new Tuple2<>(value,1);
        }
    }

    static class MyReducer implements ReduceFunction<Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
           return new Tuple2<>(value1._1, value1._2 + value2._2);
        }
    }
}
