package c0;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class ItStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(4);
        DataStream<Long> someIntegers = env.fromSequence(0, 4);

        IterativeStream<Long> iteration = someIntegers.iterate();


        DataStream<Long> minusOne = iteration.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("calling map function: " + value);
                return value - 1 ;
            }
        });

        DataStream<Long> stillGreaterThanZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                System.out.println("calling filter on >0 : " + value);
                return (value > 0);
            }
        });

        iteration.closeWith(stillGreaterThanZero);

//        DataStream<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {
//            @Override
//            public boolean filter(Long value) throws Exception {
//                System.out.println("calling filter on <0 : " + value);
//                return (value <= 0);
//            }
//        });

        iteration.print();

        env.execute("123");
    }
}
