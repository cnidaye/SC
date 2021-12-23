package c0_demos;


import a0_sources.IntSource;
import a0_sources.WordSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class SourceMerge {
   @Test
    public void test0_union() throws Exception {
       StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
       WordSource wordSource = new WordSource();

       SingleOutputStreamOperator<Tuple2<String, Integer>> ss = environment.socketTextStream("localhost", 1234)
               .map(new MapFunction<String, Tuple2<String, Integer>>() {
                   @Override
                   public Tuple2<String, Integer> map(String value) throws Exception {
                       return new Tuple2<>(value, 1);
                   }
               });

       SingleOutputStreamOperator<Tuple2<String, Integer>> ws = environment.addSource(wordSource).map(new MapFunction<String, Tuple2<String, Integer>>() {
           @Override
           public Tuple2<String, Integer> map(String value) throws Exception {
               return new Tuple2<>(value, 1);
           }
       });

       ss.union(ws).keyBy(0).sum(1).print();

       environment.execute();

   }
   
   @Test
    public void test1_connect() {
       LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(4);
       DataStreamSource<String> s1 = env.socketTextStream("localhost", 1234);
       DataStreamSource<Tuple2<Integer, Long>> s2 = env.addSource(new IntSource());
       
       s1.flatMap(new FlatMapFunction<String, String>() {
           @Override
           public void flatMap(String value, Collector<String> out) throws Exception {
               for (String s : value.split(",")) {
                   out.collect(s);
               }
           }
       })
               .connect(s2)
               .map(new CoMapFunction<String, Tuple2<Integer, Long>, String>() {
                   @Override
                   public String map1(String value) throws Exception {
                       return null;
                   }

                   @Override
                   public String map2(Tuple2<Integer, Long> value) throws Exception {
                       return null;
                   }
               });


   }
}
