package c0_demos;


import a0_sources.WordSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

public class SourceMerge {
   @Test
    public void test0() throws Exception {
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
}
