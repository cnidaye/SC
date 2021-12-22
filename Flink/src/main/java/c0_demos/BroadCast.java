package c0_demos;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class BroadCast {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);
        DataStreamSource<String> localhost = env.socketTextStream("localhost", 1234);

//        localhost.broadcast().map(new MapFunction<String, String>() {
//            @Override
//            public String map(String value) throws Exception {
//                return Thread.currentThread() +":::" + value;
//            }
//        }).print();
                localhost.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return Thread.currentThread() +":::" + value;
            }
        }).print();





        env.execute();





    }
}
