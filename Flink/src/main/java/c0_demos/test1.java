package c0_demos;

import a0_sources.MyStreamingSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class test1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Tuple3<Integer, Integer, Integer>> data = new ArrayList<>();
        data.add(new Tuple3<>(0,11,0));
        data.add(new Tuple3<>(0,1,11));
        data.add(new Tuple3<>(0,12,22));
        data.add(new Tuple3<>(0,1,33));
        data.add(new Tuple3<>(1,21,5));
        data.add(new Tuple3<>(1,223,9));
        data.add(new Tuple3<>(1,232,11));
        data.add(new Tuple3<>(1,2342,13));
        environment.fromCollection(data)
                .keyBy(x -> x.f0)
                .max(2)
                .printToErr();

        environment.execute();




    }
}

