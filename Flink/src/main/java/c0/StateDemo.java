package c0;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateDemo {
    static int fare = 30;
    static int discountFare = 15;
    public static void main(String[] args) {
        LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment();
        localEnvironment.setParallelism(4);

        localEnvironment.readTextFile("D:\\MyProject\\SC\\Data\\tourist.csv")
                .map(x -> {
                    String[] split = x.split(",");
                    return new Tourist(split[0], split[1]);
                })
                .keyBy(x -> x.gid)

                ;
    }
}

class Tourist{
    String gid;
    String name;

    public Tourist(String gid, String name) {
        this.gid = gid;
        this.name = name;
    }
}