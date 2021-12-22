package c3_stateful;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.LinkedList;

public class OperatorStateTest {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);

        DataStreamSource<String> localhost = env.socketTextStream("localhost", 1234);

        localhost.flatMap(new RichFlatMapFunction<String, String>() {
            ListState<String> banned;
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String s : value.split(" ")) {
                    out.collect(s);
                }
            }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.banned = getRuntimeContext().getListState(new ListStateDescriptor<String>("bannedWords",))
                    }
                }).map(x -> new Tuple2(x,1))
                .keyBy(0)
                .sum(1)
                .print();

        env.execute();
    }
}


class BannedFlatMapFunction implements CheckpointedFunction, FlatMapFunction<String,String> {
    private MapState<String,Integer> bannedWordsState;
    private Long count = 0L;
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.bannedWordsState = context
                .getOperatorStateStore()
                .get);
        this.bannedWordsState.add("cnm");
        this.bannedWordsState.add("cnm");
        this.bannedWordsState.add("cnm");
        this.bannedWordsState.add("cnm");
    }

    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        this.bannedWordsState.
    }
}



class MyFunction<T> implements MapFunction<T, T>, CheckpointedFunction {

    private ReducingState<Long> countPerKey;

    private long localCount;

    public void initializeState(FunctionInitializationContext context) throws Exception {
        // get the state data structure for the per-key state
        countPerKey = context.getKeyedStateStore().getReducingState(
                new ReducingStateDescriptor<>("perKeyCount", new ReduceFunction<Long>() {
                    @Override
                    public Long reduce(Long value1, Long value2) throws Exception {
                       return value1 + value2;
                    }
                }, Long.class));
    }

    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // the keyed state is always up to date anyways
        // just bring the per-partition state in shape
    }

    public T map(T value) throws Exception {
        // update the states
        countPerKey.add(1L);
        localCount++;

        return value;
    }
}










