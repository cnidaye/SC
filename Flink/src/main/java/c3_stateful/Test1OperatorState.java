package c3_stateful;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class Test1OperatorState {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);

        DataStreamSource<String> localhost = env.socketTextStream("localhost", 12345);

        localhost.flatMap(new FlowCountTestMapFunc())
                        .print();

        env.execute();
    }
}


class FlowCountTestMapFunc implements FlatMapFunction<String,String>, CheckpointedFunction{
    private ListState<Integer> state ;
    private ListState<Integer> intBuffer;
    private int k = 0; // 这个变量会一直保存在内存中！ 随着流计算的进行，一起递增
    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {

        for (String s : value.split(" ")) {
            out.collect(s);
            k++;
        }
        state.add(k);
        if (value.equals("cnt")) {
            StringBuilder sb = new StringBuilder();
            for (Integer integer : state.get()) {
                sb.append(integer);
                sb.append(",");
            }
            System.out.println(Thread.currentThread()+":::"+sb);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        /*
        暂时忽略， 复习到checkpoint再说
         */
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
         this.state = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<Integer>(
                        "count",
                        TypeInformation.of(Integer.class)
                )
        );
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










