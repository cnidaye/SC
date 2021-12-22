package c3_stateful;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BackendTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

//        env.setStateBackend()
    }
}
