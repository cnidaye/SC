package a2_test;

import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class test01_checkpointConfig {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        System.out.println(checkpointConfig.isForceUnalignedCheckpoints());
        System.out.println(checkpointConfig.isForceCheckpointing());

    }
}
