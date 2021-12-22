package a0_sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class WordSource implements SourceFunction<String> {
    boolean isRunning = true;
    String[] words = {"Hello","world","hi"};
    private int i = 0;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        i = (i+1) % 3;
        while (isRunning) {
            ctx.collect(words[i]);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
