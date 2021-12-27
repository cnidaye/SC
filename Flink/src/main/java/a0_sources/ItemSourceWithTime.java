package a0_sources;

import a1_pojo.Item;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class ItemSourceWithTime implements SourceFunction<Item> {
    boolean isRunning = true;
    String[] brands = {"OPPO", "APPLE", "HUAWEI"};
    int i = 0;
    @Override
    public void run(SourceContext<Item> ctx) throws Exception {

        while (true) {
            ctx.collect(new Item(brands[i%3], i, System.currentTimeMillis()));
            i++;
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
