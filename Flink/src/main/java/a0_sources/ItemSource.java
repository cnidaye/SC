package a0_sources;

import a1_pojo.Item;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class ItemSource implements SourceFunction<Item> {
    boolean isRunning = true;
    String[] brands = {"OPPO", "APPLE", "HUAWEI"};
    int i = 0;
    @Override
    public void run(SourceContext<Item> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(new Item(brands[i%3], (int) ( Math.random()*100) ));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
