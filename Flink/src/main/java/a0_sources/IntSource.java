package a0_sources;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

//@Slf4j
/*
收集：自增数字，时间戳
时间戳 逢三 减3s
 */
public class IntSource implements SourceFunction<Tuple2<Integer,Long>> {
    boolean isRunning = true;
    int x = 0;
    @Override
    public void run(SourceContext<Tuple2<Integer, Long>> ctx) throws Exception {
        while (isRunning) {
            if(x == 2000) this.cancel();
            x += 1;
            long t = System.currentTimeMillis();
//            t += x%3==0 ? -10000 : 0;
            t += -3000;
            System.out.println("Int: " + x + " Long: " + t);
            ctx.collect(new Tuple2<>(x,t));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;

    }
}
