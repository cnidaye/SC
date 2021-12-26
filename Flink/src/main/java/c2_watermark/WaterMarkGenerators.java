package c2_watermark;

import a1_pojo.TaxiOrder;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class WaterMarkGenerators {
    /*
    这个生成器，假定记录是乱序的，但是记录最多只会比最早到的记录晚到T秒。
    所以，这个规律是与
     */
    static class BoundOutofOrdernessGene implements WatermarkGenerator<TaxiOrder> {
        /*
        每次处理事件时触发，一般此函数可处理
        1.记录与事件相关的信息，比如：事件的事件戳；事件的属性
        2.将watermark的产生与事件进行绑定
         */
        private final long maxOutofOrdernesss = 3500;
        private long currentMaxTimestamp;
        @Override
        public void onEvent(TaxiOrder event, long eventTimestamp, WatermarkOutput output) {
            currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutofOrdernesss - 1));

        }
    }
    /*
    与上面那个类似，但是基准是以处理时间为标准；记录最多只会晚到T秒
     */
    static class TimeLagWatermarkGenerator implements WatermarkGenerator<TaxiOrder> {
        private long maxTimelog = 3000;

        @Override
        public void onEvent(TaxiOrder event, long eventTimestamp, WatermarkOutput output) {
            /*
            不需要处理任何事情，因为水位是基于物理生活的处理时间而产生的
             */
        }
        /*
        这个会周期性调用，是否产生水位取决于你自己的业务逻辑

         */
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimelog));
        }
    }
    /*
    另一种风格，标记性生成;
    基于事件信息是否含有标记信息，从而生成水位。
    水位的生成频率取决于标记信息的频率，
    水位代表的时间，取决于自己的业务逻辑
     */
    static class PunctuatedAssigner implements WatermarkGenerator<String> {
        @Override
        public void onEvent(String event, long eventTimestamp, WatermarkOutput output) {
            if (event.equals("keyword")) {
                output.emitWatermark(new Watermark(System.currentTimeMillis()));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

        }
    }

}