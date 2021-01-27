package flinksummary.common;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import flinksummary.vo.KafkaMessageVo;

public class ProductWaterMark implements WatermarkStrategy<KafkaMessageVo> {

    /**
     *
     */
    private static final long serialVersionUID = 4057990317844531722L;

    @Override
    public WatermarkGenerator<KafkaMessageVo> createWatermarkGenerator(
            org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier.Context context) {

        return new ProductWatermarkGenerator();
    }

}

class ProductWatermarkGenerator implements WatermarkGenerator<KafkaMessageVo> {

    private long maxTimestamp = Long.MIN_VALUE;

    //每个元素生成水印方法
    @Override
    public void onEvent(KafkaMessageVo event, long eventTimestamp, WatermarkOutput output) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-mm-dd HH:MM:ss");
        LocalDateTime time = LocalDateTime.parse(event.getTestTime(), formatter);
        maxTimestamp = Math.max(maxTimestamp, time.toEpochSecond(ZoneOffset.of("+8")));
        output.emitWatermark(new Watermark(maxTimestamp));
    }

    //周期性生成水印的方法。这个水印的生成周期可以这样设置：env.getConfig().setAutoWatermarkInterval(5000L);
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // TODO Auto-generated method stub

    }

}