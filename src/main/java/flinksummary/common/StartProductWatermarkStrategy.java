package nettyNIO.hander.flink;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import com.alibaba.fastjson.JSON;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

/**
 * 在数据流进入时定义watermark
 */
public class StartProductWatermarkStrategy implements WatermarkStrategy<String> {

    /**
     *
     */
    private static final long serialVersionUID = -8920612296223778768L;

    @Override
    public WatermarkGenerator<String> createWatermarkGenerator(
            org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier.Context arg0) {
        return new StartProductWatermarkGenerator();
    }
    
}

class StartProductWatermarkGenerator implements WatermarkGenerator<String>{

    private long maxTime = Long.MIN_VALUE;

    @Override
    public void onEvent(String vo, long timeStamp, WatermarkOutput out) {
        KafkaMessageVo kafkaMessageVo = JSON.parseObject(vo,KafkaMessageVo.class);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime time = LocalDateTime.parse(kafkaMessageVo.getTestTime(), formatter);
        long timeSecond = time.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        maxTime = Math.max(maxTime, timeSecond);
        out.emitWatermark(new Watermark(maxTime));
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput out) {
       // out.emitWatermark(new Watermark(maxTimestamp));

    }

}
