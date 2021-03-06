package flinksummary.common;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

import flinksummary.vo.KafkaMessageVo;

public class ProductTimestampAssignerSupplier implements SerializableTimestampAssigner<KafkaMessageVo> {

    /**
     *
     */
    private static final long serialVersionUID = -6580381219758594703L;

    @Override
    public long extractTimestamp(KafkaMessageVo vo, long timestamp) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-mm-dd HH:MM:ss");
        LocalDateTime time = LocalDateTime.parse(vo.getTestTime(), formatter);
        return time.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }


    
}
