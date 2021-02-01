package flinksummary.common;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import com.alibaba.fastjson.JSON;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

import flinksummary.vo.KafkaMessageVo;

public class StartProductTimestampAssigner implements SerializableTimestampAssigner<String> {

    /**
     *
     */
    private static final long serialVersionUID = -6742651350350360182L;

    @Override
    public long extractTimestamp(String vo, long timeStamp) {
        KafkaMessageVo kafkaMessageVo = JSON.parseObject(vo,KafkaMessageVo.class);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime time = LocalDateTime.parse(kafkaMessageVo.getTestTime(), formatter);
        long timeSecond = time.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        return timeSecond;
    }
    
}
