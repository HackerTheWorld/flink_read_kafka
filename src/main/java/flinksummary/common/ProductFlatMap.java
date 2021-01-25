package flinksummary.common;

import com.alibaba.fastjson.JSON;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import flinksummary.vo.KafkaMessageVo;

public class ProductFlatMap implements FlatMapFunction<String,KafkaMessageVo> {

    private static final long serialVersionUID = -7745497930713310978L;

    @Override
    @SuppressWarnings("all")
    public void flatMap(String str, Collector collector) throws Exception {
        collector.collect(JSON.parseObject(str,KafkaMessageVo.class));
    }
    
}
