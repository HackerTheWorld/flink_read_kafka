package flinksummary.common;

import org.apache.flink.api.common.functions.ReduceFunction;

import flinksummary.vo.KafkaMessageVo;

public class ProductReduce implements ReduceFunction<KafkaMessageVo> {

    /**
     *
     */
    private static final long serialVersionUID = 2992269023065340641L;

    @Override
    public KafkaMessageVo reduce(KafkaMessageVo arg0, KafkaMessageVo arg1) throws Exception {
        KafkaMessageVo kafkaMessageVo = new KafkaMessageVo();
        kafkaMessageVo.setJsonId("");
        kafkaMessageVo.setMouldNoSys(arg0.getMouldNoSys());
        kafkaMessageVo.setNum(arg0.getNum()+arg1.getNum());
        return kafkaMessageVo;
    }
    
}
