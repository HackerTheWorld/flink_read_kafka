package flinksummary.common;

import org.apache.flink.api.java.functions.KeySelector;
import flinksummary.vo.KafkaMessageVo;

public class ProductSecondKeySelector implements KeySelector<KafkaMessageVo, KeyVo> {

    /**
     *
     */
    private static final long serialVersionUID = 1695730433985512132L;

    @Override
    public KeyVo getKey(KafkaMessageVo vo) throws Exception {
        KeyVo keyVo = new KeyVo();
        keyVo.setJsonId(vo.getJsonId());
        keyVo.setMouldNoSys(vo.getMouldNoSys());
        return keyVo;
    }
    
}
