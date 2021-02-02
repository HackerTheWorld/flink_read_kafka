package nettyNIO.hander.flink;

import org.apache.flink.api.java.functions.KeySelector;

public class ProductSecondKeySelector implements KeySelector<RabbitMqVo, KeyVo> {

    /**
     *
     */
    private static final long serialVersionUID = 1695730433985512132L;

    @Override
    public KeyVo getKey(RabbitMqVo vo) throws Exception {
        KeyVo keyVo = new KeyVo();
        keyVo.setJsonId(vo.getJsonId());
        keyVo.setMouldNoSys(vo.getMouldNoSys());
        return keyVo;
    }
    
}
