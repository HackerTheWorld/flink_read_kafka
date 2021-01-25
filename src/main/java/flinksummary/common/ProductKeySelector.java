package flinksummary.common;

import org.apache.flink.api.java.functions.KeySelector;

import flinksummary.vo.KafkaMessageVo;

public class ProductKeySelector implements KeySelector<KafkaMessageVo,String> {

    private static final long serialVersionUID = -7061754932069669924L;

    @Override
    public String getKey(KafkaMessageVo kaVo) throws Exception {
        //定义分组逻辑，返回结果一致将被分为一组
        return kaVo.getMouldNoSys();
    }
    
}
