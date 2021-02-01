package flinksummary.common;

import java.util.TreeMap;

import org.apache.flink.api.java.functions.KeySelector;

import flinksummary.vo.KafkaMessageVo;

public class ProductKeySelector implements KeySelector<KafkaMessageVo,TreeMap<String,Object>> {

    private static final long serialVersionUID = -7061754932069669924L;

    @Override
    public TreeMap<String,Object> getKey(KafkaMessageVo kaVo) throws Exception {
        //定义分组逻辑，返回结果一致将被分为一组
        TreeMap<String,Object> treeMap = new TreeMap<String,Object>();
        treeMap.put("jsonId", kaVo.getJsonId());
        treeMap.put("mouldNoSys", kaVo.getMouldNoSys());
        return treeMap;
    }
    
}
