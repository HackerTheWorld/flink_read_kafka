package flinksummary.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import flinksummary.vo.KafkaMessageVo;

public class ProductRichSink extends RichSinkFunction<KafkaMessageVo> {

    private static final long serialVersionUID = -4495024390351471765L;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    //对数据进行操作
    @Override
    public void invoke(KafkaMessageVo value, Context context) throws Exception {
        System.out.println(value.getMouldNoSys()+"::"+value.getNum());
    }

}
