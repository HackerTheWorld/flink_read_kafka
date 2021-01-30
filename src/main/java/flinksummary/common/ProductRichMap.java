package nettyNIO.hander.flink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

/**
 * 定义键控状态用于恢复系统
 */
public class ProductRichMap extends RichMapFunction<KafkaMessageVo,KafkaMessageVo>{

    /**
     *
     */
    private static final long serialVersionUID = -4788058556367710975L;
    private ValueState<Integer> val = null;

    @Override
    public KafkaMessageVo map(KafkaMessageVo in) throws Exception {
        int count = val.value() + 1;
        val.update(count);
        return in;
    }

    @Override
    public void open(Configuration parameters) throws java.lang.Exception {
        ValueStateDescriptor<Integer> value = new ValueStateDescriptor<Integer>("keyState",Integer.class);
        val = getRuntimeContext().getState(value);
        val.update(0);
    }
    
}
