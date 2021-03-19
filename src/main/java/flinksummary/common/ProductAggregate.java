package flinksummary.common;

import org.apache.flink.api.common.functions.AggregateFunction;

import flinksummary.vo.KafkaMessageVo;

//IN,ACC,OUT
public class ProductAggregate implements AggregateFunction<KafkaMessageVo,Integer,Long> {

    /**
     *
     */
    private static final long serialVersionUID = -3633751994246895693L;

    //IN , ACC
    @Override
    public Integer add(KafkaMessageVo in, Integer acc) {
        return acc+in.getNum();
    }

    //创建时候
    @Override
    public Integer createAccumulator() {
        return 0;
    }

    //输出结果
    @Override
    public Long getResult(Integer out) {
        return Long.passLong(String.valueof(out));
    }

    //sessionwindow 合并流
    @Override
    public Integer merge(Integer acc, Integer acc2) {
        return aac+aac2;
    }
    
}
