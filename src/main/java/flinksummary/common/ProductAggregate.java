package flinksummary.common;

import org.apache.flink.api.common.functions.AggregateFunction;

import flinksummary.vo.KafkaMessageVo;

//IN,ACC,OUT
public class ProductAggregate implements AggregateFunction<KafkaMessageVo,KafkaMessageVo,KafkaMessageVo> {

    /**
     *
     */
    private static final long serialVersionUID = -3633751994246895693L;

    //IN , ACC
    @Override
    public KafkaMessageVo add(KafkaMessageVo in, KafkaMessageVo acc) {
        acc.setMouldNoSys(in.getMouldNoSys());
        acc.setNum(acc.getNum()+in.getNum());
        System.out.println("增量 "+acc.getMouldNoSys()+"::"+acc.getNum());
        return acc;
    }

    //创建时候
    @Override
    public KafkaMessageVo createAccumulator() {
        return new KafkaMessageVo();
    }

    //输出结果
    @Override
    public KafkaMessageVo getResult(KafkaMessageVo out) {
        System.out.println("窗口清空 "+out.getMouldNoSys()+"::"+out.getNum());
        return out;
    }

    //sessionwindow 合并流
    @Override
    public KafkaMessageVo merge(KafkaMessageVo acc, KafkaMessageVo acc2) {
        return null;
    }
    
}
