package flinksummary.common;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import flinksummary.vo.KafkaMessageVo;

public class ProductTrigger extends Trigger<KafkaMessageVo, TimeWindow> {

    /**
     *
     */
    private static final long serialVersionUID = -2051026925143102343L;

    private int count;

    public ProductTrigger(int count){
        this.count = count;
    }

    //方法会在窗口中每进入一条数据的时候调用一次
    @Override
    public void clear(TimeWindow arg0, TriggerContext arg1) throws Exception {
    }

    //方法会在窗口清除的时候调用
    @Override
    public TriggerResult onElement(KafkaMessageVo element, long arg1, TimeWindow window, TriggerContext ctx)
            throws Exception {
                ctx.registerProcessingTimeTimer(window.maxTimestamp());
                System.out.println("onElement ::"+element.getMouldNoSys());
                count = count + 1;
                if(count == 4){
                    count = 0;
                    System.out.println("onElement ::触发计算");
                    return TriggerResult.FIRE_AND_PURGE;
                }
         return TriggerResult.CONTINUE;
    }

    //方法会在一个EventTime定时器触发的时候调用
    @Override
    public TriggerResult onEventTime(long arg0, TimeWindow arg1, TriggerContext arg2) throws Exception {
        //在定义为evenTime时设置为FIRE
        return TriggerResult.CONTINUE;
    }

    //方法会在一个ProcessingTime定时器触发的时候调用
    @Override
    public TriggerResult onProcessingTime(long arg0, TimeWindow arg1, TriggerContext arg2) throws Exception {
        //在定义为Processing时设置为FIRE
        return TriggerResult.FIRE;
    }
    
}
