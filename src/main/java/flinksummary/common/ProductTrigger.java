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

    //方法会在窗口中每进入一条数据的时候调用一次
    @Override
    public void clear(TimeWindow arg0, TriggerContext arg1) throws Exception {
        // TODO Auto-generated method stub

    }

    //方法会在窗口清除的时候调用
    @Override
    public TriggerResult onElement(KafkaMessageVo arg0, long arg1, TimeWindow arg2, TriggerContext arg3)
            throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    //方法会在一个EventTime定时器触发的时候调用
    @Override
    public TriggerResult onEventTime(long arg0, TimeWindow arg1, TriggerContext arg2) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    //方法会在一个ProcessingTime定时器触发的时候调用
    @Override
    public TriggerResult onProcessingTime(long arg0, TimeWindow arg1, TriggerContext arg2) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }
    
}
