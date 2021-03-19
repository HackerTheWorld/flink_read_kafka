package nettyNIO.hander.flink;

import java.text.SimpleDateFormat;
import flinksummary.vo.KeyVo;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ProductWindowFunction implements WindowFunction<Long,KafkaMessageVo,KeyVo,TimeWindow> {

    /**
     *
     */
    private static final long serialVersionUID = 1748308243835403341L;

    //key为进行keyby的分类
    @Override
    public void apply(KeyVo key, TimeWindow window, Iterable<Long> input, Collector<KafkaMessageVo> out) throws Exception {
        KafkaMessageVo kafkaMessageVo = new KafkaMessageVo();
        kafkaMessageVo.setJsonId(key.getJsonId());
        kafkaMessageVo.setMouldNoSys(key.getMouldNoSys());
        String timeStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(window.getStart());
        kafkaMessageVo.setTestTime(timeStr);
        kafkaMessageVo.setNum(input.iterator().next().intValue());
        out.collect(kafkaMessageVo);
    }
    
}
