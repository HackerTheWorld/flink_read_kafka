package flinksummary.common;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//输入和输出可以不相同
public class ProductProcess extends ProcessFunction<RabbitMqVo, RabbitMqVo> {

    /**
     *
     */
    private static final long serialVersionUID = -3040429104038342112L;

    OutputTag<SideOutput> outputTag = new OutputTag<SideOutput>("c",TypeInformation.of(SideOutput.class));

    @Override
    public void processElement(RabbitMqVo in, ProcessFunction<RabbitMqVo, RabbitMqVo>.Context context,
            Collector<RabbitMqVo> out) throws Exception {
        //正常输出信息

        out.collect(in);
        if("c".equals(in.getMouldNoSys())){
            SideOutput sideOutput = new SideOutput();
            sideOutput.setMouldNoSys(in.getJsonId());
            context.output(outputTag, sideOutput);

        }
        
    }
    
}
