package flinksummary;

import java.util.Properties;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;

import flinksummary.common.ProductAggregate;
import flinksummary.common.ProductFlatMap;
import flinksummary.common.ProductKeySelector;
import flinksummary.common.ProductReduce;
import flinksummary.common.ProductRichSink;
import flinksummary.vo.KafkaMessageVo;

public class StartFlink {

    public static void main(String[] args) {
        // 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10 * 1000);
        CheckpointConfig pointConfig = env.getCheckpointConfig();
        //设置检查点的间隔时间
        pointConfig.setMinPauseBetweenCheckpoints(60 * 1000);
        //设置检查点的并行度
        pointConfig.setMaxConcurrentCheckpoints(4);

        // ExecutionConfig conf = env.getConfig();

        // Kafka参数
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        /**
         * 如果实现多个消费组，消费同一个主题需要启动两个进程
         */
        props.setProperty("group.id", "product");
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);

        // Consumer<String, String> consumer = new KafkaConsumer<>(props);
        /**
         * 只消费分区号为2的分区 TopicPartition p = new TopicPartition("test6", 2);h
         * consumer.assign(Arrays.asList(p));
         */
        // 消费所有分区数据
        // consumer.subscribe(Arrays.asList("hardware"));

        // flink创建kafka数据源
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("hardware", new SimpleStringSchema(),props);
        DataStream<String> stream = env.addSource(consumer);

        // Transformations
        // 使用Flink算子对输入流的文本进行操作
        // 按空格切词、计数、分区、设置时间窗口、聚合
        // DataStream<Tuple2<String, Integer>> wordCount = stream
        // .flatMap((String line, Collector<Tuple2<String, Integer>> collector) -> {
        // String[] tokens = line.split("\\s");
        // // 输出结果 (word, 1)
        // for (String token : tokens) {
        // if (token.length() > 0) {
        // collector.collect(new Tuple2<>(token, 1));
        // }
        // }
        // }).returns(Types.TUPLE(Types.STRING,
        // Types.INT)).keyBy(0).timeWindow(Time.seconds(5)).sum(1);

        // 将数据流转中的String换为实体类
        DataStream<KafkaMessageVo> jsonCollectot = stream.flatMap(new ProductFlatMap());
        // 根据指定列分组
        KeyedStream<KafkaMessageVo,String> keyStream = jsonCollectot.keyBy(new ProductKeySelector());
        //按时间设置分割信息
        //滑动窗口
        WindowedStream<KafkaMessageVo,String,TimeWindow> window = keyStream.window(SlidingProcessingTimeWindows.of(Time.seconds(20), Time.seconds(1)));
        window.trigger(trigger);
        //增量函数 每次收到信息
        DataStream<KafkaMessageVo> outputStream = window.aggregate(new ProductAggregate());
        /**
         * 全窗口函数 达到范围执行 windowFunction
         * window.apply(function, resultType)
         * */

        outputStream.addSink(new ProductRichSink());

        try {
            env.execute("production_summary");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
