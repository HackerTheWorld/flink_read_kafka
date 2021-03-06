package flinksummary;

import java.util.Properties;
import java.util.TreeMap;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.common.serialization.StringSerializer;

import flinksummary.common.ProductAggregate;
import flinksummary.common.ProductFlatMap;
import flinksummary.common.ProductKeySelector;
import flinksummary.common.ProductProcess;
import flinksummary.common.ProductRichMap;
import flinksummary.common.ProductRichSink;
import flinksummary.common.ProductTrigger;
import flinksummary.common.StartProductTimestampAssigner;
import flinksummary.common.StartProductWatermarkStrategy;
import flinksummary.vo.KafkaMessageVo;
import flinksummary.vo.SideOutput;

public class StartFlink {

    public static void main(String[] args) {
        // 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        /**
         * 设置内存保存点状态后端，默认内存 
         * env.setStateBackend(new MemoryStateBackend());
         */

        /**
         * 设置文件保存点状态后端,地址可设置本地地址或者hdfs地址 
         * env.setStateBackend(new FsStateBackend(""));
         */

        /**
         * 设置RocksDB保存点状态后端,地址可设置本地地址或者hdfs地址
         * try {
         *   env.setStateBackend(new RocksDBStateBackend("", true));
         * } catch (IOException e1) {
         *   e1.printStackTrace();
         * } 
         */
        
        env.enableCheckpointing(10 * 1000);
        // 取消任务checkpoint不删除
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置checkpoint的 EXACTLY_ONCE 模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        
        // 设置检查点的间隔时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60 * 1000);
        // 设置检查点的并行度
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(4);

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
        // 【重点】取消kafka管理偏移量，让flink来管理偏移量
        prop.setProperty("enable.auto.commit","false");
        // Consumer<String, String> consumer = new KafkaConsumer<>(props);
        /**
         * 只消费分区号为2的分区 TopicPartition p = new TopicPartition("test6", 2);h
         * consumer.assign(Arrays.asList(p));
         */
        // 消费所有分区数据
        // consumer.subscribe(Arrays.asList("hardware"));

        StartProductWatermarkStrategy water = new StartProductWatermarkStrategy();
        water.withTimestampAssigner(new StartProductTimestampAssigner());

        // flink创建kafka数据源
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("hardware", new SimpleStringSchema(),
                props);
        DataStream<String> stream = env.addSource(consumer)
                // 任务执行一开始设置watermark。保证任务的water一致
                .assignTimestampsAndWatermarks(water);

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

        /**
         * 自定义的分流 OutputTag<KafkaMessageVo> middleware = new
         * OutputTag<KafkaMessageVo>(分流的类型|"side-output-200~500"定义的范围);
         * 进行判断将不同种类型的数据存到不同的 OutputTag 中去。 ProcessFunction KeyedProcessFunction
         * CoProcessFunction ProcessWindowFunction ProcessAllWindowFunction
         */

        // 测流分流输出
        OutputTag<SideOutput> outputTag = new OutputTag<SideOutput>("c", TypeInformation.of(SideOutput.class));
        SingleOutputStreamOperator<KafkaMessageVo> process = jsonCollectot.process(new ProductProcess());
        process.getSideOutput(outputTag).print();
        // 定义watermark越早越好，最好在stream流进入时候定义
        /**
         * 定义水位线 WatermarkStrategy<RabbitMqVo> water = WatermarkStrategy
         * .<RabbitMqVo>forBoundedOutOfOrderness(Duration.ofSeconds(2))
         * .withTimestampAssigner(new ProductTimestampAssignerSupplier());
         * 
         * WatermarkStrategy<KafkaMessageVo> water = new ProductWaterMark();
         * water.withTimestampAssigner(new ProductTimestampAssignerSupplier());
         * SingleOutputStreamOperator<KafkaMessageVo> assOperator =
         * process.assignTimestampsAndWatermarks(water);
         */

        // 根据指定列分组
        KeyedStream<KafkaMessageVo, TreeMap<String, Object>> keyStream = process.keyBy(new ProductKeySelector());
        //设置键控属性
        keyStream = keyStream.map(new ProductRichMap()).keyBy(new ProductKeySelector());
        //按时间设置分割信息
        
        //获取迟到数据
        OutputTag<KafkaMessageVo> outputLateTag = new OutputTag<KafkaMessageVo>("later",TypeInformation.of(KafkaMessageVo.class));
        
        //滑动窗口
        WindowedStream<KafkaMessageVo,TreeMap<String, Object>,TimeWindow> window = keyStream
            .window(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(1)))
            .sideOutputLateData(outputLateTag)
            .trigger(new ProductTrigger(0));;
        //获取纰漏数据，纰漏数据为批处理方式
        //window.sideOutputLateData(outputTag);
        
        //定义watermark    
        //增量函数 每次收到信息
        SingleOutputStreamOperator<KafkaMessageVo> outputStream = window.aggregate(new ProductAggregate());
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
