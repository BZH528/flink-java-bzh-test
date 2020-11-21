package com.bzh.bigdata.hotitems;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Enumeration;

/**
 * 需求描述：每隔5分钟输出过去一小时内点击量最多的前 N 个商品
 */
public class HotItems {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 为了打印到控制台的结果不乱序，配置全局的并发为1，这里改变并发对结果的正确性没有影响
        env.setParallelism(1);

        // 告诉Flink现在按照EventTime模式处理，Flink默认使用ProcessingTime处理，要显示设置
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 创建一个PojoCsvInputFormat，一个读取csv文件并将每一行转成指定POJO类型（在案例中是UserBehavior）的输入器

        Enumeration<URL> fileURL = HotItems.class.getClassLoader().getResources("UserBehavior.csv");
        URL url = fileURL.nextElement();
        System.out.println(url.toString());
        Path filepath = Path.fromLocalFile(new File(url.toURI()));

        // 抽取Userbehavior的TypeInformation,是一个PoJoTypeInfo
        PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
        // 由于Java反射抽取出的字段顺序是不确定的，需要显示指定下文件中字段的顺序
        String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
        // 创建PojoCSVInputFormat
        PojoCsvInputFormat<UserBehavior> csvInputFormat = new PojoCsvInputFormat<>(filepath, pojoType, fieldOrder);

        // 创建输入源
        DataStream<UserBehavior> dataSource = env.createInput(csvInputFormat, pojoType);



        // 如何获得业务时间，以及生成WaterMa。WaterMa是用来追踪业务事件的概念，可以理解成EventTime世界中的时钟，
        // 用来指定当前处理到什么时刻的数据了。由于我们的数据已经经过整理，没有乱序，即事件的时间戳是单调递增的，所以
        // 可以将每条数据的业务时间就当中WaterMark。
        // 注：真实的业务场景一般都是存在乱序的，所以一般使用BoundedOutOfOrdernessTimestampExtractor

        // 基于flink1.11 新出的api
        DataStream<UserBehavior> timedData = dataSource.assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        // 原始数据单位秒，将其转成毫秒
                        return element.getTimestamp() * 1000;
                    }
                }));

        /**
         * 得到一个带有时间标记的数据流
         * 回顾下需求“每隔5分钟输出过去一小时内点击量最多的前 N 个商品”。由于原始数据中存在点击、加购、购买、收藏
         * 各种行为的数据，但是我们只需要统计点击量，所以先使用filter将点击行为数据过滤出来
         *
         */
        DataStream<UserBehavior> pvData = timedData.filter(e -> e.getBehavior().equals("pv"));


        /**
         * 由于要每隔5分钟统计一次最近1小时每个商品的点击量，所以窗口大小是1小时，每隔5分钟滑动一次。
         * 即分别要统计[09:00,10:00),[09:05,10:05),[09:10,10:10)...等窗口的商品点击量。是一个常见的滑动窗口需求
         */
        DataStream<ItemViewCount> windowData = pvData.keyBy(UserBehavior::getItemId)
                .timeWindow(Time.minutes(60), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResultFunction());


        /**
         * 为了统计每个窗口下最热门的商品，我们需要再次按窗口进行分组
         */
        DataStream<String> topItems = windowData.keyBy(ItemViewCount::getWindowEnd)
                .process(new TopNHotItems(3));// 求点击量前3名的商品

        topItems.print();
        env.execute("Hot Items Job");


    }

    /**
     * count统计的聚合函数实现，每出现一条记录加一
     */
    public static class  CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    /**用于输出窗口的结果*/
    public static class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {

        @Override
        public void apply(Long key, // 窗口的主键，即itemId、
                          TimeWindow window, // 窗口
                          Iterable<Long> aggregateResult, // 聚合函数的结果，即count值
                          Collector<ItemViewCount> collector // 输出类型为ItemViewCount
        ) throws Exception {
            Long itemId = key;
            Long count = aggregateResult.iterator().next();
            collector.collect(ItemViewCount.of(itemId, window.getEnd(), count));
        }
    }

}







