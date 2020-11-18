package com.bzh.bigdata.gdgraph;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.text.SimpleDateFormat;
import java.util.*;

public class Flink_kafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 非常关键，一定要设置检查点
        env.enableCheckpointing(5000);

        //配置kafka信息
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "node01:9092");
        props.setProperty("zookeeper.connect", "node01:2181");
        props.setProperty("group.id", "test");
        //读取数据，第一个参数是topic
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("log", new SimpleStringSchema(), props);
        // 设置只读最新数据
        consumer.setStartFromLatest();

        // 添加kafka为数据源
        //18542360152   116.410588, 39.880172   2019-05-24 23:43:38
        SingleOutputStreamOperator<String> stream = env.addSource(consumer)
                .map(str -> JSON.parseObject(str).getString("message"))
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String s) throws Exception {
                        String jingwei = s.split("\\t")[1];
                        String jing = jingwei.split(",")[0].trim();
                        String wei = jingwei.split(",")[1].trim();
                        // 调一下时间格式，es里面存储时间默认是UTC格式日期，+0800是设置成北京时区
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd\\'T\\'HH:mm:ss.SSS+0800");
                        String time = sdf.format(new Date());
                        String resultStr = wei + "," + jing + "," + time;
                        return resultStr;
                    }
                });

        stream.print();//数据清洗以后是这种样子 123.450169,41.740705,2019-05-26T19:03:59.281+0800
        //把清洗好的数据存入es中，数据入库

        List<HttpHost> httpHosts = new ArrayList<>();
        //es的client通过http请求连接到es进行增删改查操作
        httpHosts.add(new HttpHost("node01", 9200, "http"));

        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    //参数element就是上面清洗好的数据格式

                    public IndexRequest createIndexRequest(String element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("wei", element.split(",")[0]);
                        json.put("jing", element.split(",")[1]);
                        json.put("time", element.split(",")[2]);

                        return Requests.indexRequest()
                                .index("location-index")
                                .type("location")
                                .source(json);
                    }

                    @Override
                    public void process(String s, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        requestIndexer.add(createIndexRequest(s));

                    }
                });

        //批量请求的配置：这将指示接收器在每个元素之后发出请求，否则将对他们进行缓冲
        esSinkBuilder.setBulkFlushMaxActions(1);

        stream.addSink(esSinkBuilder.build());


        env.execute("Flink_kafka");

    }
}
