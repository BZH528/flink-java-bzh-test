package com.bzh.bigdata.streamingwithflink.chapter7;

import com.bzh.bigdata.streamingwithflink.util.SensorReading;
import com.bzh.bigdata.streamingwithflink.util.SensorSource;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple3;

/**
 * 展示了如何在一条传感器数据测量流上应用一个带有键值分区ValueState的FlatMapFunction
 * 该示例应用会在检测到相邻温度值变化超过给定阈值时发出警报
 */
public class KeyedStateFunction {



    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.getCheckpointConfig().setCheckpointInterval(10 * 1000);
//
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
//        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> sensorData = env.addSource(new SensorSource());

        KeyedStream<SensorReading, String> keyedStream = sensorData.keyBy(SensorReading::getId);



        // 在键值分区数据流上应用一个有状态的FlatMapFunction
        // 来比较读数并发出警报
        DataStream<Tuple3<String, Double, Double>> alerts = keyedStream.flatMap(new TemperatureAlertFunction(1.7));


        alerts.print();

        env.execute("KeyedStateFunction");

    }
}

class TemperatureAlertFunction extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

    private double threshold;

    public TemperatureAlertFunction(double threshold) {
        this.threshold = threshold;
    }

    // 状态引用对象
    private transient ValueState<Double> lastTempState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 创建状态描述符
        ValueStateDescriptor<Double> lastTempDescriptor = new ValueStateDescriptor<>("lastTemp", Types.DOUBLE);
        // 获得状态引用
        lastTempState = getRuntimeContext().getState(lastTempDescriptor);
    }

    @Override
    public void flatMap(SensorReading reading, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            // 从状态中获取上一次的温度
            // 首次获取的state为null,需转化为0.0
             Double lastTemp = lastTempState.value() == null ? 0.0 : lastTempState.value();
            // 检查是否需要发出警报
            double tempDiff = Math.abs(reading.temperature - lastTemp);
            if (tempDiff > threshold) {
                // 温度变化超过阈值
                out.collect(new Tuple3<>("lastTemp:" + lastTemp +"=========Alarm===========:" + reading.id, reading.temperature, tempDiff));
            }

            // 更新lastTemp状态
            this.lastTempState.update(reading.temperature);


    }
}
