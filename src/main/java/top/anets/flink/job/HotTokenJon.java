package top.anets.flink.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author ftm
 * @date 2023-11-08 17:45
 */
public class HotTokenJon {
    public static void main(String[] args) throws Exception {
        // 0. Kafka相关配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.10.11:9092");
        properties.setProperty("group.id", "Flink01_Source_Kafka");
        properties.setProperty("auto.offset.reset", "latest");

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<String> topics = new ArrayList<>();
        topics.add("token");

        /**
         * 数据源
         */
        DataStream<String> kafkaStream = env
                .addSource(new FlinkKafkaConsumer<>(topics, new SimpleStringSchema(), properties))
                .setParallelism(1); // 设置并行度为1以确保顺序处理


        SingleOutputStreamOperator<String> holders = kafkaStream
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        // 解析JSON字符串并获取"holders"字段的值
                        // 假设你使用的是Gson库
                        JSONObject jsonObject = JSON.parseObject(value);
                        Double volumn24 = jsonObject.getDouble("volumn24");
                        Double circulatingMarketCap = jsonObject.getDouble("circulatingMarketCap");
                        // 检查"holders"字段是否大于100
                        return BigDecimal.valueOf(volumn24).compareTo(BigDecimal.valueOf(10000000))>0
                                &&BigDecimal.valueOf(circulatingMarketCap).compareTo(BigDecimal.valueOf(100000000))<0 ;
                    }
                });

        holders.addSink(new MyListSink());

        // 3. 启动Flink作业
        env.execute("KafkaFilterJob");
    }


    public static class MyListSink  implements SinkFunction<String> {
        @Override
        public void invoke(String value, Context context) throws Exception{
            System.out.println("处理结果:"+value);
        }
    }
}
