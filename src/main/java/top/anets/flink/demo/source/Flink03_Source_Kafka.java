package top.anets.flink.demo.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2020/12/8 22:44
 */
public class Flink03_Source_Kafka {
    public static void main(String[] args) throws Exception {

        // 0.Kafka相关配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.10.11:9092");
        //设置消费组
        properties.setProperty("group.id", "Flink01_Source_Kafka");
        properties.setProperty("auto.offset.reset", "latest");  //kafka消费数据模式，latest是从最新位置，earlist是从最早位置开始

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<String> topics = new ArrayList<>();
        topics.add("token");
        env
          .addSource(new FlinkKafkaConsumer<>(topics, new SimpleStringSchema(), properties))  //topic名，序列化器，kafka配置文件
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        System.out.println("接受到数据："+value);
                    }
                })
          .print("kafka source");

        env.execute();
    }
}
