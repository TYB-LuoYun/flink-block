package top.anets.flink.job;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.io.ParseException;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import top.anets.flink.bean.Log;
import top.anets.flink.job.uv_example.ConsoleInputSource;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author ftm
 * @date 2023-11-10 16:57
 * 网页总浏览量
 */
public class WebPV {
    public static void main(String[] args) throws Exception {

        // 设置Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取info.log文件，并创建数据流
//        DataStreamSource<String> logStream = env.addSource(new ConsoleInputSource());
        DataStream<String> logStream = env.readTextFile("C:\\Users\\Administrator\\.auth\\logs\\ryu-pay\\2023-11-13\\info-log-0.log");
//        DataStream<String> logStream = env.socketTextStream("localhost",3394);
        // 转换和处理日志数据，统计请求次数
         logStream
//                .filter(line -> line.contains("get url ->") || line.contains("post url ->"))
//                .map(new RequestMapper()
                .flatMap(new LogTokenizer())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Log>forBoundedOutOfOrderness(Duration.ofSeconds(0))
//                                .<Log>forMonotonousTimestamps( )
                                .withTimestampAssigner((ele, ts) -> ele.getDate().getTime())
                        //.withIdleness(Duration.ofSeconds(5))  // 当某个并行度水印超过5s没有更新, 则以其他为准
                )
                .keyBy(Log::getMethod)  // 使用Tuple2的第一个字段作为Key
//                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum("count")
                .print();


        // 执行Flink作业
        env.execute("LogAnalysisJob");
    }

//    public static class RequestMapper implements MapFunction<String, Tuple2<String, Integer>> {
//        @Override
//        public Tuple2<String, Integer> map(String line) {
//            return new Tuple2<>("TotalRequests", 1);
//        }
//    }


    // 实现LogTokenizer类用于解析日志并生成Tuple2
    public static class LogTokenizer implements FlatMapFunction<String, Log> {
        private static final Pattern logPattern =Pattern.compile("(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}).*?\\[.*?\\] INFO.*?\\s+(get|post)\\s+(\\S+)\\s+->");


        @Override
        public void flatMap(String value, Collector<Log> out) {
            Matcher matcher = logPattern.matcher(value);
            System.out.println(value);

            if (matcher.find()) {
                try {
                    String timestampString = matcher.group(1);
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    Date date = dateFormat.parse(timestampString);
                    String method = matcher.group(2);
                    String path = matcher.group(3);
                    Log log = new Log();
                    log.setDate(date);
                    log.setMethod(method );
                    System.out.println(date);
                    System.out.println(method );
                    out.collect(log);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
