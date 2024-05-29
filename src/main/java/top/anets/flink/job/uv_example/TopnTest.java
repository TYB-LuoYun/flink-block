package top.anets.flink.job.uv_example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @author ftm
 * @date 2023-11-13 14:59
 */
public class TopnTest {

    public static void main(String[] args) throws Exception {

        // step1 读取数据 并设置水位线
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());

        SingleOutputStreamOperator<Event> stream01 = streamSource.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (x, l) -> x.time)
        );


        // step2 聚合统计 每个url的访问量
        SingleOutputStreamOperator<UrlViewCount> stream02 = stream01
                .keyBy(x -> x.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());

        stream02.print("stream02");


//        // step3 合并排序各个url的访问量 计算出Top N
//        stream02.keyBy(x -> x.windowEnd)
//                .process(new TopN(2))
//                .print();

        env.execute();

    }


    // 累加器 AggregateFunction<IN, ACC, OUT> extends Function, Serializable
    private static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event event, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;       // 返回UrlViewCountAgg 的结果 这里是url的计数count
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }

    // 增量聚合函数的 getResult作为 全窗口函数的输入
    // just 为了输出窗口信息 ProcessWindowFunction<IN, OUT, KEY, W extends Window> extends AbstractRichFunction
    private static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        // 这里的迭代器对象 Iterable<Long> 就是增量聚合函数UrlViewCountAgg 中累加器聚合的结果，即每个url的count
        public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {

            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            // 这里的迭代器只有一个元素 就是聚合函数中增量聚合的结果
            out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
        }
    }

    private static class TopN extends KeyedProcessFunction<Long, UrlViewCount, String> {

        private Integer n;
        private ListState<UrlViewCount> urlViewCountListState;

        public TopN(Integer n){
            this.n = n;
        }

        @Override
        public void open(Configuration parmeters) throws Exception{
            // 从环境中获取列表状态句柄
            urlViewCountListState =
                    getRuntimeContext()
                            .getListState(new ListStateDescriptor<UrlViewCount>("url-view-count-list", Types.POJO(UrlViewCount.class)));
        }

        @Override
        public void processElement(UrlViewCount value, KeyedProcessFunction<Long, UrlViewCount, String>.Context ctx, Collector<String> out) throws Exception {
            // 来一个数据就加入状态列表中 UrlViewCount{url='./prod?id=2', count=3, windowStart=1678676045000, windowEnd=1678676055000}
//            System.out.println("value: " + value);
            urlViewCountListState.add(value);

            // 这个key是windowEnd 同一个窗口end 说明在同一个窗口 就在这个窗口排序 同一个定时器 过了时间windowEnd+1就执行
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
//            System.out.println("ctx.getCurrentKey():" + ctx.getCurrentKey());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception{
            ArrayList<UrlViewCount> urlViewCountArrayList = new ArrayList<>();

            for (UrlViewCount urlViewCount : urlViewCountListState.get()){
                urlViewCountArrayList.add(urlViewCount);
            }

            urlViewCountListState.clear(); // 一个窗口内的数据已经全部赋值给了列表 把这个状态列表清空 让他去装其他窗口的数据（状态）

            // 接下来就对该窗口的列表进行排序咯
            // 也可以写为lambda表达式
            urlViewCountArrayList.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });

//            System.out.println("urlViewCountArrayList:" + urlViewCountArrayList);
            StringBuilder result = new StringBuilder();

            result.append("=================================================\n");
            result.append("窗口结束时间:").append(new Timestamp(timestamp - 1)).append("\n");
            for (int i = 0; i < this.n; i=i+1){         // 这个循环 关于i 有点问题！！！
                UrlViewCount UrlViewCount = urlViewCountArrayList.get(i);
                String info ="No." + (i +1) + " "
                        + "url:" + UrlViewCount.url + " "
                        +"浏览量:" + UrlViewCount.count + "\n";
                result.append(info);
            }
            result.append("=================================================\n");
            out.collect(result.toString());
        }
    }
}