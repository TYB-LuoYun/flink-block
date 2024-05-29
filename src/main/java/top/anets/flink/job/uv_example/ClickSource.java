package top.anets.flink.job.uv_example;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @author ftm
 * @date 2023-11-13 14:55
 * 需要自定义源算子 模拟网站实时访问日志
 */
public class ClickSource implements SourceFunction<Event> {

    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();
        String[] users = {"Mary","Alice","Bob","Cary"};
        String[] urls = {"./home", "./cart","./fav", "./prod?id=1","./prod?id=2"};

        while (running) {
            ctx.collect(new Event(
                    users[random.nextInt(users.length)],      // user 和 url 随机组合
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()  //getTimeInMillis 方法返回当前时间
            ));

            // 在这个循环中 每隔一秒 就collect（发送)一个数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}