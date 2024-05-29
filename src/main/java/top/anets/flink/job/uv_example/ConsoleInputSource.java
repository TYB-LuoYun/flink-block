package top.anets.flink.job.uv_example;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Scanner;

/**
 * @author ftm
 * @date 2023-11-15 15:29
 */
public class ConsoleInputSource implements SourceFunction<String> {
    private boolean running = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        Scanner scanner = new Scanner(System.in);

        while (running) {
            // 从标准输入读取数据，然后发射到 Flink 的数据流
            String data = scanner.nextLine();
            ctx.collect(data);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}