package top.anets.flink.job.uv_example;

/**
 * @author ftm
 * @date 2023-11-13 14:54
 */
public class Event {
    public String user;

    public String url;
    public long time;

    public Event() {
    }

    public Event(String user, String url, long time) {
        this.user = user;
        this.url = url;
        this.time = time;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", time=" + time +
                '}';
    }

}
