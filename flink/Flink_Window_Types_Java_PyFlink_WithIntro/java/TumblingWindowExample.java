/*
 * INTRODUCTION
 * This example demonstrates a TUMBLING window using EVENT TIME with WATERMARKS.
 * - Event time: timestamps come from records, making results deterministic across replays.
 * - Watermarks: allow the window to close even with out-of-order events (here: 5s lateness).
 * - Aggregation: per-user sum printed for each evaluated window.
 * See README.md for diagrams and best practices.
 */
/*
 * Apache Flink Windowing Examples (Java)
 * Demonstrates Tumbling, Hopping(Sliding), and Session windows with event-time + watermarks.
 * Tested with Flink 1.17+/1.18+ style APIs.
 */
import java.time.Duration;
import java.util.Arrays;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
*/

class Event {
    public String user;
    public int amount;
    public long ts; // epoch millis

    public Event() {}

    public Event(String user, int amount, long ts) {
        this.user = user;
        this.amount = amount;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "Event{" + user + "," + amount + "," + ts + "}";
    }
}

class SumReduce implements ReduceFunction<Event> {
    @Override
    public Event reduce(Event a, Event b) {
        return new Event(a.user, a.amount + b.amount, Math.max(a.ts, b.ts));
    }
}

class Common {
    static WatermarkStrategy<Event> wm() {
        return WatermarkStrategy
            .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((SerializableTimestampAssigner<Event>)(e, ts) -> e.ts);
    }

    static DataStream<Event> sample(StreamExecutionEnvironment env) {
        // timestamps are illustrative
        return env.fromElements(
            new Event("alice", 10, 1700000000000L),
            new Event("alice", 15, 1700000008000L),
            new Event("bob",   5,  1700000010000L),
            new Event("alice", 20, 1700000030000L),
            new Event("bob",   7,  1700000035000L),
            new Event("carol", 3,  1700000042000L)
        ).assignTimestampsAndWatermarks(wm());
    }
}

public class TumblingWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> stream = Common.sample(env);

        stream
            .keyBy(e -> e.user)
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .reduce(new SumReduce())
            .map(e -> "TUMBLING(user=" + e.user + ", total=" + e.amount + ")")
            .print();

        env.execute("TumblingWindowExample");
    }
}
