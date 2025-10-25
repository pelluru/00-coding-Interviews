/*
 * Table/SQL API example (Java)
 * Creates a TableEnvironment, performs a 1-minute tumbling window aggregation over a DataStream
 * converted to a dynamic table, and prints the results.
 */
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.time.Duration;
import static org.apache.flink.table.api.Expressions.*;

public class TableSqlExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // (user, amount, eventTimeMillis)
        DataStream<Tuple3<String, Double, Long>> source = env.fromElements(
                Tuple3.of("alice", 10.0, 1_700_000_000_000L),
                Tuple3.of("alice", 20.0, 1_700_000_030_000L),
                Tuple3.of("bob",   5.0,  1_700_000_020_000L)
        ).assignTimestampsAndWatermarks(
            WatermarkStrategy.<Tuple3<String,Double,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String,Double,Long>>) (e, ts) -> e.f2)
        );

        tEnv.createTemporaryView("payments",
                source,
                $("user"),
                $("amount"),
                $("ts").rowtime());

        Table result = tEnv.from("payments")
            .window(Tumble.over(lit(60).seconds()).on($("ts")).as("w"))
            .groupBy($("user"), $("w"))
            .select($("user"), $("w").start().as("window_start"), $("w").end().as("window_end"), $("amount").sum().as("total"));

        // Print to console
        tEnv.toChangelogStream(result, Types.ROW(Types.STRING, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP, Types.DOUBLE)).print();
        env.execute("TableSqlExample");
    }
}
