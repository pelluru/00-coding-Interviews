/*
 * ProcessFunction example (Java, Flink DataStream API)
 * Counts events per key with KeyedProcessFunction and emits a rolling count every 5 seconds (processing time).
 *
 * Compile with Flink dependencies (e.g., Maven) and run as a standalone job.
 */
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Stream of (user, value)
        DataStream<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("alice", 1), Tuple2.of("bob", 1),
                Tuple2.of("alice", 1), Tuple2.of("alice", 1),
                Tuple2.of("bob", 1), Tuple2.of("charlie", 1)
        );

        input
            .keyBy(t -> t.f0)
            .process(new RollingCountProcessFn(5000L)) // emit every 5s (processing-time)
            .print();

        env.execute("ProcessFunctionExample");
    }

    // Emits "key,count" every 'periodMs' using a processing-time timer
    public static class RollingCountProcessFn extends KeyedProcessFunction<String, Tuple2<String,Integer>, String> {
        private final long periodMs;
        private transient ValueState<Integer> countState;
        private transient ValueState<Long> nextTimerState;

        public RollingCountProcessFn(long periodMs) { this.periodMs = periodMs; }

        @Override
        public void open(Configuration parameters) {
            countState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("count", Types.INT));
            nextTimerState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("nextTimer", Types.LONG));
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
            Integer c = countState.value();
            if (c == null) c = 0;
            countState.update(c + value.f1);

            Long next = nextTimerState.value();
            if (next == null) {
                long trigger = ctx.timerService().currentProcessingTime() + periodMs;
                ctx.timerService().registerProcessingTimeTimer(trigger);
                nextTimerState.update(trigger);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Integer c = countState.value();
            if (c == null) c = 0;
            out.collect(ctx.getCurrentKey() + "," + c);
            // schedule next
            long trigger = timestamp + periodMs;
            ctx.timerService().registerProcessingTimeTimer(trigger);
            nextTimerState.update(trigger);
        }
    }
}
