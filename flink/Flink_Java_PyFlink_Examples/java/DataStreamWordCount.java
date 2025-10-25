/*
 * DataStream WordCount example (Java)
 * Simple flatMap + keyBy + sum over a bounded stream.
 */
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class DataStreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.fromElements(
                "To be or not to be",
                "That is the question",
                "To be Or To do"
        );

        DataStream<Tuple2<String, Integer>> counts = text
            .flatMap(new Tokenizer())
            .keyBy(t -> t.f0)
            .sum(1);

        counts.print();
        env.execute("DataStreamWordCount");
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String,Integer>> out) {
            for (String w : line.toLowerCase().split("\W+")) {
                if (!w.isEmpty()) out.collect(Tuple2.of(w, 1));
            }
        }
    }
}
