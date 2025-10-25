# INTRODUCTION
# This example demonstrates a SESSION window using EVENT TIME with WATERMARKS.
# - Event time: timestamps come from records, making results deterministic across replays.
# - Watermarks: allow the window to close even with out-of-order events (here: 5s lateness).
# - Aggregation: per-user sum printed for each evaluated window.
# See README.md for diagrams and best practices.

# PyFlink Session Window (DataStream)
from datetime import timedelta
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.watermark_strategy import WatermarkStrategy
from pyflink.java_gateway import get_gateway

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    data = [
        ("alice", 10, 1700000000000),
        ("alice", 15, 1700000008000),
        ("bob",    5, 1700000010000),
        ("alice", 20, 1700000030000),
        ("bob",    7, 1700000035000),
        ("carol",  3, 1700000042000),
    ]
    ds = env.from_collection(data, type_info=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()]))
    wm = WatermarkStrategy.for_bounded_out_of_orderness(timedelta(seconds=5)).with_timestamp_assigner(lambda e, ts: e[2])
    ds = ds.assign_timestamps_and_watermarks(wm)

    gw = get_gateway()
    Time = gw.jvm.org.apache.flink.streaming.api.windowing.time.Time
    EventTimeSessionWindows = gw.jvm.org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows

    (ds.key_by(lambda e: e[0])
       .window(EventTimeSessionWindows.withGap(Time.seconds(30)))
       .reduce(lambda a,b: (a[0], a[1]+b[1], max(a[2], b[2])))
       .map(lambda e: f"SESSION(user={e[0]}, session_sum={e[1]})", output_type=Types.STRING())
       .print())

    env.execute("pyflink_session_window")

if __name__ == "__main__":
    main()
