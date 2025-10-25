# PyFlink ProcessFunction example
# Counts per key and emits every 5 seconds using a processing-time timer.
from typing import Tuple, Optional
from pyflink.common import Types, Time
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

class RollingCount(KeyedProcessFunction):
    def __init__(self, period_ms: int = 5000):
        self.period_ms = period_ms
        self.count_state = None
        self.next_timer_state = None

    def open(self, ctx: RuntimeContext):
        self.count_state = ctx.get_state(ValueStateDescriptor('count', Types.INT()))
        self.next_timer_state = ctx.get_state(ValueStateDescriptor('next_timer', Types.LONG()))

    def process_element(self, value: Tuple[str, int], ctx: 'KeyedProcessFunction.Context'):
        c = self.count_state.value() or 0
        self.count_state.update(c + value[1])
        next_timer = self.next_timer_state.value()
        if next_timer is None:
            t = ctx.timer_service().current_processing_time() + self.period_ms
            ctx.timer_service().register_processing_time_timer(t)
            self.next_timer_state.update(t)

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        c = self.count_state.value() or 0
        ctx.output(f"{ctx.get_current_key()},{c}")
        # schedule next
        t = timestamp + self.period_ms
        ctx.timer_service().register_processing_time_timer(t)
        self.next_timer_state.update(t)

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    ds = env.from_collection(
        [("alice", 1), ("bob", 1), ("alice", 1), ("bob", 1), ("alice", 1)],
        type_info=Types.TUPLE([Types.STRING(), Types.INT()])
    )
    ds.key_by(lambda x: x[0])       .process(RollingCount())       .print()
    env.execute("pyflink_process_function")

if __name__ == "__main__":
    main()
