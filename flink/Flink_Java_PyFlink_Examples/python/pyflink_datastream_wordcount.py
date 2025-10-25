# PyFlink DataStream WordCount
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    text = env.from_collection([
        "To be or not to be",
        "That is the question",
        "To be Or To do",
    ], type_info=Types.STRING())

    counts = (text
        .flat_map(lambda line: [(w.lower(), 1) for w in filter(None, __import__('re').split(r"\W+", line))],
                  result_type=Types.TUPLE([Types.STRING(), Types.INT()]))
        .key_by(lambda x: x[0])
        .sum(1))

    counts.print()
    env.execute("pyflink_datastream_wordcount")

if __name__ == "__main__":
    main()
