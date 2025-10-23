# Confluent Kafka Interview Kit

Generated on 2025-10-23 20:29. This repo includes:

- **Docs**: Basic / Intermediate / Advanced interview Q&A.
- **Code**: Producer/Consumer (Python, Java), Streams, and CLI cheat-sheet.
- **Kafka Connect**: Source/Sink templates + SMT examples.
- **ksqlDB**: Common statements, joins, windowing.
- **Notebook**: Hands-on walkthrough (Markdown + code cells).
- **Packaging**: Ready to push to Git.

> Note: Code is illustrative and uses safe defaults. Adjust bootstrap servers, security configs, and topic names for your environment.


## Quick demo
```bash
make up            # or: make up-kraft
make ksql          # create ORDERS + ORDERS_HIGH
make demo          # produce test data
make connect       # file source & sink
./connect/scripts/post_s3_sink.sh  # optional S3 sink
```
