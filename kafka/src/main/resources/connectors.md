(Database / API / external system)
↓
🟢 **Source Connector**  → **read and write to Kafka**
↓
🔵 **Transformations **  → **modify / filter / transform**
↓
🟠 **Kafka Streams / KSQL **  → **advanced aggregate、filter**
↓
🔵 **Transformations **  → **modify/ format transform**
↓
🟣 **Sink Connector**  → **write data to the system**
↓
(relational database / NoSQL / BigQuery / Elasticsearch / S3 / other external system)