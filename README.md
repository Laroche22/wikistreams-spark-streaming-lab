# WikiStreams - Spark Structured Streaming Lab

This project reads live Wikipedia edits, sends them to Kafka and aggregates them with Spark Structured Streaming.

Repository: https://github.com/Laroche22/wikistreams-spark-streaming-lab

## Team responsibilities

- **Member A (Laroche):** Docker environment, topic creation, producer execution and integration testing.
- **Member B:** tumbling and sliding windows, and event-time versus processing-time analysis.
- **Member C:** producer filters, edit-size metrics and the final lab report.
- **Together:** integrate the completed work and verify the final pipeline.

## Verified progress

The initial demonstration was tested successfully on 16 October 2026:

1. Kafka and the Jupyter container started with Docker Compose.
2. The `wikistreams` topic was created with one partition and replication factor one.
3. The producer read live edit events from `fr.wikipedia.org`.
4. Spark consumed the JSON messages from Kafka and printed windowed counts by the `bot` field.

Observed console output for batch 0:

| Window start | Window end | bot | edit_count |
| --- | --- | --- | --- |
| 2026-10-16 10:00:00 | 2026-10-16 11:00:00 | false | 173 |
| 2026-10-16 10:00:00 | 2026-10-16 11:00:00 | true | 57 |

This snapshot contains **230 processed edits**. The displayed one-hour window groups the received edits; it does not mean that a full hour of source events was collected. Counts vary between runs.

The initial demo uses a one-hour tumbling window. The additional window experiments, business metrics, filters and final integration tests are still to be completed by the team.

## Project files

| Path | Purpose |
| --- | --- |
| `compose.yaml` | Kafka and PySpark/Jupyter services and their network. |
| `admin.py` | Creates `wikistreams`, waits for confirmation and handles an existing topic. |
| `wikistream_producer.py` | Reads Wikimedia events and sends JSON messages to Kafka. |
| `notebooks/wikistream_pyspark.ipynb` | Spark consumer and the initial windowed aggregation. |
| `requirements.txt` | Python dependencies for the admin script and producer. |
| `data/` | Shared data directory, mounted at `/home/jovyan/data`. |
| `jobs/` | Shared jobs directory, mounted at `/home/jovyan/jobs`. |

## Tested environment and shared settings

| Component or setting | Tested value |
| --- | --- |
| Host | Windows PowerShell and Docker Desktop with Linux containers |
| Docker Compose | 2.40.3-desktop.1 |
| Kafka image | `apache/kafka-native:4.1.1` |
| Notebook image | `quay.io/jupyter/pyspark-notebook` |
| Spark | 4.2.0 |
| Scala | 2.13.18 |
| Python inside the notebook container | 3.13 |
| Kafka Python client | `confluent-kafka==2.16.0` |
| Pywikibot | `pywikibot[eventstreams]==11.8.0` |
| Topic | `wikistreams` |
| Kafka address inside the Compose network | `kafka:19092` |
| Kafka address from the host | `localhost:9092` |
| Jupyter from the host | `http://localhost:8888` |

GitHub shares source files. It does not share a running local Kafka instance. Each member must start their own Docker environment when testing locally.

The commands below run the Python scripts inside the Jupyter container. Their Kafka address must therefore be `kafka:19092`, as in the Spark notebook.

## Start the environment

Run the following commands in **PowerShell from the project root**. Docker Desktop must be running.

Create the required directories, including any empty directories not present after cloning:

```powershell
New-Item -ItemType Directory -Force -Path data,jobs,notebooks
docker compose up -d
docker compose ps
```

The containers are named `kafka` and `pyspark_notebook`. Both should be running, and the notebook container should become healthy.

Display the Jupyter access links:

```powershell
docker logs --tail 50 pyspark_notebook
```

Open the complete localhost URL shown in the logs in your browser. Keep its login token private. In Jupyter, open `work/wikistream_pyspark.ipynb`.

## Check Spark and install Python dependencies

Check the Spark distribution:

```powershell
docker exec pyspark_notebook pyspark --version
```

The tested image contains Spark 4.2.0 and Scala 2.13. The supplied Compose file uses an image without a fixed tag, so check the version on a new machine. The Kafka connector in the Spark initialization cell must match that Spark version.

For the verified environment, the initialization cell is:

```python
# Initialize Spark with the matching Kafka connector
spark = SparkSession.builder \
    .appName("KafkaStreamingPrototype") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0") \
    .getOrCreate()

print("Spark version:", spark.version)
```

Run the imports cell before this initialization cell. The first initialization downloads the Kafka connector; its detailed logs appear in Docker logs.

PySpark imports were verified in the notebook. A plain `docker exec ... python -c "import pyspark"` command did not find the module in this image; use the notebook and the Spark launcher for the checks above.

Install the admin and producer dependencies:

```powershell
docker cp .\requirements.txt pyspark_notebook:/tmp/requirements.txt
docker exec pyspark_notebook python -m pip install -r /tmp/requirements.txt
```

Repeat this dependency installation when creating a new notebook container.

## Create the topic

Copy and run the admin script:

```powershell
docker cp .\admin.py pyspark_notebook:/tmp/admin.py
docker exec pyspark_notebook python -u /tmp/admin.py
```

The script prints either `Topic 'wikistreams' created successfully.` or `Topic 'wikistreams' already exists.`

The project root is not mounted by the supplied Compose file. Repeat the relevant `docker cp` command after editing a root-level Python script.

## Run the producer

The initial producer uses:

```python
stream = EventStreams(streams=["recentchange"])
stream.register_filter(server_name="fr.wikipedia.org", type="edit")
```

The fixed historical `since` argument was removed so the demo follows current changes. Messages use JSON encoded as UTF-8 and are sent to `wikistreams`.

Copy and run the script:

```powershell
docker cp .\wikistream_producer.py pyspark_notebook:/tmp/wikistream_producer.py
docker exec pyspark_notebook python -u /tmp/wikistream_producer.py
```

The supplied producer collects events for approximately ten minutes, then waits one minute before closing. A blocking wait for the next event can extend the collection time.

## Run the Spark consumer and inspect results

In Jupyter, run the notebook cells in order:

1. Imports.
2. Spark initialization with the matching connector.
3. Kafka source `raw_df`.
4. JSON schema and `parsed_df`.
5. Windowed aggregation `windowed_counts_df`.
6. Console query `streaming_query_df`.

The source uses `startingOffsets="earliest"`, which allowed the demo to consume the stored Kafka messages after the producer had finished. Console output uses complete mode.

View recent output from a PowerShell terminal:

```powershell
docker logs --tail 80 pyspark_notebook
```

To follow new output continuously:

```powershell
docker logs -f pyspark_notebook
```

Press Ctrl+C in that terminal to stop following the logs. This leaves the streaming query running.

Before rerunning the query-start cell, stop the existing query in a notebook cell:

```python
streaming_query_df.stop()
```

Save the notebook with Ctrl+S. Its `work` directory is mounted to the host's `notebooks` directory.

## Stop and resume

After stopping active notebook queries and letting the producer finish:

```powershell
docker compose stop
```

To resume the existing containers:

```powershell
docker compose start
```

## Final integration work

Once members B and C have added their code:

- Confirm that all scripts and notebook cells use the same topic, schema and Docker Kafka address.
- Run both tumbling and overlapping/sliding window explorations.
- Check the event-time and processing-time aggregations.
- Verify the selected language or page filters and the edit-size metrics.
- Record the actual results and execution commands for the final report.

The initial demonstration result above validates the base pipeline. It does not replace the final integration test of the team's extensions.
