# AIOps Assessment: Basic Monitoring and Event Processing

## Scenario
This project simulates a basic AIOps workflow for a payment service that records operational metrics and log events. The service is expected to process requests normally during steady state, but an unexpected slowdown or infrastructure issue can produce high latency, high CPU usage, high memory usage, and error-level log events. The goal of the workflow is to detect those anomalies, turn them into event messages, pass them through the simulated producer/topic/consumer pipeline, and surface the resulting operational problem in a final AIOps output.

## Operational data
The repository includes synthetic telemetry in `data/service_data.json`. Each record contains the following fields:

- `timestamp`: the time of the observation
- `service`: the service being monitored
- `response_time_ms`: request latency in milliseconds
- `cpu_percent`: percentage of CPU utilization
- `memory_percent`: percentage of memory utilization
- `log_level`: severity label such as `INFO` or `ERROR`
- `message`: human-readable log message

The metrics are the numeric fields `response_time_ms`, `cpu_percent`, and `memory_percent`. The log information is represented by `log_level` and `message`.

## Observations from the logs and metrics
The operational data shows a normal pattern during the first several minutes: response times remain near 120-145 ms, CPU stays around 42-50%, memory stays around 51-57%, and the log level is `INFO` with successful payment messages.

The unusual behavior begins at `2026-09-20T10:05:00` when the service shows a response time of 610 ms, CPU at 75%, memory at 70%, and an `ERROR` log message: `Payment service timeout`. The next record at `2026-09-20T10:06:00` is even more severe, with a response time of 640 ms, CPU at 94%, memory at 91%, and `Database connection timeout`. These values clearly differ from the normal steady-state behavior and represent the operational issue the AIOps workflow should detect.

## Anomaly detection findings
The `AnomalyDetector` class uses thresholds for response time, CPU utilization, and memory utilization. It then inspects the log level and flags `ERROR` events as relevant operational signal. The detected anomalies are the records at `10:05:00` and `10:06:00`, where the service exceeds the latency and resource thresholds and emits error logs. The detector correctly distinguishes those from the earlier `INFO` records, which are treated as normal.

During validation, the detector provides enough detail for the alert: the event includes a timestamp, service name, anomaly type, and a reasons list describing why it was raised. This makes the output readable and actionable.

## Event-processing flow
The simulated event flow is:

1. Operational data is read.
2. `AnomalyDetector.detect()` checks each record.
3. When a record is anomalous, it becomes an event dictionary.
4. `EventProducer` publishes the event to a topic.
5. `EventConsumer` reads the messages from that same topic.
6. The AIOps pipeline returns the processed output.

The key components are:

- `EventProducer`: responsible for publishing events.
- `EventTopic`: the in-memory topic where messages are stored.
- `EventConsumer`: responsible for consuming the messages from the topic.
- `Event/message`: the anomaly object passed through the pipeline.

## Final workflow result
Running the pipeline over `data/service_data.json` produces 10 records processed and 2 anomalies detected. The final output reflects the service degradation and includes the relevant error events from the time window when the application was timing out and exhausting resources.

## Issues identified and corrected
Two primary issues prevented the workflow from working as intended:

1. In `src/anomaly_detector.py`, the detector was incorrectly checking for `WARNING` logs instead of `ERROR` logs. This caused the wrong log condition to be evaluated and prevented the true anomaly signal from being recognized.
2. In `src/aiops_pipeline.py`, the producer and consumer were connected to different topic names, so the consumer never received the event that was produced.

Both issues were corrected while leaving the existing architecture in place.

## Limitation and possible improvement
The current detection logic is rule-based and threshold-driven. It is easy to understand and works well for this synthetic dataset, but it would miss more complex anomaly patterns such as gradual degradation, multi-service correlations, or seasonal variations without additional context. A possible improvement would be to introduce baseline-based or statistical detection using rolling averages and alert severity scoring.

## Reproduction steps
Run the following commands from the project root:

```bash
cd /workspaces/github-skills-challenge
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest -q
python src/aiops_pipeline.py
```

Expected behavior:

- The unit tests pass.
- The pipeline reads the operational data.
- The anomaly detector flags the timeout and resource saturation events.
- The event producer publishes the anomaly events.
- The consumer reads the same messages from the topic.
- The output shows the final AIOps result.

## Validation
The repository validation confirms that normal records are not treated as anomalies, abnormal metric behavior is identified, events are produced, and the in-memory producer/topic/consumer flow functions correctly.

## Notes
This repository was updated to keep the assessment architecture intact while fixing the faults that prevented the end-to-end AIOps workflow from operating correctly.
