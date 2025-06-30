import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import json
import logging


# 🔹 Parse JSON and filter required fields
class ParseEvent(beam.DoFn):
    def process(self, element):
        try:
            row = json.loads(element)
            if all(k in row for k in ['user_id', 'event_type', 'event_time']):
                yield {
                    'user_id': row['user_id'],
                    'event_type': row['event_type'],
                    'event_time': row['event_time']  # must be RFC3339 timestamp string
                }
        except Exception as e:
            logging.warning(f"Invalid record skipped: {element} | Error: {e}")


def run():
    # 🔹 Setup Beam pipeline options
    pipeline_options = PipelineOptions(
        streaming=True,
        project='oa-apmena-spacecsb2b-ap-dv',
        region='asia-southeast1',
        temp_location='gs://pubsub-dataflow-bq',
        staging_location='gs://pubsub-dataflow-bq/staging',
        service_account_email = 'spacecsb2b-sa-workflows-dv@oa-apmena-spacecsb2b-ap-dv.iam.gserviceaccount.com',
        job_name='pubsub-to-bq-streaming-job'
    )
    pipeline_options.view_as(StandardOptions).streaming = True

    # 🔹 Define your Pub/Sub topic and BigQuery table
    input_topic = 'projects/oa-apmena-spacecsb2b-ap-dv.pubsub_to_bq/topics/user-events'
    output_table = 'oa-apmena-spacecsb2b-ap-dv.pubsub_to_bq.user_events'

    # 🔹 Start pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Read from PubSub' >> beam.io.ReadFromPubSub(topic=input_topic).with_output_types(bytes)
            | 'Decode UTF-8' >> beam.Map(lambda x: x.decode('utf-8'))
            | 'Parse and Filter Events' >> beam.ParDo(ParseEvent())
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                table=output_table,
                schema='user_id:STRING, event_type:STRING, event_time:TIMESTAMP',
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )


if __name__ == '__main__':
    run()
