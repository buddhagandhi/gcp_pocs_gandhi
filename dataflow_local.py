import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
import io
import json
import datetime


class ParseCSVWithDynamicHeader(beam.DoFn):
    def __init__(self, header_side_input):
        self.header_side_input = header_side_input

    def process(self, line, header):
        if not line.strip():
            return
        reader = csv.DictReader(io.StringIO(line), fieldnames=header.split(','))
        for row in reader:
            yield row


def run():
    options = PipelineOptions(runner='DirectRunner')
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"output/output_{timestamp}"

    with beam.Pipeline(options=options) as p:
        # Read all lines
        lines = p | 'Read CSV' >> beam.io.ReadFromText('sample.csv')

        # Get header (first line) as side input
        header = (
            lines
            | 'Sample First Line' >> beam.combiners.Sample.FixedSizeGlobally(1)
            | 'Extract Header String' >> beam.Map(lambda x: x[0] if x else "")
        )

        # Filter out header line from data
        data_lines = lines | 'Skip Header Line' >> beam.Filter(lambda line: not line.startswith('user_id'))

        (
            data_lines
            | 'Parse CSV with Header' >> beam.ParDo(ParseCSVWithDynamicHeader(header), header=beam.pvalue.AsSingleton(header))
            | 'Convert to JSON string' >> beam.Map(json.dumps)
            | 'Write JSON Output' >> beam.io.WriteToText(
                output_path,
                file_name_suffix='.json',
                shard_name_template='-SS-of-NN'
            )
        )


if __name__ == '__main__':
    run()
