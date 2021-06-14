import apache_beam as beam
from apache_beam.io.gcp.gcsio import GcsIO
from datetime import datetime

# Split the CSV input data and return the fields of interest for the next transform
class SplitCSV(beam.DoFn):
    def process(self, element):
        timestamp, origin, destination, transaction_amount = element.split(",")
        date_time_obj = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S UTC')
        return [{
            'date': date_time_obj.strftime("%Y/%m/%d"),
            'timestamp': date_time_obj,
            'amount': float(transaction_amount)
        }]

# Transform the data into a format that can be output to JSON
class Output(beam.DoFn):
    def process(self, element):
        response = {
            'date': element[0],
            'total_amount': element[1]
        }
        return [response]


def filterByMinTransactionValue(item, minTransactionValue):
    return item['amount'] > minTransactionValue


def filterByMinDateValue(item, minDate):
    return item['timestamp'] >= minDate


# Create the pipeline and run it
with beam.Pipeline() as pipeline:
    lines = (pipeline
             | 'ReadMyFile' >> beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv', skip_header_lines=1)
             | beam.ParDo(SplitCSV()))
    minTransactionTransform = lines | 'Filter min transaction' >> beam.Filter(
        filterByMinTransactionValue, 20)
    minDateTransform = minTransactionTransform | 'Filter min date' >> beam.Filter(
        filterByMinDateValue, datetime(2009, 12, 31))
    sumRecordsByDate = (minDateTransform | 'Map to tuple keyed by date' >>
                        beam.Map(lambda item: (item['date'], item['amount']))
                        | beam.CombinePerKey(sum))
    _ = sumRecordsByDate | beam.ParDo(Output()) | beam.io.WriteToText(
        "output/results.json", shard_name_template='', file_name_suffix=".gz")
