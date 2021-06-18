import apache_beam as beam
from datetime import date, datetime
from decimal import Decimal
from dataclasses import dataclass
import json
import csv
import io

#Dataclass representing the input data
@dataclass
class TransactionData:
    date: str
    timestamp: datetime
    amount: Decimal

# Split the CSV input data and return the fields of interest for the next transform
class SplitCSV(beam.DoFn):
    def process(self, element):
        csvReader = csv.reader([element], delimiter=',')
        item = next(csvReader)
        date_time_obj = datetime.strptime(item[0], '%Y-%m-%d %H:%M:%S UTC')
        return [TransactionData(date_time_obj.strftime("%Y-%m-%d"), date_time_obj,Decimal(item[3]) )]

# Transform the data into a format that can be output to JSON
class Output(beam.DoFn):
    def process(self, element):
        jsonObj = json.dumps((element[0],str(element[1])))
        return [jsonObj]


def filterByMinTransactionValue(item: TransactionData, minTransactionValue: Decimal):
    return item.amount > minTransactionValue


def filterByMinDateValue(item: TransactionData, minDate: datetime):
    return item.timestamp >= minDate

#composite transform grouping together the filter operations
#and the sum operation.
@beam.ptransform_fn
def SumTransactionByValueAndDate(pcoll):
    return (
        pcoll
        | 'Filter min transaction' >> beam.Filter(
          filterByMinTransactionValue, Decimal(20.00))
        | 'Filter min date' >> beam.Filter(
          filterByMinDateValue, datetime(2010, 1, 1))
        | 'Map to tuple keyed by date' >>
        beam.Map(lambda item: (item.date, item.amount))
        | beam.CombinePerKey(sum)
        | beam.ParDo(Output())
    )


def run():
    # Create the pipeline and run it
    with beam.Pipeline() as pipeline:
      lines = (pipeline
               | 'ReadMyFile' >> beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv', skip_header_lines=1)
               | beam.ParDo(SplitCSV()))
      _ = (lines | SumTransactionByValueAndDate()
           | beam.io.WriteToText("output/results.jsonl", shard_name_template='', file_name_suffix=".gz"))

if __name__ == '__main__':
    run()
