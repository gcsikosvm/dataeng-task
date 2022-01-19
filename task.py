#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
import csv
import datetime
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class FilterHeaderDoFn(beam.DoFn):
    def process(self, element):
        try:
            float(element[3])
            yield element
        except ValueError as e:
            return


class FilterTransactionLT20DoFn(beam.DoFn):
    def process(self, element):
        if element[1] > 20.0:
            yield element


class StandardiseRowsDoFn(beam.DoFn):
    """Convert to date, amount to float, catch convert exception to filter the header"""
    def process(self, element):
        try:
            Date, _, _, Amount = element
            yield datetime.datetime.strptime(Date, "%Y-%m-%d %H:%M:%S %Z").date(), float(Amount)
        except:
            pass


class DateToStringDoFn(beam.DoFn):
    def process(self, element):
        yield str(element[0]), element[1]


class ToStringFormatDoFn(beam.DoFn):
    """"""
    def process(self, element):
        yield ', '.join([element[0], str(*element[1])])


def parse_file(element):
    for line in csv.reader([element], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL,
                           skipinitialspace=True):
        return line


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        default='./output/results',
        required=False,
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:

        parsed_csv = (
                p
                | 'Read input file' >> beam.io.ReadFromText(known_args.input)
                | 'Parse file' >> beam.Map(parse_file)
        )
        filtered_input = parsed_csv | 'Filter Input Required' >> beam.ParDo(StandardiseRowsDoFn())
        not_lt_20 = filtered_input | 'Filter Transactions < 20' >> beam.ParDo(FilterTransactionLT20DoFn())
        not_before_2010 = not_lt_20 | 'Filter Year < 2010' >> beam.Filter(
            lambda x: x[0] > datetime.datetime.strptime('2010', '%Y').date())
        final_transform = not_before_2010 | 'Date to str' >> beam.ParDo(DateToStringDoFn())
        final_transform = final_transform | 'SUM per Date' >> beam.GroupByKey(sum)
        final_transform = final_transform | 'Transform to Output to string' >> beam.ParDo(ToStringFormatDoFn())
        final_transform | 'Write to gzipped file with header' >> beam.io.WriteToText(known_args.output, file_name_suffix='jsonl.gz',
                                                   header='date, total_amount',
                                                   compression_type=beam.io.filesystem.CompressionTypes.GZIP)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
