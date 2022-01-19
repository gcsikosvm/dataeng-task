import unittest

import apache_beam as beam

from task import parse_file, Task2Transform
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class WordCountTest(unittest.TestCase):


    EXPECTED_OUTPUT = [
        '2017-03-18, 2102.22',
        '2017-08-31, 13700000023.08',
        '2018-02-27, 129.12',
        '2018-02-28, 60.0', #test for combine
        '2010-01-01, 20.99' #test for date
    ]

    def test_count_words(self):
        with TestPipeline() as p:
            input = (
                    p
                    | 'Read input file' >> beam.io.ReadFromText('./fixture.csv')
                    | 'Parse file' >> beam.Map(parse_file)
            )
            output = input.apply(Task2Transform())

            assert_that(output, equal_to(self.EXPECTED_OUTPUT), label='CheckOutput')
