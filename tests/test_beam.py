from apache_beam.testing.util import assert_that, is_not_empty
from apache_beam.testing.util import equal_to
import apache_beam
from apache_beam.testing.test_pipeline import TestPipeline
import unittest
from src.beam import SumTransactionByValueAndDate
from src.beam import SplitCSV


class SumTransactionByValueAndDateTest(unittest.TestCase):
    DATA = ([
        '2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99',
        '2009-12-31 23:59:59 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,30.00',
        '2010-01-01 00:00:00 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,30.00',
        '2017-01-01 04:22:23 UTC, wallet00000e719adfeaa64b5a, wallet00001e494c12b3083634, 19.95',
        '2017-01-01 04:23:23 UTC, wallet00000e719adfeaa64b5a, wallet00001e494c12b3083634, 20.00',
        '2017-01-01 04:23:24 UTC, wallet00000e719adfeaa64b5a, wallet00001e494c12b3083634, 30.00',
        '2017-01-01 04:23:25 UTC, wallet00000e719adfeaa64b5a, wallet00001e494c12b3083634, 35.00',
        ])

    def test_sumTransaction(self):
        p = TestPipeline()
        input = p | apache_beam.Create(self.DATA) | apache_beam.ParDo(SplitCSV())
        output = input | SumTransactionByValueAndDate()
        assert_that(output, equal_to([
            {'date': '2010/01/01', 'total_amount': 30.0},
            {'date': '2017/01/01', 'total_amount': 65.0}
            ]))
            
        p.run()


if __name__ == '__main__':
    unittest.main()
