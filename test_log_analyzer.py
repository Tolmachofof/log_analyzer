import datetime
import unittest
from unittest import mock

from log_analyzer import (
    CONFIG, get_last_log, _fetch_date_from_file_name, LogEntry
)


class TestGetLastLog(unittest.TestCase):

    def setUp(self):
        self.logs_dir = CONFIG['LOGS_DIR']
        self.log_name_template = CONFIG['LOG_NAME_TEMPLATE']
        self.date_format = CONFIG['LOG_DATE_FORMAT']

    def test_for_empty_directory(self):
        """Should return None if the logs directory is empty"""
        with mock.patch('os.listdir', mock.MagicMock(return_value=[])):
            self.assertIsNone(
                get_last_log(
                    self.logs_dir, self.log_name_template, self.date_format
                )
            )

    def test_return_last_log_entry_in_directory(self):
        """
        Should return a last log that matches pattern and date format
        in the logs directory
        """
        early_date = datetime.datetime.strptime('01.01.2018', '%d.%m.%Y')
        late_date = datetime.datetime.strptime('02.01.2018', '%d.%m.%Y')
        early_log = self.log_name_template.format(
            date=early_date.strftime(self.date_format)
        )
        late_log = self.log_name_template.format(
            date=late_date.strftime(self.date_format)
        )
        with mock.patch(
                'os.listdir',
                mock.MagicMock(return_value=[early_log, late_log])
        ):
            self.assertEqual(
                get_last_log(
                    self.logs_dir, self.log_name_template, self.date_format
                ),
                LogEntry(log_name=late_log, log_date=late_date)
            )


class TestFetchDateFromFileName(unittest.TestCase):

    def setUp(self):
        self.log_name_template = CONFIG['LOG_NAME_TEMPLATE']
        self.date_format = CONFIG['LOG_DATE_FORMAT']

    def test_return_date_for_correct_filename(self):
        expected_date = datetime.datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        correct_filename = self.log_name_template.format(
            date=expected_date.strftime(self.date_format)
        )
        self.assertEqual(
            _fetch_date_from_file_name(
                correct_filename, self.log_name_template, self.date_format
            ),
            expected_date
        )

    def test_return_none_for_incorrect_filename(self):
        incorrect_filename = 'test.txt'
        self.assertIsNone(
            _fetch_date_from_file_name(
                incorrect_filename, self.log_name_template, self.date_format
            )
        )
