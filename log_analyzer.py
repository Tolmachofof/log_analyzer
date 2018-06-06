import argparse
import configparser
import datetime
import gzip
import logging
import os
import json
import re
from collections import defaultdict, namedtuple
from statistics import median

CONFIG = {
    'LOGS_DIR': './logs',
    'LOG_NAME_TEMPLATE': 'nginx-access-ui.log-{date}',
    'LOG_DATE_FORMAT': '%Y%m%d',
    'REPORTS_DIR': './reports',
    'REPORT_NAME_TEMPLATE': 'report-{date}.html',
    'REPORT_DATE_FORMAT': '%Y.%m.%d',
    'REPORT_TEMPLATE': './report.html',
    'REPORT_SIZE': 100,
    'REPORT_ACCURACY': 2,
    'ERRORS_PERCENT_LIMIT': 0,
    'LOG_LEVEL': 'DEBUG',
}

CONFIG_SECTION = 'LOG_ANALYZER'

LOG_LINE_PATTERN = re.compile(
    r'(?P<remote_addr>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s'
    r'(?P<remote_user>\S+)\s+'
    r'(?P<http_x_real_ip>\S+)\s+'
    r'\[(?P<time_local>.+)\]\s+'
    r'"(?P<request>.*?)"\s+'
    r'(?P<status>\d{3})\s+'
    r'(?P<body_bytes_sent>\d+)\s+'
    r'"(?P<http_referer>.+)"\s+'
    r'"(?P<http_user_agent>.+)"\s+'
    r'"(?P<http_x_forwarded_for>.+)"\s+'
    r'"(?P<http_X_REQUEST_ID>.+)"\s+'
    r'"(?P<http_X_RB_USER>.+)"\s+'
    r'(?P<request_time>.+)'
)


LogEntry = namedtuple('LogEntry', ('log_name', 'log_date'))
Report = namedtuple(
    'Report', ('report', 'total_requests', 'total_errors')
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config', help='Path to analyzer config.', default='config.ini'
    )
    return parser.parse_args()


def parse_config(config_path):
    config_parser = configparser.ConfigParser(defaults=CONFIG)
    config_parser.read(config_path)
    config = {
        option: config_parser['DEFAULT'].get(option, raw=True)
        for option in CONFIG
    }
    if config_parser.has_section(CONFIG_SECTION):
        config.update(
            {
                option: config_parser[CONFIG_SECTION].get(option, raw=True)
                for option in CONFIG
            }
        )
    return config


def set_logging_settings(log_level, filename=None):
    logging.basicConfig(
        format='[%(asctime)s] %(levelname).1s %(message)s',
        datefmt='%Y.%m.%d %H:%M:%S',
        level=log_level,
        filename=filename
    )


def _fetch_date_from_file_name(file_name, template, date_format):
    try:
        file_name = (
            file_name if not file_name.endswith('.gz')
            else os.path.splitext(file_name)[0]
        )
        return datetime.datetime.strptime(
            file_name,
            template.format(date=date_format),
        )
    except ValueError:
        return


def get_last_log(logs_dir, log_template, date_format):
    logging.info('Find late log in directory: {}'.format(logs_dir))
    try:
        logs_in_directory = (
            LogEntry(
                log_name,
                _fetch_date_from_file_name(log_name, log_template, date_format)
            )
            for log_name in os.listdir(logs_dir)
        )
        return max(
            filter(
                lambda log_entry: log_entry.log_date is not None,
                logs_in_directory
            )
        )
    except ValueError:
        logging.info(
            'Log files matching template do not exist in directory: {}'.format(
                logs_dir
            )
        )


def read_file(path):
    logging.info('Read file: {}'.format(path))
    fopen = gzip.open if path.endswith('.gz') else open
    with fopen(path, 'rb') as f:
        for line in f:
            yield line.decode('utf-8')


def parse_line(line, pattern):
    parsed_line = pattern.match(line)
    if parsed_line is not None:
        return parsed_line.groupdict()
    else:
        logging.debug('Line {} does not match pattern.'.format(line))


def summarize_report(
    report, total_requests, total_requests_time, size, accuracy
):
    top_reports_size = size if size < len(report) else len(report)
    top_reports = sorted(
        report.values(), key=lambda item: item['time_sum']
    )[:top_reports_size]
    for url_report in top_reports:
        url_requests = url_report.pop('requests')
        url_report.update(
            **{
                'count_perc': round(
                    100 * url_report['count'] / total_requests, accuracy
                ),
                'time_perc': round(
                    100 * url_report['time_sum'] / total_requests_time,
                    accuracy
                ),
                'time_avg': round(
                    url_report['time_sum'] / url_report['count'], accuracy
                ),
                'time_max': max(url_requests),
                'time_med': median(url_requests),
            }
        )
    return top_reports


def create_report(file_path, pattern, report_size, accuracy):
    total_requests = total_requests_time = total_errors = 0
    report = defaultdict(lambda: defaultdict(int))
    for line in read_file(file_path):
        total_requests += 1
        try:
            request = parse_line(line, pattern)
            url, request_time = (
                request['remote_addr'], float(request['request_time']),
            )
        except (ValueError, KeyError):
            total_errors += 1
            break
        if 'url' and 'requests' not in report[url]:
            report[url]['url'], report[url]['requests'] = url, []
        report[url]['count'] += 1
        report[url]['time_sum'] = round(
            report[url]['time_sum'] + request_time, accuracy
        )
        report[url]['requests'].append(request_time)
        total_requests_time = round(
            total_requests_time + request_time, accuracy
        )
    return Report(
        summarize_report(
            report, total_requests, total_requests_time, report_size, accuracy
        ), total_requests, total_errors
    )


def render_report(report, report_path, template_path):
    with open(template_path, 'r') as tp, open(report_path, 'w') as rp:
        template = tp.read()
        template = template.replace('$table_json', json.dumps(report))
        rp.write(template)


def main(config):
    logging.info('Starting app.')
    log_for_analyze = get_last_log(
        config['LOGS_DIR'], config['LOG_NAME_TEMPLATE'],
        config['LOG_DATE_FORMAT']
    )
    # Shutdown if the logs dir does not contain logs
    if log_for_analyze is None:
        return
    report_name = config['REPORT_NAME_TEMPLATE'].format(
        date=log_for_analyze.log_date.strftime(config['REPORT_DATE_FORMAT'])
    )
    if os.path.exists(os.path.join(config['REPORTS_DIR'], report_name)):
        logging.info(
            "Report for log {} has already exists!".format(
                log_for_analyze.log_name
            )
        )
        return
    report = create_report(
        os.path.join(config['LOGS_DIR'], log_for_analyze.log_name),
        LOG_LINE_PATTERN, int(config['REPORT_SIZE']),
        int(config['REPORT_ACCURACY'])
    )
    errors_percent = round(100 * report.total_errors / report.total_requests)
    if errors_percent > config['ERRORS_PERCENT_LIMIT'] > 0:
        logging.error(
            (
                'The {total_errors} errors was occurred at report creation. '
                'It is about {errors_percent} percent '
                'relatively the total lines number in the {file} log file.'
            ).format(
                total_errors=report.total_errors,
                errors_percent=errors_percent,
                file=log_for_analyze.log_name
            )
        )
    else:
        render_report(
            report.report, os.path.join(config['REPORTS_DIR'], report_name),
            config['REPORT_TEMPLATE']
        )


if __name__ == '__main__':
    args = parse_args()
    config = parse_config(args.config)
    set_logging_settings(config['LOG_LEVEL'], config.get('LOG_FILE'))
    try:
        start_time = datetime.datetime.now()
        main(config)
    except Exception as exc:
        logging.exception(exc)
