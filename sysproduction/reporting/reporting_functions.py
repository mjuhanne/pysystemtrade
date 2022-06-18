import pandas as pd
import os

from syscore.objects import resolve_function, arg_not_supplied, missing_data
from syscore.objects import header, table, body_text
from syscore.fileutils import get_resolved_pathname
from syscore.text import landing_strip_from_str, landing_strip, centralise_text
from sysdata.data_blob import dataBlob

from syslogdiag.email_via_db_interface import send_production_mail_msg

from sysproduction.reporting.report_configs import reportConfig
from sysproduction.reporting.table_formatting import get_formatted_html_table

def run_report(report_config:
reportConfig, data: dataBlob = arg_not_supplied):
    """

    :param report_config:
    :return:
    """
    pandas_display_for_reports()
    if data is arg_not_supplied:
        data = dataBlob(log_name="Reporting %s" % report_config.title)

    run_report_with_data_blob(report_config, data)


def run_report_with_data_blob(report_config: reportConfig, data: dataBlob):
    """

    :param report_config:
    :return:
    """

    data.log.msg("Running report %s" % str(report_config))
    report_function = resolve_function(report_config.function)
    report_kwargs = report_config.kwargs

    report_results = report_function(data, **report_kwargs)
    parsed_report = parse_report_results(report_results, "plain")
    if report_config.formatting == "html":
        parsed_report_html = parse_report_results(report_results, "html")
    else:
        parsed_report_html = None
    output = report_config.output

    # We either print or email or send to file or ...
    if output == "console":
        print(parsed_report)
    elif output == "email":
        email_report(parsed_report, parsed_report_html,
                     report_config=report_config, data=data)
    elif output == "file":
        file_report(parsed_report,
                     report_config=report_config, data=data)
    elif output == "emailfile":
        email_report(parsed_report, parsed_report_html,
                     report_config=report_config, data=data)
        file_report(parsed_report,
                     report_config=report_config, data=data)
    else:
        raise Exception("Report config %s not recognised!" % output)

def email_report(parsed_report,
                 parsed_report_html,
                 report_config,
                 data):
    send_production_mail_msg(
        data, parsed_report,
        subject=report_config.title,
        email_is_report=True,
        body_html=parsed_report_html
    )

def file_report(parsed_report,
                 report_config,
                 data):
    filename_with_spaces = report_config.title
    filename = filename_with_spaces.replace(" ", "_")
    write_report_to_file(data, parsed_report, filename=filename)

def parse_report_results(report_results: list, formatting):
    """
    Parse report results into human readable text for display, email, or christmas present

    :param report_results: list of header, body or table
    :return: String, with more \n than you can shake a stick at
    """
    output_string = ""
    for report_item in report_results:
        if isinstance(report_item, header):
            parsed_item = parse_header(report_item, formatting)
        elif isinstance(report_item, body_text):
            parsed_item = parse_body(report_item, formatting)
        elif isinstance(report_item, table):
            parsed_item = parse_table(report_item, formatting)
        else:
            parsed_item = " %s failed to parse in report\n" % str(report_item)

        output_string = output_string + parsed_item

    if formatting == "html":
        output_string = "<html>" + output_string + "</html>"
    return output_string



def parse_table_as_html(report_table: table) -> str:
    if isinstance(report_table.Body, pd.Series):
        df = pd.DataFrame(report_table.Body)
    else:
        df = report_table.Body
    html = get_formatted_html_table(report_table.Heading, df)
    return  "<h3>" + report_table.Heading + "</h3>" + html


def parse_table(report_table: table, formatting) -> str:
    if formatting == "html":
        return parse_table_as_html(report_table)

    table_header = report_table.Heading
    table_body = str(report_table.Body)
    table_header_centred = centralise_text(table_header, table_body)
    underline_header = landing_strip_from_str(table_header_centred)

    table_string = "\n%s\n%s\n%s\n\n%s\n\n" % (
        underline_header,
        table_header_centred,
        underline_header,
        table_body,
    )

    return table_string


def parse_body(report_body: body_text, formatting) -> str:
    if formatting == "html":
        return "<p>" + report_body.Text + "</p>"
    body_text = report_body.Text
    return "%s\n" % body_text


def parse_header(report_header: header, formatting) -> str:
    if formatting == "html":
        return "<h2>" + report_header.Heading + "</h2>"
    header_line = landing_strip(80, "*")
    header_text = centralise_text(report_header.Heading, header_line)

    return "\n%s\n%s\n%s\n\n\n" % (header_line, header_text, header_line)


def pandas_display_for_reports():
    pd.set_option("display.width", 1000)
    pd.set_option("display.max_columns", 1000)
    pd.set_option("display.max_rows", 1000)


def write_report_to_file(data: dataBlob, parsed_report: str, filename: str):
    use_directory = get_directory_for_reporting(data)
    use_directory_resolved = get_resolved_pathname(use_directory)
    full_filename = os.path.join(use_directory_resolved, filename)
    with open(full_filename, "w") as f:
        f.write(parsed_report)
    data.log.msg("Written report to %s" % full_filename)


def get_directory_for_reporting(data):
    # eg '/home/rob/reports/'
    production_config = data.config
    store_directory = production_config.get_element_or_missing_data(
        "reporting_directory"
    )
    if store_directory is missing_data:
        raise Exception("Need to specify reporting_directory in config file")

    return store_directory
