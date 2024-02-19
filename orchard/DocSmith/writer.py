from jinja2 import Environment, select_autoescape
import re
from datetime import datetime, timedelta
from orchard.DocSmith.DataConnector import BaseConnector
from orchard.DocSmith.model.doc_template import DocTemplate


def th_datetime_format(val, format="medium-date"):
    f = "%Y-%m-%dT%H:%M:%S.%fZ"
    value = datetime.strptime(val, f)
    value += timedelta(hours=7)
    s = str(value.day) + ' '
    short_month_text = [
        "",
        "ม.ค.",
        "ก.พ.",
        "ม.ค.",
        "เม.ย.",
        "พ.ค.",
        "มิ.ย.",
        "ก.ค.",
        "ส.ค.",
        "ก.ย.",
        "ต.ค.",
        "พ.ย.",
        "ธ.ค."
    ]
    med_month_text = [
        "",
        "มกราคม",
        "กุมภาพันธ์",
        "มีนาคม",
        "เมษายน",
        "พฤษภาคม",
        "มิถุนายน",
        "กรกฎาคม",
        "สิงหาคม",
        "กันยายน",
        "ตุลาคม",
        "พฤศจิกายน",
        "ธันวาคม"
    ]
    if format == 'medium-date':
        s += med_month_text[value.month]
        s += ' พ.ศ. '
    if format == 'short-date':
        s += short_month_text[value.month]
        s += ' '
    y_str = str(int(value.year) + 543)
    if format == 'medium-date':
        s += y_str
    if format == 'short-date':
        s += y_str[-2:]
    return s


def th_number_format(val, format='currency'):
    try:
        v = float(val)
    except ValueError:
        v = 0.0
    return '{:,.2f}'.format(v)


def th_year_format(val, format=''):
    try:
        v = int(val) + 543
    except ValueError:
        return val
    return v


class DocWriter:

    def __init__(self):
        self.doc_template = None
        self.template_body = ''
        self.connector: BaseConnector = None
        self.data = dict()
        self.env = Environment(
            autoescape=select_autoescape(['html', 'xml'])
        )
        self.env.filters['th_datetime'] = th_datetime_format
        self.env.filters['th_year'] = th_year_format
        self.env.filters['th_number'] = th_number_format

    def init_with_doc_template(self, doc_template: DocTemplate, params: dict):
        self.doc_template = doc_template
        self.set_template_body(doc_template.body)
        data = self.connector.query_data(doc_template.data_query, params)
        self.set_data(data)

    def set_template_body(self, body: str) -> None:
        self.template_body = body

    def set_data(self, data: dict) -> None:
        self.data = data

    # def format_field(self, field_code: str) -> str:
    #     fieldname = re.sub(r'\}$', '', re.sub(r'^\$\{', '', field_code))
    #     if fieldname in self.data.keys():
    #         return self.data[fieldname]
    #     return fieldname
    #
    # def merge_fields(self, template: str) -> str:
    #     fields = set(re.findall(r'\$\{[\w#|:]*\}', template))
    #     result = self.template_body
    #     for field in fields:
    #         merged_field = self.format_field(field)
    #         result = result.replace(field, merged_field)
    #     return result

    def merge_doc(self) -> str:
        # Change to Jinja 2 template
        template = self.template_body
        # for backward compatibility
        fields = set(re.findall(r'\$\{[\w#|:]*\}', template))
        for field in fields:
            jinja_field = field.replace('${', '{{').replace('}', '}}')
            template = template.replace(field, jinja_field)
        jinja_template = self.env.from_string(template)
        result = jinja_template.render(self.data)
        return result

    def set_data_connector(self, data_connector: BaseConnector):
        self.connector = data_connector
