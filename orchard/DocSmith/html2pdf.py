import os
import tempfile
import logging
import pdfkit

from orchard.settings import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HTML2PDF_BIN = config.HTML2PDF_BIN


class Html2Pdf:
    """
    Wrapper class for HTML2PDF
    """

    def __init__(self):
        self.page_size = 'A4'
        self.orientation = 'Portrait'
        self.page_width = None
        self.page_height = None
        self.dpi = None
        self.margin_top = None
        self.margin_right = None
        self.margin_bottom = None
        self.margin_left = None
        self.cookies = []

    def set_pagesize(self, paper_size=None, page_width=None, page_height=None) -> None:
        """
        set page/paper size
        See this for supporting paper_size https://doc.qt.io/archives/qt-4.8/qprinter.html#PaperSize-enum
        :param paper_size: str
        :param page_width: str width in mm
        :param page_height: str height in mm
        :return:
        """
        if paper_size:
            self.page_size = paper_size
        if page_width:
            self.page_width = page_width
        if page_height:
            self.page_height = page_height

    def set_margin(self,
                   margin_top=None,
                   margin_right=10,
                   margin_bottom=None,
                   margin_left=10) -> None:
        """
        Set margin
        :param margin_top: in mm
        :param margin_right: in mm
        :param margin_bottom: in mm
        :param margin_left: in mm
        :return:
        """
        self.margin_top = margin_top
        self.margin_right = margin_right
        self.margin_bottom = margin_bottom
        self.margin_left = margin_left

    def set_dpi(self, dpi) -> None:
        """
        Set DPI
        :param dpi: 96 (default)
        :return:
        """
        self.dpi = dpi

    def set_orientation(self, orientation: str) -> None:
        """
        Set orientation
        :param orientation: Portrait (default / Landscape)
        :return: filename
        """
        self.orientation = orientation

    def add_cookie(self, key: str, value: str) -> None:
        c = {
            'key': key,
            'val': value
        }
        self.cookies.append(c)

    def remove_cookie(self, key) -> None:
        for c in self.cookies:
            if c['key'] == key:
                self.cookies.remove(c)

    def pdf_settings(self, output_file=None):
        if output_file == None:
            with tempfile.NamedTemporaryFile(delete=False) as fp:
                output_file = fp.name
        config = pdfkit.configuration(wkhtmltopdf=HTML2PDF_BIN)
        options = {}
        # options
        options['encoding'] = "utf-8"
        if self.page_width and self.page_height:
            options['page-width'] = '%smm' % self.page_width
            options['page-height'] = '%smm' % self.page_height
        else:
            options['page-size'] = '%s' % self.page_size
        if self.orientation.lower() == 'landscape':
            options['orientation'] = 'Landscape'
        if self.dpi:
            options['dpi'] = '%s' % self.dpi
        if self.margin_top:
            options['margin-top'] = '%smm' % self.margin_top
        if self.margin_right:
            options['margin-right'] = '%smm' % self.margin_right
        if self.margin_bottom:
            options['margin-bottom'] = '%smm' % self.margin_bottom
        if self.margin_left:
            options['margin-left'] = '%smm' % self.margin_left

        options['enable-local-file-access'] = None
        # print('pdf_settings.config', str(config))
        print('pdf_settings.options', options)
        print('pdf_settings.output_file', output_file)
        return config, options, output_file

    def render_content(self, content: str, output_file=None) -> str:
        print('content', content)
        config, options, output_file = self.pdf_settings(output_file)
        pdfkit.from_string(content, output_file,
                           configuration=config, options=options)
        # except:
        #     pdfkit.from_string("<html><h1>Error rendering document.</h1></html>", output_file,
        #                        configuration=config, options=options)
        return output_file

    def render_url(self, url: str, output_file=None) -> str:
        """
        Convert URL to pdf
        :param url:
        :param output_file:
        :return: filename
        :rtype: str
        """
        config, options, output_file = self.pdf_settings(output_file)
        pdfkit.from_url(url, output_file, configuration=config, options=options)
        return output_file

    def render_file(self, filename: str, output_file=None) -> str:
        """
        Convert URL to pdf
        :param url:
        :param output_file:
        :return: filename
        :rtype: str
        """
        config, options, output_file = self.pdf_settings(output_file)
        pdfkit.from_file(filename, output_file, configuration=config, options=options)
        return output_file
