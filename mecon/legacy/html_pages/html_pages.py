import abc
import base64
import os
import tempfile
import matplotlib.pyplot as plt
import uuid

_TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')


def _read_template(template_name):
    filename = os.path.join(_TEMPLATE_DIR, template_name+'.html')
    with open(filename, 'r') as f:
        return f.read()


class HTMLObject(abc.ABC):
    @abc.abstractmethod
    def html(self) -> str:
        return None

    def save(self, path):
        with open(path, 'w', encoding="utf-8") as f:
            f.write(self.html())


class HTMLBuilder(HTMLObject):
    def __init__(self, template_str):
        self._html_str = template_str

    def replace(self, this, with_that):
        self._html_str = self._html_str.replace(f"[{this}]", with_that)

    def html(self):
        return self._html_str


class HTMLPage(HTMLBuilder):
    def __init__(self):
        super().__init__('')

    def add_element(self, element):
        if issubclass(element.__class__, HTMLBuilder):
            element_html_str = element.html()
        else:
            element_html_str = str(element)

        self._html_str = f"{self._html_str}\n<div>{element_html_str}</div>"
        return self


class TemplateHTMLBuilder(HTMLBuilder):
    def __init__(self, template_name):
        template_html = _read_template(template_name)
        super().__init__(template_html)


class ImageHTML(TemplateHTMLBuilder):
    def __init__(self, img_bytes, width=None, height=None):
        super().__init__('image')
        self.replace('image', self._bytes_to_utf8(img_bytes))

        if height:
            self.replace('height', f'height="{height}"')
        else:
            self.replace('height', '')

        if width:
            self.replace('width', f'width="{width}"')
        else:
            self.replace('width', '')

    @staticmethod
    def _bytes_to_utf8(_bytes):
        return base64.b64encode(_bytes).decode("utf-8")

    @staticmethod
    def from_image_file(image_path):
        with open(image_path, "rb") as image_file:
            return ImageHTML(image_file.read())

    @staticmethod
    def from_matplotlib():
        tmp_file = tempfile.NamedTemporaryFile(suffix='_image.png', delete=False)
        plt.savefig(tmp_file.name)

        image_html = ImageHTML.from_image_file(tmp_file.name)

        tmp_file.close()
        os.unlink(tmp_file.name)

        return image_html


class TabsHTML(TemplateHTMLBuilder):
    def __init__(self):

        super().__init__('tabs')
        self._id = uuid.uuid4().hex[:6].upper()

        self._add_default_id = True

    def _add_button(self, label):
        default_id = f"id=\"defaultOpen_[id]\"" if self._add_default_id else ""
        self._add_default_id = False
        to_add = f"<button class=\"tablinks_[id]\" onclick=\"open_tab_[id](event, '{label}_[id]')\" {default_id}>{label}</button>\n\t[button]"
        self.replace('button', to_add)

    def _add_div(self, label, html_content):
        to_add = f"<div id=\"{label}_[id]\" class=\"tabcontent_[id]\">\n{html_content}\n</div>\n[div_tab]"
        self.replace('div_tab', to_add)

    def add_tab(self, label, html_content):
        self._add_button(label)
        self._add_div(label, html_content.html() if issubclass(html_content.__class__, HTMLObject) else html_content)
        return self

    def html(self):
        self.replace("button", '')
        self.replace("div_tab", '')
        self.replace("id", self._id)
        return self._html_str


class DropMenuHTML(TemplateHTMLBuilder):

    def __init__(self):
        super().__init__('dropmenu')
        self._id = uuid.uuid4().hex[:6].upper()

        self._add_default_id = True
        self._opt_cnt = 0

    def _add_label(self, label):
        to_add = f"<option class=\"opt\" {'selected' if self._add_default_id else ''} value=\"{self._opt_cnt}\">{label}</option>\n\t[option_label]"
        self._add_default_id = False
        self.replace('option_label', to_add)

    def _add_div(self, label, html_content):
        to_add = f"<div class=\"dropmenu_content_[id]\" id=\"hidden_div{self._opt_cnt}_[id]\">{html_content}</div>\n\t[option_div]"
        self.replace('option_div', to_add)


    def add_option(self, label, html_content):
        self._add_label(label)
        self._add_div(label, html_content.html() if issubclass(html_content.__class__, HTMLObject) else html_content)
        self._opt_cnt += 1
        return self

    def html(self):
        self.replace("option_label", '')
        self.replace("option_div", '')
        self.replace("id", self._id)

        return self._html_str


class HTMLTableBuilder(TemplateHTMLBuilder):
    def __init__(self, title):
        super().__init__('table')
        self.add_title(title)

    def add_title(self, title):
        self.replace('title', title)

    def add_column_names(self, columns):
        cols_html = "".join([f"<th>{column}</th>" for column in columns])
        cols_html = f"<tr>{cols_html}</tr>"
        self.replace('columns', cols_html)

    def add_row(self, row):
        cols_html = "".join([f"<td>{column}</td>" for column in row])
        cols_html = f"<tr>{cols_html}</tr>\n    [row]"
        self.replace('row', cols_html)

    def html(self):
        self.replace("row", '')
        return self._html_str


class SimpleHTMLTable(HTMLTableBuilder):
    def __init__(self, df, title=''):
        super().__init__(title)
        self._df = df
        self.add_column_names(self._df.columns)
        for row in self._df.iterrows():
            self.add_row(row[1].values)

