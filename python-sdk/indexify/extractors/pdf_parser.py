import tempfile
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class PageFragmentType(str, Enum):
    TEXT = "text"
    FIGURE = "figure"
    TABLE = "table"


class Image(BaseModel):
    data: bytes
    mime_type: str


class TableEncoding(str, Enum):
    CSV = "csv"
    HTML = "html"


class Table(BaseModel):
    data: str
    encoding: TableEncoding


class PageFragment(BaseModel):
    fragment_type: PageFragmentType
    text: Optional[str] = None
    image: Optional[Image] = None
    table: Optional[Table] = None
    reading_order: Optional[int] = None


class Page(BaseModel):
    number: int
    fragments: List[PageFragment]


class PDFParser:
    def __init__(self, data: bytes, language: Optional[str] = "eng"):
        self._data = data
        self._language = language

    def parse(self) -> List[Page]:
        import deepdoctection as dd

        analyzer = dd.get_dd_analyzer(config_overwrite=[f"LANGUAGE='{self._language}'"])
        parsed_pages = []
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as f:
            f.write(self._data)
            f.flush()
            df = analyzer.analyze(path=f.name)
            df.reset_state()
            for page in df:
                parsed_pages.append(page)
        outputs: List[Page] = []
        for parsed_page in parsed_pages:
            page_num = parsed_page.page_number
            fragments = []
            for layout in parsed_page.layouts:
                if layout.category_name in ["text", "title"]:
                    fragments.append(
                        PageFragment(
                            fragment_type=PageFragmentType.TEXT,
                            text=layout.text,
                            reading_order=layout.reading_order,
                        )
                    )
            figures = parsed_page.get_annotation(category_names=dd.LayoutType.FIGURE)
            for figure in figures:
                image_bytes = dd.viz_handler.encode(figure.viz())
                fragments.append(
                    PageFragment(
                        fragment_type=PageFragmentType.FIGURE,
                        image=Image(data=image_bytes, mime_type="image/png"),
                        reading_order=figure.reading_order,
                    )
                )

            tables = parsed_page.get_annotation(category_names=dd.LayoutType.TABLE)
            for table in tables:
                fragments.append(
                    PageFragment(
                        fragment_type=PageFragmentType.TABLE,
                        table=Table(data=table.html, encoding=TableEncoding.HTML),
                        reading_order=table.reading_order,
                    )
                )

            outputs.append(Page(number=page_num, fragments=fragments))

        return outputs
