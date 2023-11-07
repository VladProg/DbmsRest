from decimal import Decimal
from enum import Enum
from pydantic import BaseModel, conint, NonNegativeInt, PrivateAttr, model_validator
from typing import Optional, Union


class Color(BaseModel):
    r: conint(ge=0, le=255)
    g: conint(ge=0, le=255)
    b: conint(ge=0, le=255)


Value = Union[int, Decimal, str, Color]


class Type(str, Enum):
    Integer = 'Integer'
    Real = 'Real'
    Char = 'Char'
    String = 'String'
    Color = 'Color'
    ColorInvl = 'ColorInvl'


class Column(BaseModel):
    name: str
    type: Type
    r_min: Optional[conint(ge=0, le=255)] = None
    r_max: Optional[conint(ge=0, le=255)] = None
    g_min: Optional[conint(ge=0, le=255)] = None
    g_max: Optional[conint(ge=0, le=255)] = None
    b_min: Optional[conint(ge=0, le=255)] = None
    b_max: Optional[conint(ge=0, le=255)] = None

    @model_validator(mode='after')
    def check(self):
        if self.type == Type.ColorInvl:
            if None in [self.r_min, self.r_max, self.g_min, self.g_max, self.b_min, self.b_max]:
                raise ValueError('Column: if type="ColorInvl", fields "r_min", "r_max", "g_min", "g_max", "b_min", '
                                 '"b_max" must be also provided')
            for field in 'rgb':
                if getattr(self, f'{field}_min') > getattr(self, f'{field}_max'):
                    raise ValueError(f'{field}_min must be less or equal that {field}_max')
        else:
            if not (self.r_min is self.r_max is self.g_min is self.g_max is self.b_min is self.b_max is None):
                raise ValueError(f'Column: if type="{self.type}", fields "r_min", "r_max", "g_min", "g_max", '
                                 f'"b_min", "b_max" must not be provided')
        return self

    def type_str(self) -> str:
        if self.type != Type.ColorInvl:
            return self.type
        else:
            return f'ColorInvl (R∈[{self.r_min}..{self.r_max}], G∈[{self.g_min}..{self.g_max}], B∈[{self.b_min}..{self.b_max}])'

    def check_value(self, value):
        try:
            if self.type == Type.Integer:
                assert isinstance(value, int)
            elif self.type == Type.Real:
                assert isinstance(value, int) or isinstance(value, Decimal)
            elif self.type == Type.Char:
                assert isinstance(value, str) and len(value) == 1
            elif self.type == Type.String:
                assert isinstance(value, str)
            elif self.type == Type.Color:
                assert isinstance(value, Color)
            elif self.type == Type.ColorInvl:
                assert (isinstance(value, Color) and
                        self.r_min <= value.r <= self.r_max and
                        self.g_min <= value.g <= self.g_max and
                        self.b_min <= value.b <= self.b_max)
            else:
                assert False
        except AssertionError:
            raise ValueError(f"{self.type_str()} expected but {type(value).__name__} value '{value}' found")

    def get_type(self, name: str = "") -> 'Column':
        return Column(name=name,
                      type=self.type,
                      r_min=self.r_min, r_max=self.r_max,
                      g_min=self.r_min, g_max=self.r_max,
                      b_min=self.r_min, b_max=self.r_max)


class Row(BaseModel):
    id: NonNegativeInt
    cells: list[Value]


class Table(BaseModel):
    id: NonNegativeInt
    name: str
    columns: list[Column]
    _rows: dict[NonNegativeInt, Row] = {}
    _next_id: NonNegativeInt = PrivateAttr(0)

    def add_row(self, cells: list[Value]) -> Row:
        if len(cells) != len(self.columns):
            raise ValueError("Row length must be the same as number of columns")
        for i in range(len(self.columns)):
            self.columns[i].check_value(cells[i])
        row = Row(id=self._next_id, cells=cells)
        self._rows[self._next_id] = row
        self._next_id += 1
        return row

    def remove_row(self, id):
        if id in self._rows:
            del self._rows[id]

    def contains_row(self, row) -> bool:
        return any(row.cells == value.cells for value in self._rows.values())

    def __sub__(left: 'Table', right: 'Table') -> 'TableDifference':
        if len(left.columns) != len(right.columns):
            raise ValueError("Table difference: tables have different column counts")
        if any(l.get_type() != r.get_type() for l, r in zip(left.columns, right.columns)):
            raise ValueError("Table difference: tables have different column types")
        columns = [l.get_type(l.name if l.name == r.name else f"'{l.name}' / '{r.name}'")
                   for l, r in zip(left.columns, right.columns)]
        rows = {}
        for id, row in left._rows.items():
            if not right.contains_row(row):
                rows[id] = row
        return TableDifference(
            left_table=left,
            right_table=right,
            columns=columns,
            rows=rows
        )


class Database(BaseModel):
    name: str
    tables: dict[NonNegativeInt, Table] = {}
    _next_id: NonNegativeInt = PrivateAttr(0)

    def add_table(self, name: str, columns: list[Column]):
        table = Table(id=self._next_id, name=name, columns=columns)
        self.tables[self._next_id] = table
        self._next_id += 1
        return table

    def remove_table(self, id):
        if id in self.tables:
            del self.tables[id]


class TableDifference(BaseModel):
    left_table: Table
    right_table: Table
    columns: list[Column]
    rows: dict[NonNegativeInt, Row] = []
