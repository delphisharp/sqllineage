# select_statement
#  基于 4.py 重新整理下代码， 重复功能进行 重用
from itertools import filterfalse
from typing import Optional, Dict


# 返回值涉及：
#  有前后关系的list， list 里面的对象按照生成列的顺序 进行排列。
#  里面的对象应该也是 list ，一个对应的column ，可以是多个 column 组成

# 对象定义

class Fields():
    

class Field():
    def __init__(self, database_name: Optional[str], schema_name: Optional[str], table_name: str, field_name: str, alias_name: Optional[str]):
        self._database_name = database_name
        self._schema_name = schema_name
        self._table_name = table_name
        self._field_name = field_name
        self._alias_name = alias_name

    @property
    def database_name(self):
        return self._database_name

    @property
    def schema_name(self):
        return self._schema_name

    @property
    def table_name(self):
        return self._table_name

    @property
    def field_name(self):
        return self._field_name

    @property
    def alias_name(self):
        return self._alias_name


def parse_select_statement()-> list[Dict[]]:
    pass


if __name__ == '__main__':
    a = ['1', '2', None, '3']
    for x in filterfalse(lambda x: not x, a):
        print(x)

    c = '.'.join([x for x in filterfalse(lambda x: not x, a)])
    print(c)

    # c='.'.join ( filterfalse(lambda x: not x, a))
    # print(c )
