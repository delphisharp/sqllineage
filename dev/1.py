# 从1开始构建血缘分析工具

# 构建思路，
# 从 sqlfluff构建后，对结果 保存为 sqlite3 和 图信息
# sqlite3 文件保留原始信息，但是补充 父节点关系
# 图信息 节点为ID，其它为属性，保留上下级关系。
#  构建 查询工具，对图可以方便展现，对数据可以快速查询

# 使用递归，对每段 代码 逐层 构建。
#  以 sqlsfss的解析规则作为基础，是否需要每个数据库 针对性的进行处理？
# 每个需要涉及的函数，需要

# 包含的字段， 子函数或过程， union 或 minis ， 行列转换

from pprint import pprint
from typing import Any, Dict
import sqlfluff
from sqlfluff.core.linter import Linter
import sqlite3


class tree:

    def parsedict2list(self, parse_dict: Dict[str, Any], parent_idx: int):
        for keyword, items in parse_dict.items():
            if keyword in ("whitespace", "comma"):
                continue

            self.idx = self.idx + 1
            self.v_res.append([keyword, str(items), self.idx, parent_idx])

            if isinstance(items, dict):
                self.parsedict2list(parse_dict=items, parent_idx=self.idx)

            if isinstance(items, list):
                parent_idx=self.idx
                for item in items:
                    self.parsedict2list(parse_dict=item, parent_idx=parent_idx)

    def __init__(self, sql: str, dialect: str) -> None:
        self.v_dict = sqlfluff.parse(sql=sql, dialect=dialect)

        self.v_res = []
        self.idx = -1
        self.parsedict2list(parse_dict=self.v_dict , parent_idx=None)

        self.conn = sqlite3.connect(
            "/Users/liuzhou/工作/亚信/工具开发/sqllineage_github/sqllineage/dev/1.db"
        )

        cur = self.conn.cursor()
        cur.execute("drop table if exists sql_line")
        cur.execute(
            "create table sql_line (keyword text,key_value text,idx,parent_idx)"
        )
        cur.executemany("insert into sql_line values(?,?,?,?) ", self.v_res)
        self.conn.commit()

    def gen_xx(self):
        for statements in self.v_dict['file'].keys():
            if statements != "statement":
                continue
            for key in self.v_dict['file'][statements].keys():
                if key=='insert_statement':
                    # find table 
                    b= insert_statement(self.v_dict['file'][statements])
                    b.gen_insert_tab()
                    b.gen_insert_column()

        
         

class insert_statement():
    def __init__(self,v_res:dict) -> None:
        self.v_res = v_res
    
    def gen_insert_tab(self):
        for item in self.v_res['insert_statement']:
            if isinstance(item,dict) and "table_reference" in item.keys():
                print('this is target table define')
                print(item )

    def gen_insert_column(self):
        for item in self.v_res['insert_statement']:
            if isinstance(item,dict) and "bracketed" in item.keys():
                print('this is target column')
                print(statement_parse.gen_bracket_column(item['bracketed']))





class statement_parse:
    @classmethod
    def gen_bracket_column(cls,v_res:list):
        v_column_list=[]
        for x in v_res:
            if 'column_reference' in  x.keys():
                v_column_list.append(x['column_reference']['naked_identifier'])
        return v_column_list


if __name__ == "__main__":
    sql = "insert into target_tab(A,B) select sum(t.a) a , e+f as g  from source_tab  t "
    a = tree(sql, "mysql")
    a.gen_xx()
