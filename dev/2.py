# 关注于 select 的解析
# 每个 最简单的 select 分成二类 节点， column  and table(subquery),然后建立他们之间的关系
# 重新设定， select 处理成最基本的 column的定义，从 最简单的整起
# 需要考虑 横向（多个left join），纵向 union ， 子查询的实现
import json
from typing import Optional
from loguru import logger

import sqlfluff


class select:
    def __init__(self) -> None:
        sql = "select c.a as c , d.b as b   from ods.source_table as c, ods.source_table2 as d"
        self.v_dict = sqlfluff.parse(sql=sql, dialect="mysql")
        self.v_dict = self.v_dict["file"]["statement"]["select_statement"]
        print(json.dumps(self.v_dict))

        self._table_list = []
        self._with_list = []
        self._subQuery = []
        self._column_list = []

    def do(self):
        self.do_from_clause()

        logger.info("this is table")
        for x in self._table_list:
            logger.info(x)
        self.do_select_clause()

        logger.info("this is column")
        for x in self._column_list:
            logger.info(x)

    def do_result(self):
        pass

    def do_select_clause(self):
        a = self.v_dict["select_clause"]
        for item in a:
            if "select_clause_element" in item.keys():
                b = item["select_clause_element"]
                col_dict = b["column_reference"]
                alias_dict = b["alias_expression"]
                v_col_name = "".join(["".join(x.values()) for x in col_dict])
                alias_name = alias_dict["naked_identifier"]
                print(v_col_name, alias_name)

                v_col_name_split = v_col_name.split(".")
                len_col_split = len(v_col_name_split)

                database_name = schema_name = table_name = column_name = None
                if len_col_split == 4:
                    (
                        database_name,
                        schema_name,
                        table_name,
                        column_name,
                    ) = v_col_name_split
                elif len_col_split == 3:
                    schema_name, table_name, column_name = v_col_name_split
                elif len_col_split == 2:
                    table_name, column_name = v_col_name_split

                    #     如果长度是2，说明 可能是表名 可能是别名
                    #  查找 顺序为 别名， 不含模式名的表名，含模式名的表
                    #  优先级为 tab， subquery

                    for table_info in self._table_list:
                        if table_info["table_name"] == table_name:
                            schema_name = table_info["schema_name"]
                            database_name = table_info["database_name"]

                        elif table_info["alias_name"] == table_name:
                            schema_name = table_info["schema_name"]
                            database_name = table_info["database_name"]
                            table_name = table_info["table_name"]
                self._column_list.append(
                    {
                        "database_name": database_name,
                        "schema_name": schema_name,
                        "table_name": table_name,
                        "column_name": column_name,
                        "alias_name": alias_name,
                    }
                )

    def do_from_clause(self):
        a = self.v_dict["from_clause"]
        print(json.dumps(a))

        for item in a:
            if "from_expression" in item.keys():
                for key in item["from_expression"].keys():
                    if key == "from_expression_element":
                        alias_name = None
                        table_name = None
                        schema_name = None
                        database_name = None
                        v_table_name = ""
                        for key1 in item["from_expression"][
                            "from_expression_element"
                        ].keys():
                            if key1 == "table_expression":
                                for key2 in item["from_expression"][
                                    "from_expression_element"
                                ]["table_expression"].keys():
                                    if key2 == "table_reference":
                                        b = item["from_expression"][
                                            "from_expression_element"
                                        ]["table_expression"]["table_reference"]

                                        for x in b:
                                            for y in x.values():
                                                v_table_name = v_table_name + y

                                        if v_table_name:
                                            b = v_table_name.split(".")
                                            for i in range(v_table_name.count("."), 2):
                                                b.insert(0, None)
                                            database_name, schema_name, table_name = b

                                        print(database_name, schema_name, table_name)
                            if key1 == "alias_expression":
                                alias_name = item["from_expression"][
                                    "from_expression_element"
                                ]["alias_expression"]["naked_identifier"]
                                print(alias_name)
                        self._table_list.append(
                            {
                                "database_name": database_name,
                                "schema_name": schema_name,
                                "table_name": table_name,
                                "alias_name": alias_name,
                            }
                        )


if __name__ == "__main__":
    a = select()
    a.do()
    # c = Table(full_table_name="ods.source_table", alias_name="c")
    # print(str(c))
