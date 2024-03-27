import sqlite3
import pandas as pd
from functools import wraps


class SqliteServer:
    def __init__(self, db_file):
        """创建一个数据库连接到 SQLite 数据库"""
        try:
            self.conn = sqlite3.connect(db_file)
            self.cursor = self.conn.cursor()
            print("SQLite 连接成功")
        except sqlite3.Error as e:
            print(e)

    # 类内部定义的装饰器
    @staticmethod
    def _auto_commit(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            result = func(self, *args, **kwargs)
            self.conn.commit()
            return result
        return wrapper

    def __del__(self):
        self.conn.close()
        print("SQLite 断开成功")

    @_auto_commit
    def create_table_from_df(self, df, table_name):
        """检查表是否存在，如果不存在则基于 DataFrame 的列创建表"""
        # 检查表是否存在
        is_exist = self.is_table_exist(table_name)
        if not is_exist:
            # 表不存在，根据 DataFrame 列创建表
            cols = ", ".join([f'"{col}" TEXT' for col in df.columns])
            self.cursor.execute(f'CREATE TABLE "{table_name}" ({cols})')
            print(f"表 {table_name} 创建成功")

    def is_table_exist(self, table_name):
        is_exist = True
        self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        if not self.cursor.fetchone():
            is_exist = False
            print(f"表 {table_name} 不存在")
        return is_exist

    # def create_table(self, table_name, columns):
    #     """在 SQLite 数据库中创建表"""
    #     try:
    #         cur = self.conn.cursor()
    #         cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)});")
    #         print(f"表 {table_name} 创建成功")
    #     except sqlite3.Error as e:
    #         print(e)

    @_auto_commit
    def append_new_data_to_table(self, new_data: pd.DataFrame, table_name):
        """只将新数据追加到 SQLite 数据库"""
        new_data.to_sql(table_name, self.conn, if_exists='append', index=False)
        print(f"新数据已追加到表 {table_name}")

    def get_table(self, table_name) -> pd.DataFrame:
        """从 SQLite 数据库中获取表的所有数据，并转换为 DataFrame"""
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql_query(query, self.conn)

    @_auto_commit
    def execute_query(self, query: str):
        return self.cursor.execute(query)

    def deduplicate_sort(self, table_name, keys):
        cols = ""
        for key in keys:
            cols = cols+'"'+key+'",'
        cols = cols[:-1]
        sql = f"""
        -- 创建一个临时表来存储每组重复数据中需要保留的 ROWID
        CREATE TEMP TABLE UniqueRows AS
        SELECT MIN(ROWID) as RowId
        FROM {table_name}
        GROUP BY {cols}; -- KeyColumn1, KeyColumn2 是你用来确定重复的列
        """
        self.execute_query(sql)

        sql = f"""
        -- 删除那些不在临时表中的行，即删除了重复的行，只保留了每组的一个
        DELETE FROM {table_name}
        WHERE ROWID NOT IN (SELECT RowId FROM UniqueRows);
        """
        self.execute_query(sql)

        sql = """
        -- 删除临时表
        DROP TABLE UniqueRows;
        """
        self.execute_query(sql)

        sql = f"""
        CREATE TABLE new_table AS
        SELECT * FROM {table_name}
        ORDER BY {cols};
        """
        self.execute_query(sql)

        sql = f"""
        DROP TABLE {table_name};
        """
        self.execute_query(sql)

        sql = f"""
        ALTER TABLE new_table RENAME TO {table_name};
        """
        self.execute_query(sql)
