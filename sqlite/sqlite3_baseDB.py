import sqlite3

class BaseDB():
    def __init__(self, db_path):
        print("database initialization: connect to " + db_path)
        self.m_con = sqlite3.connect(db_path)
        self.table_name = ""

    def get(self, where={}, limit=None, page=None):
        assert isinstance(where, dict), 'argument is not a dict'
        assert (limit is None and page is None) or (limit is not None and page is not None), 'page and limit are interdependent'
        c = self.m_con.cursor()
        sql = "SELECT rowid, * FROM "+self.table_name+(" WHERE " if len(where)>0 else "")
        for key, value in where.items():
            sql += key+"=? AND "
        if len(where)>0:
            sql = sql[:-4]
        if limit is not None and page is not None:
            offset = (int(page)-1)*int(limit)
            sql += " ORDER BY rowid DESC LIMIT ? OFFSET ?"
            params = list(where.values())
            params.extend([limit, offset])
            res = c.execute(sql, tuple(params))
        else:
            res = c.execute(sql, tuple(where.values()))
        cols = [column[0] for column in c.description]
        result = []	
        for row in res:
            r = {}
            i = 0
            for col in cols:
                r[col] = row[i]
                i = i + 1
            result.append(r)
        return result

    def getCount(self, where={}):
        assert isinstance(where, dict), 'argument is not a dict'
        c = self.m_con.cursor()
        sql = "SELECT count(*) FROM " + self.table_name + (" WHERE " if len(where)>0 else "")
        for key, value in where.items():
            sql += key + "=? AND "
        if len(where)>0:
            sql = sql[:-4]
            c.execute(sql, tuple(where.values()))
        else:
            c.execute(sql)
        count = c.fetchone()[0]
        return count

    def update(self, column, where):
        assert isinstance(column, dict) and isinstance(where, dict), 'argument is not a dict'
        c = self.m_con.cursor()
        sql = "UPDATE "+self.table_name+" SET "
        for key, value in column.items():
            sql += key + "=" + str(value) + " AND "
        sql = sql[:-4]
        sql += "WHERE "
        for key, value in where.items():
            sql += key + "=" + str(value) + " AND "
        sql = sql[:-4]
        c.execute(sql)
        self.m_con.commit()
        return c.lastrowid

    def delete(self, rowid):
        assert rowid is not None, 'have no index'
        c = self.m_con.cursor()
        sql = "DELETE FROM "+self.table_name+" WHERE rowid=?"
        c.execute(sql, (rowid,))
        self.m_con.commit()
        return c.lastrowid

