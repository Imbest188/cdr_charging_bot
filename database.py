import psycopg2
from threading import Thread
import os

class DB:
    def __init__(self, host, user, password, dbname, table):
        self.host = host
        self.user = user
        self.password = password
        self.dbname = dbname
        self.table = table
        self.defaultpath = ''
        self.defaultformat = ''
        self.pool = []
        self.work = False
        self.aborted = False
        self.name = ''

    def set_default_path(self, path):
        self.defaultpath = path

    def state(self):
        return self.work

    def filecode(self, filename):
        try:
            return filename[7:17]
        except:
            return '0'

    def try_to_delete(self, file):
        try:
            os.remove(file)
        except:
            pass

    def write_last_file(self, pushing, mode):     
        filename = pushing.replace('\\','/').split('/')[-1]
        self.try_to_delete(pushing)
        with open(self.name, 'r+') as r:
            code = r.read()
            if int(code) < int(self.filecode(filename)):
                with open(self.name, 'w+') as w:
                    w.write(self.filecode(filename))
            return code
        return ''
        

    def abort(self):
        self.aborted = True
        print('db aborted')

    def set_name(self, name):
        self.name = name
            
    def set_default_format(self, defaultformat):
        self.defaultformat = defaultformat

    def in_pool(self):
        try:
            return len(self.pool)
        except:
            return 0

    def connect(self):
        conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, dbname=self.dbname)
        cursor = conn.cursor()
        cursor.execute('SET DateStyle=\'YMD\';')
        return conn, cursor

    def copy_from_file(self, cdr_filename):
        if not self.work:
            print('push ' + cdr_filename)
            self.work = True
            conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, dbname=self.dbname)
            cursor = conn.cursor()
            cursor.execute('SET DateStyle=\'YMD\';')
            
            if self.aborted:
                print(self.name + ' push to db aborted')
                self.work = False
                return True
                
            filename = self.defaultpath + cdr_filename + self.defaultformat
            f = open(filename, 'r')
            pushing = filename
            oldest = ''
            try:
                oldest = self.write_last_file(pushing, 0)
                try:
                    cursor.copy_from(f, self.table, sep=';')
                except:
                    print('exc')
                    try:
                        con, cursor = self.connect()
                        cursor.copy_from(f, self.table, sep=';')
                    except Exception as e:
                        print('exc2')
                        if hasattr(e, 'message'):
                            stringerror = str(e.message)
                        else:
                            stringerror = str(e)
                        print(stringerror)
                        try:
                            with open('exceptions.log', 'a+') as log:
                                log.write(stringerror + '\n')
                        except:
                            pass
                conn.commit()
                print(filename + ' pushed to db')
            except:
                print('#exc')
                self.write_last_file(pushing, 1)
            f.close()
            self.work = False
            print('end dbsess + ' + str(len(self.pool)))
            return True
        else:
            return False
