import psycopg2
from tabulate import tabulate
import sys

def table_size(name):
    try:
        url = "host='{0}' dbname='{1}' user='{2}' password='{3}'".format('localhost', arguments[1], 'postgres', 'espartan10')

        size = []
        conn = psycopg2.connect(url)

        cursor = conn.cursor()

        sql = """SELECT pg_size_pretty(pg_total_relation_size('{0}')), pg_total_relation_size('{0}');""".format(name)

        cursor.execute(sql)

        for row in cursor:
            size.append(row[0])
            size.append(row[1])
        
        cursor.close()
        return size
    except (Exception, psycopg2.DatabaseError) as e:
        print(e)



if __name__ == "__main__":
    try:
        arguments = sys.argv
        url = "host='{0}' dbname='{1}' user='{2}' password='{3}'".format('localhost', arguments[1], 'postgres', 'espartan10')


        conn = psycopg2.connect(url)
        cursor = conn.cursor()

        sql = """SELECT db.datname, u.usename, t.table_name
        FROM pg_catalog.pg_database db, pg_catalog.pg_user u, information_schema.tables t
        WHERE db.datdba = u.usesysid
        AND db.datname = t.table_catalog
        AND t.table_type = 'BASE TABLE'
        AND t.table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY db.datname, t.table_name
        """

        cursor.execute(sql)

        matriz = []
        
        for row in cursor:
            partition = ''
            size = table_size(row[2])
            if size[1] >= 100000:
                partition = 'partition'
            
            matriz.append([row[0], row[1], row[2], size[0], partition])
            #print(f'{row[0]}\t{row[1]}\t{row[2]}\t{size[0]}\t{partition}')

        print(tabulate(matriz, tablefmt='psql' , headers=['Base de Datos', 'Usuario', 'Tabla', 'Tamaño', 'Particion']))

        cursor.close()
    except (Exception, psycopg2.DatabaseError) as e:
        print(e)


