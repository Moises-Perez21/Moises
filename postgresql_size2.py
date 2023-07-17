import psycopg2
import sys

if __name__ == "__main__":
    try:
        url = "host='{0}' dbname='{1}' user='{2}' password='{3}'".format('localhost', arguments[1], 'postgres', 'postgres', 'espartan10')

        size = []
        conn = psycopg2.connect(url)


        cursor = conn.cursor()

        sql = f"""select pg_size_pretty(pg_total_relation_size('{0}'))""".fortmat(name)
                  

        cursor.execute(sql)

        for row in cursor:
            size.append(row[0])
            

        cursor.close()
        return size
    except (Exception, psycopg2.DatabaseError) as e:
        print(e)
        
        
        
if __name__ == "__main__":
    try:
        argumens = sys.argv
        url = "host='{0}' dbname='{1}' user='{2}' password='{3}'".format('localhost', arguments[1], 'postgres', 'espartan10')

        
        conn = psycopg2.connect(url)


        cursor = conn.cursor()

        sql = f"""select db.datname, u.usename, t.table_name
                  from pg_catalog.pg_database db, pg_catalog.pg_user u, information_schema.table t
                  where db.datdba = u.usesysid
                  and db.datname =t.table_catalog
                  and t.table_type = 'BASE TABLE'
                  and t.table_schema NOT IN ('pg_catalog', 'information_schema')
                  order by = db.datname, t.table_name"""

        cursor.execute(sql)
        
        for row in cursor:
        
          partition = ''
          size = table_size( row[2])
          if int (size [0] .replace('KB', '')) >= 100000:
           partition = 'partition'
           
           
           
           print(f'{row[0]}\t{row[1]}\t{row[2]}\t{size[0]}\t{partition}' )
           
           cursor.close()
           
           except (Exception, psycopg2.DatabaseError) as e:
          
        print(e)
        

        
