import psycopg2


if __name__ == "__main__":
    try:
        url = "host='{0}' dbname='{1}' user='{2}' password='{3}'".format('localhost', 'postgres', 'postgres', 'espartan10')


        conn = psycopg2.connect(url)


        cursor = conn.cursor()

        sql = """Select datname as "Base de batos", rolname as "Dueno", pg_database_size(datname) as "Tamano"
                 from pg_database join pg_roles on datdba = pg_roles.oid"""

        cursor.execute(sql)

        for row in cursor:
            print("\t",row[0],"\t",row[1],"\t",row[2])


        cursor.close()
    except (Exception, psycopg2.DatabaseError) as e:
        print(e)
