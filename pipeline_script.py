import os
import psycopg2
import csv

def main():
    # Datos cambiantes
    CSV_PATH = "CSVs/procesamiento_interesados_cubarral.csv"
    TABLE_NAME = "cubarral"

    # 1. Leer variables de entorno (colocadas en el workflow)
    dbname = os.environ["DB_NAME"]
    user = os.environ["DB_USER"]
    password = os.environ["DB_PASS"]
    host = os.environ["DB_HOST"]
    port = os.environ["DB_PORT"]

    # 2. Conectar a PostgreSQL
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )

    try:
        # 3. Crear/verificar la tabla
        crear_tabla(conn, TABLE_NAME)

        # 4. Insertar los datos desde el CSV local "zambrano.csv"
        insertar_datos_csv(conn, CSV_PATH, TABLE_NAME)

        # 5. Crear la tabla final donde se insertaran los datos resultantes del modelo de IA
        crear_tabla_final(conn, TABLE_NAME)

    except Exception as e:
        print(f"Ocurrió un error en el pipeline: {e}")
    finally:
        conn.close()

def crear_tabla(conn, table_name):
    create_table_query = f"""
    CREATE SCHEMA IF NOT EXISTS ia_real_data;

    CREATE TABLE IF NOT EXISTS ia_real_data.{table_name} (
        id SERIAL4 NOT NULL,
        t_ili_tid VARCHAR NULL,
        i_primer_nombre VARCHAR NULL,
        CONSTRAINT {table_name}_pkey PRIMARY KEY (id)
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_query)
    conn.commit()
    print(f"Tabla 'ia_real_data.{table_name}' verificada/creada con éxito.")

def insertar_datos_csv(conn, ruta_csv, table_name):
    CSV_COL_TID = "t_l_id"
    CSV_COL_NOMBRE = "primer_nombre"

    insert_query = f"""
        INSERT INTO ia_real_data.{table_name} (
            t_ili_tid,
            i_primer_nombre
        )
        VALUES (%s, %s);
    """

    with open(ruta_csv, mode='r', encoding='utf-8') as f:
        lector = csv.DictReader(f)
        with conn.cursor() as cursor:
            for fila in lector:
                t_ili_tid = fila.get(CSV_COL_TID, "")
                i_primer_nombre = fila.get(CSV_COL_NOMBRE, "")

                valores = (
                    t_ili_tid,
                    i_primer_nombre
                )
                cursor.execute(insert_query, valores)
        conn.commit()
    print(f"Datos insertados correctamente desde {ruta_csv} en la tabla {table_name}")

def crear_tabla_final(conn, table_name):
    create_table_query = f"""
    CREATE SCHEMA IF NOT EXISTS final;
    
    CREATE TABLE IF NOT EXISTS final.{table_name} (
        t_id_interesado varchar NOT NULL,
        nombre_apellido varchar NOT NULL,
        nombre_1 varchar NULL,
        nombre_2 varchar NULL,
        apellido_1 varchar NULL,
        apellido_2 varchar NULL,
        sexo varchar NULL,
        id serial4 NOT NULL,
        CONSTRAINT {table_name}_pk PRIMARY KEY (id)
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_query)
    conn.commit()
    print(f"Tabla 'final.{table_name}' verificada/creada con éxito.")


if __name__ == "__main__":
    main()