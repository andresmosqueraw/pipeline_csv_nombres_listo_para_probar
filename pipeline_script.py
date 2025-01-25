import os
import psycopg2
import csv
import psycopg2.extras

def main():
    # Datos cambiantes
    CSV_PATH = "CSVs/procesamiento_interesados_iza.csv"
    TABLE_NAME = "iza"
    SCHEMA_NAME_CSV = "ia_real_data"
    SCHEMA_NAME_FINAL = "final"
    
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
        crear_tabla(conn, TABLE_NAME, SCHEMA_NAME_CSV)

        # 4. Verificar si la tabla ya tiene datos
        if not tabla_tiene_datos(conn, TABLE_NAME, SCHEMA_NAME_CSV):
            # Insertar los datos desde el CSV local
            insertar_datos_csv(conn, CSV_PATH, TABLE_NAME, SCHEMA_NAME_CSV)
        else:
            print(f"La tabla {SCHEMA_NAME_CSV}.{TABLE_NAME} ya contiene datos. No se insertarán nuevos datos.")

        # 5. Crear la tabla final donde se insertaran los datos resultantes del modelo de IA
        crear_tabla_final(conn, TABLE_NAME, SCHEMA_NAME_FINAL)

    except Exception as e:
        print(f"Ocurrió un error en el pipeline: {e}")
    finally:
        conn.close()

def crear_tabla(conn, table_name, schema_name):
    create_table_query = f"""
    CREATE SCHEMA IF NOT EXISTS {schema_name};

    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        id SERIAL4 NOT NULL,
        t_ili_tid VARCHAR NULL,
        i_primer_nombre VARCHAR NULL,
        CONSTRAINT {table_name}_pkey PRIMARY KEY (id)
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_query)
    conn.commit()
    print(f"Tabla '{schema_name}.{table_name}' verificada/creada con éxito.")

def tabla_tiene_datos(conn, table_name, schema_name):
    query = f"SELECT COUNT(*) FROM {schema_name}.{table_name};"
    with conn.cursor() as cursor:
        cursor.execute(query)
        count = cursor.fetchone()[0]
    return count > 0

def insertar_datos_csv(conn, ruta_csv, table_name, schema_name):
    CSV_COL_TID = "t_ili_tid"
    CSV_COL_NOMBRE = "i_primer_nombre"

    insert_query = f"""
        INSERT INTO {schema_name}.{table_name} (
            t_ili_tid,
            i_primer_nombre
        )
        VALUES %s;
    """

    batch_size = 1000  # Ajusta el tamaño del batch según sea necesario
    batch = []

    try:
        with open(ruta_csv, mode='r', encoding='utf-8') as f:
            lector = csv.DictReader(f)
            with conn.cursor() as cursor:
                for fila in lector:
                    t_ili_tid = fila.get(CSV_COL_TID, "")
                    i_primer_nombre = fila.get(CSV_COL_NOMBRE, "")

                    if not t_ili_tid or not i_primer_nombre:
                        print(f"Fila inválida: {fila}")
                        continue

                    batch.append((t_ili_tid, i_primer_nombre))

                    if len(batch) >= batch_size:
                        psycopg2.extras.execute_values(cursor, insert_query, batch)
                        batch = []

                # Insertar cualquier dato restante
                if batch:
                    psycopg2.extras.execute_values(cursor, insert_query, batch)

            conn.commit()
        print(f"Datos insertados correctamente desde {ruta_csv} en la tabla {schema_name}.{table_name}")
    except Exception as e:
        print(f"Error al insertar datos: {e}")

def crear_tabla_final(conn, table_name, schema_name):
    create_table_query = f"""
    CREATE SCHEMA IF NOT EXISTS {schema_name};
    
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
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
    print(f"Tabla '{schema_name}.{table_name}' verificada/creada con éxito.")

if __name__ == "__main__":
    main()