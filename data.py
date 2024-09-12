import pandas as pd
import mysql.connector
from mysql.connector import Error

def extract_and_combine_data_from_excel(estudiantes_file, cursos_file):
    """universidad."""
    try:
       
        df_estudiantes = pd.read_excel(estudiantes_file)
        df_cursos = pd.read_excel(cursos_file)
    except Exception as e:
        raise Exception(f"Error al leer los archivos de Excel: {e}")

    
    df_estudiantes = df_estudiantes.rename(columns={
        'ID del Estudiante': 'ID del Estudiante',
        'Nombre': 'Nombre',
        'Edad': 'Edad',
        'Curso_ID': 'Curso_ID'
    })

    df_cursos = df_cursos.rename(columns={
        'Curso_ID': 'Curso_ID',
        'Nombre del Curso': 'Nombre del Curso',
        'Duración': 'Duración'
    })

    
    df_combinado = pd.merge(df_estudiantes, df_cursos, on='Curso_ID', how='inner')

    return df_combinado

def save_to_database(df_combinado):
    """Inserta los datos combinados."""
    try:
    
        connection = mysql.connector.connect(
            host='localhost',
            user='valentina',  
            password='valentina',  
            database='universidad'  
        )

        if connection.is_connected():
            cursor = connection.cursor()

            
            create_table_query = """
            CREATE TABLE IF NOT EXISTS estudiantes_cursos (
                id INT AUTO_INCREMENT PRIMARY KEY,
                id_estudiante VARCHAR(255),
                nombre_estudiante VARCHAR(255),
                edad INT,
                curso_id VARCHAR(255),
                nombre_curso VARCHAR(255),
                duracion VARCHAR(255)
            )
            """
            cursor.execute(create_table_query)

           
            insert_query = """
            INSERT INTO estudiantes_cursos 
            (id_estudiante, nombre_estudiante, edad, curso_id, nombre_curso, duracion)
            VALUES (%s, %s, %s, %s, %s, %s)
            """

            
            data_to_insert = df_combinado[['ID del Estudiante', 'Nombre', 'Edad', 'Curso_ID', 'Nombre del Curso', 'Duración']].values.tolist()

            
            cursor.executemany(insert_query, data_to_insert)
            connection.commit()

            
            cursor.execute("SELECT COUNT(*) FROM estudiantes_cursos")
            row_count = cursor.fetchone()[0]

            cursor.close()
            connection.close()

            if row_count > 0:
                return f"Datos guardados en la base de datos correctamente. Total de registros: {row_count}."
            else:
                return "No se insertaron datos en la base de datos."
        else:
            raise Exception("No se pudo conectar a la base de datos.")
    except Error as err:
        raise Exception(f"Error al conectar o insertar en la base de datos: {err}")

