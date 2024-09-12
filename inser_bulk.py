import os
import streamlit as st
import mysql.connector
from mysql.connector import Error

def insert_students_in_bulk(df, table_name='students'):
    connection = None
    cursor = None

    try:
        # Configura la conexión a la base de datos
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
        
        st.write("Conexión a la base de datos establecida.")  # Mensaje de depuración

        if connection.is_connected():
            cursor = connection.cursor()
            st.write("Cursor creado.")  # Mensaje de depuración

            # Prepara la consulta SQL de inserción
            insert_query = f"""
            INSERT INTO {table_name} (
                curso_id, nombre_curso, profesor, horario, correo_profesor, 
                curso_id_estudiante, id_estudiante, nombre_estudiante, apellido_estudiante, edad, correo_estudiante
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # Extrae las 11 columnas necesarias del DataFrame
            students_data = df[['Curso_ID', 'Nombre_Curso', 'Profesor', 'Horario', 'Correo_x', 
                                'Curso_ID.1', 'ID_Estudiante', 'Nombre', 'Apellido', 'Edad', 'Correo_y']].to_records(index=False).tolist()

            # Ejecuta la consulta en bloques
            cursor.executemany(insert_query, students_data)
            st.write(f"Se intentaron insertar {cursor.rowcount} filas.")  # Mensaje de depuración
            
            # Confirma la transacción
            connection.commit()
            st.success(f"{cursor.rowcount} filas insertadas correctamente.")

    except Error as e:
        st.error(f"Error: {e}")  # Mensaje de error
        if connection:
            connection.rollback()

    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()
