import streamlit as st
import pandas as pd
from inser_bulk import insert_students_in_bulk

def _extract_students_from_excel(excel_file):
    """Extracts student information from the provided Excel file."""
    try:
        df = pd.read_excel(excel_file)
        return df
    except Exception as e:
        st.write(f"Error reading the Excel file: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error

st.title("Upload two students attendance list Excel files")

uploaded_files = st.file_uploader("Attendance list Excel files", type=["xls", "xlsx"], accept_multiple_files=True)

if uploaded_files is not None:
    if len(uploaded_files) == 2:
        # Extract data from both files
        df1 = _extract_students_from_excel(uploaded_files[0])
        df2 = _extract_students_from_excel(uploaded_files[1])

        if not df1.empty and not df2.empty:
            # Merge the two DataFrames based on 'Curso_ID'
            combined_df = pd.merge(df1, df2, on='Curso_ID', how='inner')

            st.write("Files were uploaded and merged successfully.")
            st.write(combined_df)

            if st.button("Insert Merged Data into Database"):
                insert_students_in_bulk(combined_df)
                st.success("Data inserted into the database successfully.")
        else:
            st.write("One or both of the files are invalid.")
    else:
        st.write("Please upload exactly two files.")
