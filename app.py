import re
from csv import reader
from datetime import datetime
import psycopg2
from psycopg2 import pool

# Initialize connection pool
data_pool = psycopg2.pool.SimpleConnectionPool(1, 20,
                                               dbname='hdc',
                                               host='localhost',
                                               user='hdcroot',
                                               password='testPass123')

def clean_value(val):
    """Replace various representations of 'not available' with None."""
    na_values = {'', ' ', 'na', 'NA', 'N.A', 'n.a', 'n.a.'}
    return None if val in na_values else val

def normalize_date(date_str):
    """Convert different date formats to standardized YYYY-MM-DD or return None if invalid."""
    date_formats = ["%Y.%m.%d", "%Y/%m/%d", "%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"]
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

def process_ransom_field(value):
    """Remove decimal points from ransom amounts."""
    if value and re.match(r'^\d+\.\d+$', value):
        return value.replace('.', '')
    return value

def process_row(row):
    """Clean and transform each row of CSV data."""
    row = [clean_value(val) for val in row]  # Clean NA values
    processed_row = []
    
    for i, val in enumerate(row):
        # Clean dates and normalize them
        if val and re.match(r'\d', val):
            normalized_date = normalize_date(val)
            if normalized_date:
                processed_row.append(normalized_date)
            else:
                processed_row.append(val) 
        else:
            processed_row.append(val)

    # Additional field-specific processing
    processed_row[27] = process_ransom_field(processed_row[27]) 
    
    # Process specific fields that must be integers or have specific cleaning rules
    integer_fields = [4, 8, 15, 24, 25, 26, 27, 31]
    for idx in integer_fields:
        if idx < len(processed_row) and processed_row[idx] == 'Null':
            processed_row[idx] = None
    
    # Example to handle additional comment handling for custom codes
    if processed_row[25] and not normalize_date(processed_row[25]):
        processed_row[31] = f"{processed_row[31]}, Invalid date in field 25" if processed_row[31] else "Invalid date in field 25"
        processed_row[25] = None

    if processed_row[24] and not normalize_date(processed_row[24]):
        processed_row[31] = f"{processed_row[31]}, Invalid date in field 24" if processed_row[31] else "Invalid date in field 24"
        processed_row[24] = None

    return processed_row

def insert_data(row):
    """Insert a single row of data into the database."""
    conn = data_pool.getconn()
    try:
        with conn.cursor() as cursor:
            dbquery = """
            INSERT INTO captives_data (
                volume, id, name, sex, height, build, dentition, 
                special_peculiarities, date_of_birth, place_of_birth, 
                place_of_residence, residence, religion, childhood_status, 
                marital_status, number_of_children, occupation, occupation_2, 
                occupation_3, military_service, literacy, education, 
                criminal_history, crime, sentence_begins, sentence_expires, 
                prison_term_day, ransom, associates, degree_of_crime, 
                degree_of_punishment, notes, arrest_site, username
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                      %s, %s, %s, %s, %s, 'hdcroot');
            """
            cursor.execute(dbquery, tuple(row))
        conn.commit()
        print(f"Inserted row: {row}")
    except Exception as e:
        print(f"Error inserting row: {e}")
    finally:
        data_pool.putconn(conn)

def main():
    """Main function to read CSV data, process each row, and insert it into the database."""
    with open('RawCSVDataFromExcelHcr.csv', encoding='utf-8') as f:
        cases = reader(f)
        next(cases, None)  
        for row in cases:
            cleaned_row = process_row(row)
            insert_data(cleaned_row)

if __name__ == "__main__":
    main()
