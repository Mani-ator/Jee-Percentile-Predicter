import mysql.connector
import json
import csv

# 1. Database Configuration
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "123456789", 
    "database": "jee_predictor"
}

# Global connection and cursor
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

def add_paper_and_data(year, session, date_str, shift, source, marks_data):
    try:
        # 1. Get or Create the Source
        cursor.execute("SELECT id FROM data_sources WHERE source_name = %s", (source,))
        source_row = cursor.fetchone()
        
        if source_row:
            source_id = source_row[0]
        else:
            # If source is missing, add it with a default weight
            print(f"Adding missing source to DB: {source}")
            cursor.execute("INSERT INTO data_sources (source_name, reliability_weight) VALUES (%s, 0.85)", (source,))
            conn.commit()
            source_id = cursor.lastrowid

        # 2. Get or Create the Paper
        cursor.execute("SELECT id FROM papers WHERE year=%s AND date=%s AND shift=%s", (year, date_str, shift))
        paper_row = cursor.fetchone()
        
        if paper_row:
            paper_id = paper_row[0]
        else:
            cursor.execute("INSERT INTO papers (year, session, date, shift) VALUES (%s, %s, %s, %s)", 
                           (year, session, date_str, shift))
            conn.commit()
            paper_id = cursor.lastrowid

        # 3. Add the Percentile Curve Data
        # Convert dict to JSON string for MySQL
        curve_json = json.dumps(marks_data)
        
        # Use REPLACE INTO so we don't get duplicates if we run the script twice
        cursor.execute("""
            REPLACE INTO percentile_curves (paper_id, source_id, curve_data)
            VALUES (%s, %s, %s)
        """, (paper_id, source_id, curve_json))
        
        conn.commit()
        print(f"✅ Success: {year} | {date_str} | {shift} | Source: {source}")

    except Exception as e:
        print(f"❌ Error on {year} {date_str}: {str(e)}")
        conn.rollback()

def run_bulk_import(csv_filename):
    print(f"Starting import from {csv_filename}...")
    try:
        with open(csv_filename, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Basic cleaning of marks_json to ensure it loads
                marks_json_str = row['marks_json'].strip()
                marks_data = json.loads(marks_json_str)
                
                add_paper_and_data(
                    year=int(row['year']),
                    session=int(row['session']),
                    date_str=row['date'],
                    shift=row['shift'],
                    source=row['source_name'],
                    marks_data=marks_data
                )
        print("\n--- Import Completed Successfully ---")
    except FileNotFoundError:
        print(f"Error: Could not find {csv_filename}. Make sure it's in the same folder!")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run_bulk_import('jee_data_collection.csv')