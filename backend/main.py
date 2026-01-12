import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import mysql.connector
import json
import numpy as np
from scipy.interpolate import interp1d

app = FastAPI()

# 1. Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 2. Serve Static Files (CSS, JS, Images)
# Move your frontend files into backend/static/
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# 3. Serve the Frontend (index.html)
@app.get("/")
async def read_index():
    index_path = os.path.join("static", "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"error": "Frontend files not found. Ensure index.html is in backend/static/"}

# Database Connection Helper
def get_db_connection():
    return mysql.connector.connect(
        # These names must match your Railway Variables tab exactly
        host=os.getenv("MYSQLHOST"),
        user=os.getenv("MYSQLUSER"),
        password=os.getenv("MYSQLPASSWORD"),
        database=os.getenv("MYSQLDATABASE"),
        port=int(os.getenv("MYSQLPORT", 3306))
    )

@app.get("/get_years")
def get_years():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT year FROM papers ORDER BY year DESC")
        years = [row[0] for row in cursor.fetchall()]
        return years
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn: conn.close()

@app.get("/get_dates")
def get_dates(year: int):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT date FROM papers WHERE year = %s ORDER BY date ASC", (year,))
        dates = [str(row[0]) for row in cursor.fetchall()]
        return dates
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn: conn.close()

@app.get("/predict")
def predict(year: int, date: str, shift: str, marks: float):
    conn = get_db_connection()
    try:
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT id FROM papers WHERE year=%s AND date=%s AND shift=%s", (year, date, shift))
        paper = cursor.fetchone()
        if not paper:
            return {"error": "Shift not found in database."}

        cursor.execute("""
            SELECT pc.curve_data, ds.reliability_weight 
            FROM percentile_curves pc
            JOIN data_sources ds ON pc.source_id = ds.id
            WHERE pc.paper_id = %s
        """, (paper['id'],))
        results = cursor.fetchall()
        
        if not results:
            return {"error": "No data curves found for this shift."}

        cursor.execute("SELECT total_candidates FROM annual_stats WHERE year = %s", (year,))
        stats = cursor.fetchone()
        total_candidates = stats['total_candidates'] if (stats and stats['total_candidates']) else 1250000

        total_weighted_p, total_weight = 0, 0
        for res in results:
            curve = json.loads(res['curve_data'])
            x = np.array([float(m) for m in curve.keys()])
            y = np.array([float(p) for p in curve.values()])
            
            idx = np.argsort(x)
            f = interp1d(x[idx], y[idx], kind='linear', fill_value="extrapolate")
            
            p_val = max(0, min(100, float(f(marks))))
            weight = float(res['reliability_weight'])
            total_weighted_p += (p_val * weight)
            total_weight += weight

        avg_p = total_weighted_p / total_weight
        predicted_rank = int(((100 - avg_p) / 100) * total_candidates) + 1

        return {
            "percentile": round(avg_p, 3),
            "predicted_rank": predicted_rank,
            "total_candidates_year": total_candidates,
            "range_low": round(max(0, avg_p - 0.15), 2),
            "range_high": round(min(100, avg_p + 0.15), 2)
        }

    except Exception as e:
        print(f"Server Error: {str(e)}")
        return {"error": f"Math Error: {str(e)}"}
    finally:
        conn.close()

