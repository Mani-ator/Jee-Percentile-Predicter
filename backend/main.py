from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import mysql.connector
import json
import numpy as np
from scipy.interpolate import interp1d

app = FastAPI()

# Enable CORS so your HTML file can talk to this script
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database Connection Helper
def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASS", "123456789"),
        database=os.getenv("DB_NAME", "jee_predictor"),
        port=int(os.getenv("DB_PORT", 3306))
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
        
        # 1. Get Paper ID
        cursor.execute("SELECT id FROM papers WHERE year=%s AND date=%s AND shift=%s", (year, date, shift))
        paper = cursor.fetchone()
        if not paper:
            return {"error": "Shift not found in database."}

        # 2. Get Percentile Data
        cursor.execute("""
            SELECT pc.curve_data, ds.reliability_weight 
            FROM percentile_curves pc
            JOIN data_sources ds ON pc.source_id = ds.id
            WHERE pc.paper_id = %s
        """, (paper['id'],))
        results = cursor.fetchall()
        
        if not results:
            return {"error": "No data curves found for this shift."}

        # 3. Get Candidate Stats (with a Fallback to prevent crashes)
        cursor.execute("SELECT total_candidates FROM annual_stats WHERE year = %s", (year,))
        stats = cursor.fetchone()
        total_candidates = stats['total_candidates'] if (stats and stats['total_candidates']) else 1250000

        # 4. Weighted Interpolation
        total_weighted_p, total_weight = 0, 0
        for res in results:
            curve = json.loads(res['curve_data'])
            x = np.array([float(m) for m in curve.keys()])
            y = np.array([float(p) for p in curve.values()])
            
            # Sort and interpolate
            idx = np.argsort(x)
            f = interp1d(x[idx], y[idx], kind='linear', fill_value="extrapolate")
            
            p_val = max(0, min(100, float(f(marks))))
            weight = float(res['reliability_weight'])
            total_weighted_p += (p_val * weight)
            total_weight += weight

        avg_p = total_weighted_p / total_weight
        
        # 5. Rank Formula: ((100 - Percentile) / 100) * Total Candidates
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

# Start with: uvicorn main:app --reload