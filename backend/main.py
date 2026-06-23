from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Optional       # ✅ แก้ไข #1: เพิ่ม Optional
from google.cloud import bigquery
import os
import time
from threading import Lock
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "bq-key.json"
client = bigquery.Client(project="pro-analytics-db")

PENDING_WAVES_CACHE_TTL_SECONDS = 300
pending_waves_cache = {"data": None, "expires_at": 0}
pending_waves_cache_lock = Lock()

# ==================== DATA MODELS ====================
class ScanData(BaseModel):
    wave_no: str
    branch_code: Optional[str] = None
    branch_name: Optional[str] = None
    lpn: str
    type: str
    color: str
    qty: int = 1
    emp_id: Optional[str] = None

ScanData.model_rebuild()   # ← เพิ่มบรรทัดนี้

class CombineData(BaseModel):
    master_lpn: str
    child_lpns: List[str]

class CloseData(BaseModel):
    wave_no: str
    branch_code: str = "ALL"
    close_type: str

class CloseJobData(BaseModel):
    wave: str
    branch: str
    emp_id: Optional[str] = None
    completed_at: Optional[str] = None

# ==================== ROUTES & APIs ====================

@app.get("/")
async def read_root():
    return {"status": "ok", "message": "Scanner API is running perfectly!"}

# 🚀 [API 1] โหลดข้อมูล Wave
@app.get("/api/check-wave")
async def check_wave(wave_no: str):
    try:
        search_wave_id = int(wave_no.strip())
    except ValueError:
        raise HTTPException(status_code=400, detail="รหัส Wave ต้องเป็นตัวเลขเท่านั้น")

    query = f"""
        WITH ScanHistory AS (
            SELECT 
                SAFE_CAST(Wave_Number AS INT64) AS Wave_ID,
                TRIM(UPPER(LPN)) AS Clean_LPN,
                MAX(Qty) AS Scanned_Qty,
                MAX(Scan_Type) AS Scan_Type,
                MAX(Color) AS Color
            FROM `pro-analytics-db.logistics_db.app_scan_transactions`
            WHERE Wave_Number IS NOT NULL
              AND SAFE_CAST(Wave_Number AS INT64) = {search_wave_id}
            GROUP BY 1, 2
        ),
        WaveMonitoringFiltered AS (
            SELECT 
                TRIM(CAST(Branch_Code AS STRING)) AS Branch_Code, 
                TRIM(CAST(Branch_Name AS STRING)) AS Branch_Name,
                SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) AS Wave_ID
            FROM `pro-analytics-db.logistics_db.wave_monitoring`
            WHERE SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) = {search_wave_id}
        )
        SELECT 
            d.LPN, 
            d.Zone, 
            d.Wave_Number AS Full_Wave, 
            TRIM(CAST(d.Branch_Code AS STRING)) AS Branch_Code, 
            COALESCE(MAX(m.Branch_Name), 'Unknown') AS Branch_Name,
            MAX(IFNULL(d.Total_Qty, 1)) AS Total_Qty, 
            IF(MAX(s.Clean_LPN) IS NOT NULL, 'Scanned', 'Pending') AS status,
            MAX(s.Scanned_Qty) AS qty,
            MAX(s.Scan_Type) AS scan_type,
            COALESCE(MAX(TRIM(CAST(d.Owner AS STRING))), 'Unknown') AS owner,
            MAX(s.Color) AS color
        FROM `pro-analytics-db.logistics_db.wave_lpn_detail_record` AS d
        LEFT JOIN WaveMonitoringFiltered AS m 
          ON TRIM(CAST(d.Branch_Code AS STRING)) = m.Branch_Code
         AND SAFE_CAST(d.Wave_Number AS INT64) = m.Wave_ID
        LEFT JOIN ScanHistory AS s
          ON SAFE_CAST(d.Wave_Number AS INT64) = s.Wave_ID 
         AND TRIM(UPPER(d.LPN)) = s.Clean_LPN
        WHERE d.Wave_Number = '{wave_no.strip()}' OR SAFE_CAST(d.Wave_Number AS INT64) = {search_wave_id}
        GROUP BY d.LPN, d.Zone, d.Branch_Code, d.Wave_Number
    """

    meta_query = f"""
        SELECT 
            COALESCE(MAX(TRIM(CAST(Vehicle_Booking_No AS STRING))), '') AS booking_no,
            COALESCE(MAX(TRIM(CAST(License_Plate AS STRING))), '') AS license_plate
        FROM `pro-analytics-db.logistics_db.wave_monitoring`
        WHERE SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) = {search_wave_id}
    """

    try:
        # Load wave metadata
        meta_job = client.query(meta_query)
        meta_rows = list(meta_job.result())
        booking_no = ""
        license_plate = ""
        if len(meta_rows) > 0:
            booking_no = meta_rows[0]["booking_no"] or ""
            license_plate = meta_rows[0]["license_plate"] or ""

        query_job = client.query(query)
        results = query_job.result()

        lpn_list = []
        zones_calc = {}
        row_count = 0
        real_wave_no = wave_no

        for row in results:
            row_count += 1
            real_wave_no = row["Full_Wave"]
            z = row["Zone"] if row["Zone"] else "N/A"
            raw_code = row["Branch_Code"]
            br_code_str = str(raw_code).strip() if raw_code else "Unknown"
            br_name = row["Branch_Name"]

            lpn_list.append({
                "lpn": row["LPN"],
                "zone": z,
                "branch": br_code_str,
                "branch_name": br_name,
                "status": row["status"],
                "total_qty": row["Total_Qty"],
                "qty": row["qty"] if row["qty"] is not None else 0,
                "scan_type": row["scan_type"],
                "owner": row["owner"] or "Unknown",
                "color": row["color"] or "None",
                "wave_no": str(row["Full_Wave"]).strip()
            })

            if z not in zones_calc:
                zones_calc[z] = {"zone": z, "scanned": 0, "total": 0}
            zones_calc[z]["total"] += 1
            if row["status"] == "Scanned":              # ✅ แก้ไข #3: นับยอด scanned ใน zone
                zones_calc[z]["scanned"] += 1

        if row_count == 0:
            raise HTTPException(status_code=404, detail=f"ไม่พบข้อมูล Wave [{wave_no}]")

        return {
            "wave_no": real_wave_no,
            "booking_no": booking_no,
            "license_plate": license_plate,
            "lpn_list": lpn_list,
            "zone_summary": list(zones_calc.values())
        }
    except Exception as e:
        print(f"🚨 SELECT ERROR: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# 🚀 [API 1.5] โหลดข้อมูล Booking
@app.get("/api/check-booking")
async def check_booking(booking_no: str):
    clean_booking = booking_no.strip().upper()
    
    wave_query = f"""
        SELECT DISTINCT 
            TRIM(Wave_Number) AS Wave_Number,
            TRIM(License_Plate) AS License_Plate
        FROM `pro-analytics-db.logistics_db.wave_monitoring`
        WHERE (Vehicle_Booking_No = '{clean_booking}' OR Vehicle_Booking_No = '{booking_no.strip()}')
          AND Wave_Number IS NOT NULL
          AND Wave_Number != ''
    """
    
    try:
        wave_job = client.query(wave_query)
        wave_rows = list(wave_job.result())
        
        if len(wave_rows) == 0:
            raise HTTPException(status_code=404, detail=f"ไม่พบข้อมูลสำหรับ Booking No. [{booking_no}]")
            
        wave_ids = []
        license_plate = ""
        for r in wave_rows:
            w_no = r["Wave_Number"]
            import re
            cleaned_w_no = re.sub(r'[^0-9]', '', w_no)
            if cleaned_w_no:
                try:
                    wave_ids.append(int(cleaned_w_no))
                except ValueError:
                    continue
            if r["License_Plate"] and not license_plate:
                license_plate = r["License_Plate"]

        if not wave_ids:
            raise HTTPException(status_code=404, detail=f"ไม่พบรหัส Wave ที่ถูกต้องใน Booking [{booking_no}]")
            
        wave_ids_str = ",".join(map(str, wave_ids))
        
        query = f"""
            WITH ScanHistory AS (
                SELECT 
                    SAFE_CAST(Wave_Number AS INT64) AS Wave_ID,
                    TRIM(UPPER(LPN)) AS Clean_LPN,
                    MAX(Qty) AS Scanned_Qty,
                    MAX(Scan_Type) AS Scan_Type,
                    MAX(Color) AS Color
                FROM `pro-analytics-db.logistics_db.app_scan_transactions`
                WHERE Wave_Number IS NOT NULL
                  AND SAFE_CAST(Wave_Number AS INT64) IN ({wave_ids_str})
                GROUP BY 1, 2
            ),
            WaveMonitoringFiltered AS (
                SELECT 
                    TRIM(CAST(Branch_Code AS STRING)) AS Branch_Code, 
                    TRIM(CAST(Branch_Name AS STRING)) AS Branch_Name,
                    SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) AS Wave_ID
                FROM `pro-analytics-db.logistics_db.wave_monitoring`
                WHERE SAFE_CAST(REGEXP_REPLACE(TRIM(CAST(Wave_Number AS STRING)), r'[^0-9]', '') AS INT64) IN ({wave_ids_str})
            )
            SELECT 
                d.LPN, 
                d.Zone, 
                d.Wave_Number AS Full_Wave, 
                TRIM(CAST(d.Branch_Code AS STRING)) AS Branch_Code, 
                COALESCE(MAX(m.Branch_Name), 'Unknown') AS Branch_Name,
                MAX(IFNULL(d.Total_Qty, 1)) AS Total_Qty, 
                IF(MAX(s.Clean_LPN) IS NOT NULL, 'Scanned', 'Pending') AS status,
                MAX(s.Scanned_Qty) AS qty,
                MAX(s.Scan_Type) AS scan_type,
                COALESCE(MAX(TRIM(CAST(d.Owner AS STRING))), 'Unknown') AS owner,
                MAX(s.Color) AS color
            FROM `pro-analytics-db.logistics_db.wave_lpn_detail_record` AS d
            LEFT JOIN WaveMonitoringFiltered AS m 
              ON TRIM(CAST(d.Branch_Code AS STRING)) = m.Branch_Code
             AND SAFE_CAST(d.Wave_Number AS INT64) = m.Wave_ID
            LEFT JOIN ScanHistory AS s
              ON SAFE_CAST(d.Wave_Number AS INT64) = s.Wave_ID 
             AND TRIM(UPPER(d.LPN)) = s.Clean_LPN
            WHERE SAFE_CAST(d.Wave_Number AS INT64) IN ({wave_ids_str})
            GROUP BY d.LPN, d.Zone, d.Branch_Code, d.Wave_Number
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        lpn_list = []
        zones_calc = {}
        waves_included = set()
        row_count = 0
        
        for row in results:
            row_count += 1
            full_wave = row["Full_Wave"]
            waves_included.add(str(full_wave).strip())
            z = row["Zone"] if row["Zone"] else "N/A"
            br_code_str = str(row["Branch_Code"]).strip() if row["Branch_Code"] else "Unknown"
            
            lpn_list.append({
                "lpn": row["LPN"],
                "zone": z,
                "branch": br_code_str,
                "branch_name": row["Branch_Name"],
                "status": row["status"],
                "total_qty": row["Total_Qty"],
                "qty": row["qty"] if row["qty"] is not None else 0,
                "scan_type": row["scan_type"],
                "owner": row["owner"] or "Unknown",
                "color": row["color"] or "None",
                "wave_no": str(full_wave).strip()
            })
            
            if z not in zones_calc:
                zones_calc[z] = {"zone": z, "scanned": 0, "total": 0}
            zones_calc[z]["total"] += 1
            if row["status"] == "Scanned":
                zones_calc[z]["scanned"] += 1
                
        if row_count == 0:
            raise HTTPException(status_code=404, detail=f"ไม่พบข้อมูลสำหรับรหัส Wave ใน Booking [{booking_no}]")
            
        return {
            "booking_no": booking_no,
            "license_plate": license_plate,
            "waves": list(waves_included),
            "lpn_list": lpn_list,
            "zone_summary": list(zones_calc.values())
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"🚨 BOOKING SELECT ERROR: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# 🚀 [API 2] บันทึกข้อมูลสแกนทีละกล่อง
@app.post("/api/scan")
async def process_scan(data: ScanData):
    wave_clean = str(int(data.wave_no))

    def esc(val):
        return (val or "").replace("\\", "\\\\").replace("'", "\\'")

    branch_val      = esc(data.branch_code)
    branch_name_val = esc(data.branch_name)
    emp_val         = esc(data.emp_id)
    lpn_val         = esc(data.lpn)
    type_val        = esc(data.type)
    color_val       = esc(data.color)

    # ✅ ตรวจสอบว่า LPN นี้มีอยู่ใน Wave และ Branch จริงๆ ก่อน
    check_query = f"""
        SELECT COUNT(*) AS found
        FROM `pro-analytics-db.logistics_db.wave_lpn_detail_record`
        WHERE SAFE_CAST(Wave_Number AS INT64) = {wave_clean}
          AND TRIM(UPPER(CAST(LPN AS STRING))) = '{lpn_val.upper()}'
          AND TRIM(UPPER(CAST(Branch_Code AS STRING))) = '{branch_val.upper()}'
    """

    try:
        check_result = client.query(check_query).result()
        found = next(iter(check_result))["found"]

        if found == 0:
            print(f"🚫 REJECTED | LPN: {lpn_val} ไม่พบใน Wave {wave_clean} / Branch {branch_val}")
            raise HTTPException(
                status_code=400,
                detail=f"ไม่พบ LPN [{data.lpn}] ใน Wave {wave_clean} สาขา {data.branch_code}"
            )
    except HTTPException:
        raise
    except Exception as e:
        print(f"🚨 CHECK ERROR | LPN: {lpn_val} | Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

    # ✅ ผ่านการตรวจแล้ว ค่อย INSERT
    print(f"📦 SCAN | Wave: {wave_clean} | LPN: {lpn_val} | Branch: {branch_val} | Emp: {emp_val}")

    insert_query = f"""
        INSERT INTO `pro-analytics-db.logistics_db.app_scan_transactions`
        (`Wave_Number`, `LPN`, `Scan_Type`, `Color`, `Qty`, `Timestamp`, `Branch_Code`, `Branch_Name`, `Emp_ID`)
        VALUES
        ('{wave_clean}', '{lpn_val}', '{type_val}', '{color_val}', {data.qty},
         CURRENT_TIMESTAMP(), '{branch_val}', '{branch_name_val}', '{emp_val}')
    """
    try:
        client.query(insert_query).result()
        print(f"✅ SAVED | LPN: {lpn_val}")
        return {"status": "success", "message": "Saved"}
    except Exception as e:
        print(f"🚨 INSERT ERROR | LPN: {lpn_val} | Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# 🚀 [API 5] ปิดจบงานสาขา
@app.post("/api/close-job")
async def close_job(data: CloseJobData):
    try:
        wave_clean = str(int(data.wave.strip()))
    except ValueError:
        raise HTTPException(status_code=400, detail="รหัส Wave ไม่ถูกต้อง")

    def esc(val):
        return str(val or "").replace("\\", "\\\\").replace("'", "\\'")

    branch = esc(data.branch.strip().upper())
    emp_id = esc((data.emp_id or "").strip())
    completed_at = esc((data.completed_at or "").strip())
    timestamp_expr = "CURRENT_TIMESTAMP()"
    if completed_at:
        timestamp_expr = f"COALESCE(SAFE_CAST('{completed_at}' AS TIMESTAMP), CURRENT_TIMESTAMP())"

    insert_zero_query = f"""
        INSERT INTO `pro-analytics-db.logistics_db.app_scan_transactions`
        (`Wave_Number`, `LPN`, `Scan_Type`, `Color`, `Qty`, `Timestamp`)
        WITH Expected AS (
            SELECT TRIM(UPPER(CAST(LPN AS STRING))) AS LPN
            FROM `pro-analytics-db.logistics_db.wave_lpn_detail_record`
            WHERE SAFE_CAST(Wave_Number AS INT64) = {wave_clean}
              AND TRIM(UPPER(CAST(Branch_Code AS STRING))) = '{branch}'
        ),
        Scanned AS (
            SELECT TRIM(UPPER(CAST(LPN AS STRING))) AS LPN
            FROM `pro-analytics-db.logistics_db.app_scan_transactions`
            WHERE SAFE_CAST(Wave_Number AS INT64) = {wave_clean}
        )
        SELECT '{wave_clean}', e.LPN, 'AUTO_NOT_FOUND', 'None', 0, CURRENT_TIMESTAMP()
        FROM Expected e
        LEFT JOIN Scanned s ON e.LPN = s.LPN
        WHERE s.LPN IS NULL
    """

    insert_close_marker = f"""
        INSERT INTO `pro-analytics-db.logistics_db.app_scan_transactions`
        (`Wave_Number`, `LPN`, `Scan_Type`, `Color`, `Qty`, `Timestamp`, `Branch_Code`, `Emp_ID`)
        VALUES ('{wave_clean}', 'BRANCH_{branch}', 'CLOSE_JOB', 'None', 0, {timestamp_expr}, '{branch}', '{emp_id}')
    """

    try:
        client.query(insert_zero_query).result()
        client.query(insert_close_marker).result()
        return {
            "status": "success",
            "message": f"ปิดจบงานสาขา {branch} และบันทึกยอด 0 ให้กล่องที่ค้างสำเร็จ!",
            "completed_at": data.completed_at
        }
    except Exception as e:
        print(f"🚨 CLOSE JOB ERROR: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

def query_pending_waves_from_bigquery():
    query = """
        SELECT Wave_Number
        FROM `pro-analytics-db.logistics_db.wave_lpn_detail_record`
        WHERE Wave_Number IS NOT NULL 
          AND TRIM(Wave_Number) != ''
          AND REGEXP_CONTAINS(TRIM(Wave_Number), r'^[0-9]+$')
        GROUP BY Wave_Number
        ORDER BY MAX(Saved_Timestamp) DESC
        LIMIT 50
    """
    query_job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(use_query_cache=True)
    )
    results = query_job.result()
    waves = [{"wave_no": str(row["Wave_Number"]).strip()} for row in results]
    return {"success": True, "waves": waves, "cached": False}


def refresh_pending_waves_cache():
    data = query_pending_waves_from_bigquery()
    with pending_waves_cache_lock:
        pending_waves_cache["data"] = data
        pending_waves_cache["expires_at"] = time.time() + PENDING_WAVES_CACHE_TTL_SECONDS
    return data


# 🚀 [API] โหลดรายการ Wave
@app.get("/api/pending-waves")
async def get_pending_waves(background_tasks: BackgroundTasks, force: bool = False):
    now = time.time()
    with pending_waves_cache_lock:
        cached_data = pending_waves_cache["data"]
        is_fresh = pending_waves_cache["expires_at"] > now

    if cached_data and not force:
        response = {**cached_data, "cached": True, "stale": not is_fresh}
        if not is_fresh:
            background_tasks.add_task(refresh_pending_waves_cache)
        return response

    try:
        return refresh_pending_waves_cache()
    except Exception as e:
        if cached_data:
            return {**cached_data, "cached": True, "stale": True, "error": str(e)}
        return {"success": False, "error": str(e)}
