# Tote

รวมโปรเจกต์ scanner ไว้ใน repo เดียว โดยยังแยก frontend/backend ชัดเจน

## โครงสร้าง

- `backend/` FastAPI + BigQuery API
- `frontend/` หน้าเว็บ scanner แบบ static HTML

## รัน backend

```powershell
cd backend
python -m pip install -r requirements.txt
python -m uvicorn main:app --reload
```

## เปิด frontend

เปิด `frontend/index.html` ใน browser ได้เลย หรือแก้ `backendApiUrl` ในไฟล์นี้ให้ชี้ API ที่ต้องการใช้งาน

## หมายเหตุสำคัญ

`backend/bq-key.json` เป็นไฟล์ key ส่วนตัว อย่า commit หรืออัปขึ้น Git/public
