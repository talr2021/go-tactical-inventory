import io
import os
import json
import pandas as pd
import requests
from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from datetime import datetime

# יצירת אפליקציה FastAPI
app = FastAPI()

# חיבור לתיקיות סטטיות ותבניות
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# קריאת משתני סביבה (WooCommerce API)
WOOCOMMERCE_URL = os.getenv("WOOCOMMERCE_URL", "")
WOOCOMMERCE_KEY = os.getenv("WOOCOMMERCE_KEY", "")
WOOCOMMERCE_SECRET = os.getenv("WOOCOMMERCE_SECRET", "")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """עמוד הבית - העלאת קובץ"""
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/preview", response_class=HTMLResponse)
async def preview(request: Request, file: UploadFile = File(...)):
    """תצוגה מקדימה לפני עדכון"""
    try:
        content = await file.read()
        filename = file.filename.lower()

        # זיהוי סוג הקובץ
        if filename.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(content), encoding="utf-8", engine="python")
        elif filename.endswith((".xls", ".xlsx")):
            df = pd.read_excel(io.BytesIO(content), engine="openpyxl")
        else:
            return HTMLResponse(
                content="<h2>שגיאה: פורמט קובץ לא נתמך (יש להשתמש ב־CSV או XLSX בלבד)</h2>",
                status_code=400,
            )

        # בדיקת עמודות נדרשות
        required_cols = {"sku", "quantity"}
        if not required_cols.issubset(df.columns):
            return HTMLResponse(
                content=f"<h2>שגיאה: חסרות עמודות חובה ({', '.join(required_cols)})</h2>",
                status_code=400,
            )

        # ניקוי ערכים ריקים
        df = df.fillna("")
        rows = df.to_dict(orient="records")

        stats = {
            "total_rows": len(df),
            "unique_skus": df["sku"].nunique(),
            "total_quantity": int(pd.to_numeric(df["quantity"], errors="coerce").fillna(0).sum()),
        }

        return templates.TemplateResponse(
            "preview.html",
            {
                "request": request,
                "rows": rows,
                "stats": stats,
                "errors": [],
                "raw": df.to_json(orient="records"),
                "do_stock": True,
                "do_prices": False,
                "dry_run": True,
            },
        )

    except Exception as e:
        import traceback
        traceback.print_exc()
        return HTMLResponse(
            content=f"<h2>שגיאה פנימית בעת קריאת הקובץ:</h2><pre>{str(e)}</pre>",
            status_code=500,
        )


@app.post("/apply", response_class=HTMLResponse)
async def apply_update(
    request: Request,
    raw: str = Form(...),
    do_stock: bool = Form(True),
    do_prices: bool = Form(False),
    dry_run: bool = Form(True),
):
    """ביצוע העדכון בפועל"""
    try:
        df = pd.DataFrame(json.loads(raw))
        not_found = []
        updated = []
        errors = []

        for _, row in df.iterrows():
            sku = str(row.get("sku", "")).strip()
            qty = int(row.get("quantity", 0) or 0)
            reg_price = row.get("regular_price", "")
            sale_price = row.get("sale_price", "")

            if not sku:
                continue

            if dry_run:
                updated.append(sku)
                continue

            # בקשת WooCommerce לפי SKU
            endpoint = f"{WOOCOMMERCE_URL}/wp-json/wc/v3/products?sku={sku}"
            resp = requests.get(endpoint, auth=(WOOCOMMERCE_KEY, WOOCOMMERCE_SECRET))

            if resp.status_code != 200:
                errors.append(f"שגיאה בגישה ל־API עבור {sku}")
                continue

            products = resp.json()
            if not products:
                not_found.append(sku)
                continue

            product_id = products[0]["id"]
            data = {}

            if do_stock:
                data["stock_quantity"] = qty
                data["manage_stock"] = True
            if do_prices:
                if reg_price != "":
                    data["regular_price"] = str(reg_price)
                if sale_price != "":
                    data["sale_price"] = str(sale_price)
                else:
                    data["sale_price"] = ""

            # עדכון בפועל
            put_url = f"{WOOCOMMERCE_URL}/wp-json/wc/v3/products/{product_id}"
            r = requests.put(put_url, json=data, auth=(WOOCOMMERCE_KEY, WOOCOMMERCE_SECRET))
            if r.status_code in (200, 201):
                updated.append(sku)
            else:
                errors.append(f"שגיאה בעדכון SKU {sku}: {r.text}")

        summary = {
            "updated_total": len(updated),
            "not_found_count": len(not_found),
            "not_found": not_found,
            "errors_count": len(errors),
            "errors": errors,
            "log_name": f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
        }

        # יצירת לוג
        log_content = json.dumps(summary, ensure_ascii=False, indent=2)
        log_path = f"/tmp/{summary['log_name']}"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(log_content)

        return templates.TemplateResponse(
            "result.html",
            {"request": request, "summary": summary},
        )

    except Exception as e:
        import traceback
        traceback.print_exc()
        return HTMLResponse(
            content=f"<h2>שגיאה בעת עדכון הנתונים:</h2><pre>{str(e)}</pre>",
            status_code=500,
        )


@app.get("/download-log", response_class=FileResponse)
async def download_log(name: str):
    """הורדת קובץ לוג"""
    path = f"/tmp/{name}"
    if os.path.exists(path):
        return FileResponse(path, filename=name, media_type="text/plain")
    return HTMLResponse(content="<h2>קובץ לוג לא נמצא</h2>", status_code=404)


@app.get("/health", response_class=HTMLResponse)
async def health():
    """בדיקת תקינות בסיסית"""
    return HTMLResponse("<h3>✅ האפליקציה פועלת כראוי</h3>")
