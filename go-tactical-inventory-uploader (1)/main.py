
import os, io, json, time, uuid, csv, datetime, math
from typing import List, Dict, Any, Optional, Tuple
from fastapi import FastAPI, Request, UploadFile, Form, Depends, HTTPException, status, Query
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from dotenv import load_dotenv
import pandas as pd
import requests


def _read_table_from_upload(filename: str, content: bytes):
    name = (filename or "").lower()
    if name.endswith(".xlsx") or name.endswith(".xls"):
        # Excel - sheet ראשון
        return pd.read_excel(io.BytesIO(content), sheet_name=0)
    # default CSV
    try:
        return pd.read_csv(io.BytesIO(content))
    except Exception:
        return pd.read_csv(io.BytesIO(content), encoding="utf-8", engine="python")


load_dotenv()
WC_SITE = os.getenv("WC_SITE")
CK = os.getenv("WC_CK")
CS = os.getenv("WC_CS")
APP_USER = os.getenv("APP_USER") or "admin"
APP_PASS = os.getenv("APP_PASS") or "change_me"

LOG_DIR = os.path.abspath("./logs")
os.makedirs(LOG_DIR, exist_ok=True)

app = FastAPI(title="Go Tactical - Inventory Uploader")
templates = Jinja2Templates(directory="templates")
security = HTTPBasic()

def require_auth(credentials: HTTPBasicCredentials = Depends(security)):
    if not (credentials.username == APP_USER and credentials.password == APP_PASS):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated", headers={"WWW-Authenticate": "Basic"})
    return True

SESSION = requests.Session()
if CK and CS:
    SESSION.params = {"consumer_key": CK, "consumer_secret": CS}
SESSION.headers.update({"Accept": "application/json"})

VARIATION_INDEX: Dict[str, Tuple[int, int]] = {}

def _wc_get(url: str, **params) -> Any:
    r = SESSION.get(url, params=params)
    r.raise_for_status()
    return r.json()

def find_simple_or_parent_by_sku(sku: str) -> Optional[Dict[str, Any]]:
    url = f"{WC_SITE}/wp-json/wc/v3/products"
    data = _wc_get(url, sku=sku, per_page=1)
    return data[0] if data else None

def find_variation_by_sku_global(sku: str) -> Optional[Tuple[int, int]]:
    if sku in VARIATION_INDEX:
        return VARIATION_INDEX[sku]
    products_url = f"{WC_SITE}/wp-json/wc/v3/products"
    page = 1
    per_page = 50
    while True:
        vars_page = _wc_get(products_url, type="variable", per_page=per_page, page=page)
        if not vars_page:
            break
        for parent in vars_page:
            parent_id = parent["id"]
            variations_url = f"{WC_SITE}/wp-json/wc/v3/products/{parent_id}/variations"
            vpage = 1
            while True:
                variations = _wc_get(variations_url, per_page=100, page=vpage)
                if not variations:
                    break
                for v in variations:
                    vsku = (v.get("sku") or "").strip()
                    if vsku:
                        VARIATION_INDEX[vsku] = (parent_id, v["id"])
                if sku in VARIATION_INDEX:
                    return VARIATION_INDEX[sku]
                vpage += 1
                time.sleep(0.05)
        page += 1
        time.sleep(0.05)
    return None

def resolve_sku(sku: str) -> Optional[Tuple[str, int, Optional[int]]]:
    prod = find_simple_or_parent_by_sku(sku)
    if prod:
        return ("product", prod["id"], None)
    v = find_variation_by_sku_global(sku)
    if v:
        parent_id, variation_id = v
        return ("variation", variation_id, parent_id)
    return None

def build_update_item_for_product(prod_id: int, qty: Optional[int], rp: Optional[str], sp: Optional[str]) -> Dict[str, Any]:
    item: Dict[str, Any] = {"id": prod_id}
    if qty is not None:
        qty = max(0, int(qty))
        item.update({"manage_stock": True, "stock_quantity": qty, "stock_status": "instock" if qty > 0 else "outofstock"})
    def clean(v):
        if v is None: return None
        s = str(v).strip()
        return s if s != "" else None
    rp = clean(rp); sp = clean(sp)
    if rp is not None: item["regular_price"] = str(rp)
    if sp is not None: item["sale_price"] = str(sp)
    elif rp is not None: item["sale_price"] = ""
    return item

def build_update_item_for_variation(variation_id: int, qty: Optional[int], rp: Optional[str], sp: Optional[str]) -> Dict[str, Any]:
    item: Dict[str, Any] = {"id": variation_id}
    if qty is not None:
        qty = max(0, int(qty))
        item.update({"manage_stock": True, "stock_quantity": qty, "stock_status": "instock" if qty > 0 else "outofstock"})
    def clean(v):
        if v is None: return None
        s = str(v).strip()
        return s if s != "" else None
    rp = clean(rp); sp = clean(sp)
    if rp is not None: item["regular_price"] = str(rp)
    if sp is not None: item["sale_price"] = str(sp)
    elif rp is not None: item["sale_price"] = ""
    return item

def batch_update_products(products_payload: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not products_payload: return {"updated": 0, "errors": []}
    url = f"{WC_SITE}/wp-json/wc/v3/products/batch"
    r = SESSION.put(url, json={"update": products_payload})
    if r.status_code >= 400:
        return {"updated": 0, "errors": [f"{r.status_code}: {r.text}"]}
    data = r.json()
    return {"updated": len(data.get("update", [])), "errors": []}

def batch_update_variations(parent_id: int, variations_payload: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not variations_payload: return {"updated": 0, "errors": []}
    url = f"{WC_SITE}/wp-json/wc/v3/products/{parent_id}/variations/batch"
    r = SESSION.put(url, json={"update": variations_payload})
    if r.status_code >= 400:
        return {"updated": 0, "errors": [f"{r.status_code}: {r.text}"]}
    data = r.json()
    return {"updated": len(data.get("update", [])), "errors": []}

def write_csv_log(rows: List[Dict[str, Any]]) -> str:
    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    name = f"log-{ts}-{uuid.uuid4().hex[:8]}.csv"
    path = os.path.join(LOG_DIR, name)
    fieldnames = ["sku","action","message","kind","parent_id","object_id","quantity","regular_price","sale_price"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})
    return name

@app.get("/", response_class=HTMLResponse)
def index(request: Request, _: bool = Depends(require_auth)):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/preview", response_class=HTMLResponse)
async def preview(request: Request,
                  file: UploadFile,
                  do_stock: str = Form(default="on"),
                  do_prices: str = Form(default="off"),
                  dry_run: str = Form(default="off"),
                  _: bool = Depends(require_auth)):
    content = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(content))
    except Exception:
        df = pd.read_csv(io.BytesIO(content), encoding="utf-8", engine="python")

    cols = [c.strip().lower() for c in df.columns]
    df.columns = cols

    errors = []
    if "sku" not in cols: errors.append("חסרה עמודת sku")
    do_stock_bool = (do_stock == "on")
    do_prices_bool = (do_prices == "on")
    dry_run_bool = (dry_run == "on")

    if do_stock_bool and "quantity" not in cols:
        errors.append("סימנת עדכון מלאי אבל חסרה עמודת quantity")
    if do_prices_bool and not (("regular_price" in cols) or ("sale_price" in cols)):
        errors.append("סימנת עדכון מחירים אבל חסרות עמודות regular_price / sale_price")

    if "sku" in df: df["sku"] = df["sku"].astype(str).str.strip()
    if "quantity" in df: df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)

    if do_prices_bool:
        if "regular_price" in df: df["regular_price"] = df["regular_price"].astype(str).str.strip()
        if "sale_price" in df: df["sale_price"] = df["sale_price"].astype(str).str.strip()
        def invalid_price(row):
            rp = row.get("regular_price"); sp = row.get("sale_price")
            if rp and sp:
                try: return float(sp) > float(rp)
                except: return True
            return False
        bad = df.apply(invalid_price, axis=1)
        if bad.any():
            df = df[~bad]
            errors.append(f"נפסלו {int(bad.sum())} שורות עם sale_price גבוה מ-regular_price או ערכים לא תקינים.")

    sample = df.head(10).to_dict(orient="records")
    stats = {
        "total_rows": len(df),
        "with_qty": int(df["quantity"].notna().sum()) if "quantity" in df else 0,
        "with_rp": int(df["regular_price"].notna().sum()) if "regular_price" in df else 0,
        "with_sp": int(df["sale_price"].notna().sum()) if "sale_price" in df else 0
    }
    raw_json = df.to_json(orient="records", force_ascii=False)
    return templates.TemplateResponse("preview.html", {
        "request": request,
        "errors": errors,
        "rows": sample,
        "stats": stats,
        "raw": raw_json,
        "do_stock": do_stock_bool,
        "do_prices": do_prices_bool,
        "dry_run": dry_run_bool
    })

@app.post("/apply", response_class=HTMLResponse)
async def apply(request: Request,
                raw: str = Form(...),
                do_stock: str = Form(default="true"),
                do_prices: str = Form(default="false"),
                dry_run: str = Form(default="false"),
                _: bool = Depends(require_auth)):
    rows: List[Dict[str, Any]] = json.loads(raw)
    do_stock_bool = (do_stock == "true")
    do_prices_bool = (do_prices == "true")
    dry_run_bool = (dry_run == "true")

    updated_total = 0
    not_found: List[str] = []
    errors: List[str] = []
    log_rows: List[Dict[str, Any]] = []

    product_payload: List[Dict[str, Any]] = []
    variations_payload_by_parent: Dict[int, List[Dict[str, Any]]] = {}

    for row in rows:
        sku = str(row.get("sku", "")).strip()
        if not sku: continue
        qty = int(row.get("quantity", 0)) if do_stock_bool else None
        rp = row.get("regular_price") if do_prices_bool else None
        sp = row.get("sale_price") if do_prices_bool else None

        try:
            res = resolve_sku(sku)
            if not res:
                not_found.append(sku)
                log_rows.append({"sku": sku, "action": "not_found", "message": "SKU לא נמצא", "kind": "", "parent_id": "", "object_id": "", "quantity": qty, "regular_price": rp, "sale_price": sp})
                continue
            kind, obj_id, parent_id = res
            if dry_run_bool:
                if kind == "product":
                    payload = build_update_item_for_product(obj_id, qty, rp, sp)
                    log_rows.append({"sku": sku, "action": "would_update", "message": json.dumps(payload, ensure_ascii=False), "kind": "product", "parent_id": "", "object_id": obj_id, "quantity": qty, "regular_price": rp, "sale_price": sp})
                else:
                    payload = build_update_item_for_variation(obj_id, qty, rp, sp)
                    log_rows.append({"sku": sku, "action": "would_update", "message": json.dumps(payload, ensure_ascii=False), "kind": "variation", "parent_id": parent_id, "object_id": obj_id, "quantity": qty, "regular_price": rp, "sale_price": sp})
            else:
                if kind == "product":
                    product_payload.append(build_update_item_for_product(obj_id, qty, rp, sp))
                else:
                    variations_payload_by_parent.setdefault(parent_id, []).append(build_update_item_for_variation(obj_id, qty, rp, sp))
        except Exception as e:
            errors.append(f"{sku}: {e}")
            log_rows.append({"sku": sku, "action": "error", "message": str(e), "kind": "", "parent_id": "", "object_id": "", "quantity": qty, "regular_price": rp, "sale_price": sp})

    if not dry_run_bool:
        for i in range(0, len(product_payload), 100):
            res = batch_update_products(product_payload[i:i+100])
            updated_total += res["updated"]
            if res["errors"]: errors.extend(res["errors"])
            time.sleep(0.1)
        for parent_id, vlist in variations_payload_by_parent.items():
            for i in range(0, len(vlist), 100):
                res = batch_update_variations(parent_id, vlist[i:i+100])
                updated_total += res["updated"]
                if res["errors"]: errors.extend(res["errors"])
                time.sleep(0.1)

        for p in product_payload:
            log_rows.append({"sku": "", "action": "updated", "message": "מוצר עודכן", "kind": "product", "parent_id": "", "object_id": p.get("id",""), "quantity": p.get("stock_quantity",""), "regular_price": p.get("regular_price",""), "sale_price": p.get("sale_price","")})
        for parent_id, vlist in variations_payload_by_parent.items():
            for v in vlist:
                log_rows.append({"sku": "", "action": "updated", "message": "וריאציה עודכנה", "kind": "variation", "parent_id": parent_id, "object_id": v.get("id",""), "quantity": v.get("stock_quantity",""), "regular_price": v.get("regular_price",""), "sale_price": v.get("sale_price","")})

    # Write log CSV
    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    name = f"log-{ts}-{uuid.uuid4().hex[:8]}.csv"
    path = os.path.join(LOG_DIR, name)
    import csv as _csv
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = _csv.DictWriter(f, fieldnames=["sku","action","message","kind","parent_id","object_id","quantity","regular_price","sale_price"])
        w.writeheader()
        for r in log_rows:
            w.writerow(r)

    summary = {"updated_total": updated_total, "not_found": not_found[:50], "not_found_count": len(not_found), "errors": errors[:10], "errors_count": len(errors), "log_name": name, "dry_run": dry_run_bool}
    return templates.TemplateResponse("result.html", {"request": request, "summary": summary})

@app.get("/download-log", response_class=FileResponse)
def download_log(name: str = Query(...), _: bool = Depends(require_auth)):
    path = os.path.join(LOG_DIR, name)
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="Log not found")
    return FileResponse(path, filename=name, media_type="text/csv")
