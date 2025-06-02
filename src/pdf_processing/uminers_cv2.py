from __future__ import annotations

from pathlib import Path
from typing import List
import difflib
import re

import cv2
import numpy as np
import pandas as pd
import pdf2image
import pytesseract

from src.config import settings
from .pdf_parser import _clean_cell as _base_clean_cell, fallback_tables

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS & PRESETS
# ─────────────────────────────────────────────────────────────────────────────
TEMP_IMG_DIR = Path(settings.base_data_dir) / "temp_images"
TEMP_IMG_DIR.mkdir(parents=True, exist_ok=True)

AREA_PT = [245, 65, 2150, 1500]
COLS_PT = [330, 500, 700, 940, 1250]

HEADER = ["Модель", "Хэшрейт", "Цена у.е./$", "Цена ₽", "Цена $", "Примечание"]

UNIT_FIXES = {
    " h/s": " h/s",
    "Мh/s": "Mh/s",
    "Мх/s": "Mh/s",
}
AVAIL_PAT = re.compile(r"(?iu)(?:в|b)\s*на[лн]ич[ие][a-zа-я]*")
RU_MONTH_PAT = re.compile(
    r"(?iu)"  # case‑insensitive, Unicode
    r"(янв\w*|фев\w*|мар\w*|апр\w*|ма[йя]|июн\w*|июл\w*|авг\w*|"
    r"сен\w*|окт\w*|ноя\w*|дек\w*)"
)
_SLASH_ZERO_PAT = re.compile(r"(\d)/0\s*Th", re.IGNORECASE)
_NUM_ONLY_PAT = re.compile(r"[^0-9]")

# Known canonical model names – used for fuzzy normalisation
KNOWN_MODELS = [
    "S21+", "S21 XP", "T21", "S21 Pro", "L9", "L7", "S19k pro",
    "S21+Hyd", "S21i+Hyd", "S21 XP Hyd", "M60s 18,5 W", "M61 19,9 W",
    "M60s+ 17 W", "DG 1+",
]

# ---------------------------------------------------------------------------
# OCR CONFIGS (column‑aware)
# ---------------------------------------------------------------------------
_NUM_CFG = '--oem 3 --psm 7 -l eng -c tessedit_char_whitelist="0123456789,."'
_HASH_CFG = '--oem 3 --psm 7 -l eng -c tessedit_char_whitelist="0123456789TGtgMmHh/"'
_MODEL_CFG = (
    '--oem 3 --psm 7 -l eng '
    '-c tessedit_char_whitelist="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ -"'
)
_NOTE_CFG = "--oem 3 --psm 7 -l rus"


def _cfg_for(col: str) -> str:
    if col == "Модель":
        return _MODEL_CFG
    if col == "Примечание":
        return _NOTE_CFG
    if col == "Хэшрейт":
        return _HASH_CFG
    if col.startswith("Цена"):
        return _NUM_CFG
    return _NOTE_CFG  # fallback

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────


def _clean_number(txt):
    """Remove non‑numeric chars while keeping empty values as ``pd.NA``."""
    if pd.isna(txt):
        return pd.NA
    cleaned = _NUM_ONLY_PAT.sub("", str(txt))
    return cleaned or pd.NA


def _drop_garbage_rows(df: pd.DataFrame) -> pd.DataFrame:
    price_mask = (
        df["Цена у.е./$"].str.contains(r"\d", na=False)
        | df["Цена ₽"].str.contains(r"\d", na=False)
        | df["Цена $"].str.contains(r"\d", na=False)
    )
    hashrate_mask = df["Хэшрейт"].str.contains(r"\d", na=False)
    return df.loc[price_mask & hashrate_mask].reset_index(drop=True)


# -----------------------  Post‑OCR fixers  ----------------------------------


_CYR_TO_LAT = str.maketrans({
    "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M", "Н": "H",
    "О": "O", "Р": "P", "С": "C", "Т": "T", "У": "Y", "Х": "X",
    "а": "a", "с": "c", "е": "e", "о": "o", "р": "p", "у": "y",
})


def _cyr2lat(txt: str) -> str:
    """Cheap Cyrillic‑to‑Latin transliteration for look‑alike letters."""
    return txt.translate(_CYR_TO_LAT)


def _fuzzy_to_known(txt: str) -> str:
    """Return the closest known model name if the similarity is high."""
    if not txt:
        return txt
    m = difflib.get_close_matches(txt, KNOWN_MODELS, n=1, cutoff=0.75)
    return m[0] if m else txt


def _fix_model_ocr(txt: str):
    if pd.isna(txt) or not isinstance(txt, str):
        return txt
    t = _cyr2lat(txt.strip())

    # Manual one‑offs --------------------------------------------------------
    if t.upper() in {"LS", "L5", "L$"}:  # typical mis‑read of "L9"
        return "L9"
    if t.startswith("$"):  # "$21+" ➜ "S21+"
        t = "S" + t[1:]
    if re.match(r"DG\s+it", t, re.IGNORECASE):
        return "DG 1+"

    return _fuzzy_to_known(t)


def _fix_hashrate_ocr(txt):
    if pd.isna(txt) or not isinstance(txt, str):
        return txt
    return _SLASH_ZERO_PAT.sub(lambda m: f"{m.group(1)}70 Th", txt)


def _fix_hashrate_unit(txt: str) -> str:
    if pd.isna(txt) or not isinstance(txt, str):
        return txt
    m = re.search(r"\b(\d+)\s*h/s\b", txt)
    if not m:
        return txt
    unit = "Gh/s" if int(m.group(1)) < 1000 else "Mh/s"
    return re.sub(r"h/s\b", unit, txt, count=1)


def _clean_cell(v, col: str):
    """Column‑specific post‑processing."""
    t = _base_clean_cell(v)
    if pd.isna(t):
        return pd.NA
    t = str(t)

    if col == "Модель":
        return _fix_model_ocr(t)

    if col == "Примечание":
        # Normalise availability / month mentions
        t = AVAIL_PAT.sub("В наличии", t)
        t = RU_MONTH_PAT.sub(lambda m: m.group(1).capitalize(), t)
        return t.strip() or pd.NA

    # Other columns ---------------------------------------------------------
    for old, new in UNIT_FIXES.items():
        t = t.replace(old, new)
    t = _fix_hashrate_ocr(t)
    t = _fix_hashrate_unit(t)
    t = AVAIL_PAT.sub("В наличии", t)
    t = RU_MONTH_PAT.sub(lambda m: m.group(1).capitalize(), t)
    return re.sub(r"\s{2,}", " ", t).strip() or pd.NA


# ─────────────────────────────────────────────────────────────────────────────
# MAIN EXTRACTOR
# ─────────────────────────────────────────────────────────────────────────────


def extract_uminers(pdf_path: Path, *, save_cells: bool = False) -> pd.DataFrame:
    """Convert the first page of *pdf_path* into a structured DataFrame."""
    try:
        pages = pdf2image.convert_from_path(pdf_path, dpi=300)
        if not pages:
            return pd.DataFrame(columns=HEADER)
        img = cv2.cvtColor(np.array(pages[0]), cv2.COLOR_RGB2BGR)
        sf = 300 / 72  # pts → px scale factor

        top, left, bot, right = [int(v * sf) for v in AREA_PT]
        tbl = img[
            max(0, top): min(img.shape[0], bot),
            max(0, left): min(img.shape[1], right),
        ]

        gray = cv2.cvtColor(tbl, cv2.COLOR_BGR2GRAY)
        gray = cv2.medianBlur(gray, 3)
        _, bw = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        if np.mean(bw) < 127:
            bw = cv2.bitwise_not(bw)

        cnts, _ = cv2.findContours(255 - bw, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        boxes = [
            (y, y + h) for (_, y, _, h) in (cv2.boundingRect(c) for c in cnts) if h > 5
        ]
        boxes.sort()
        rows: list[list[tuple[int, int]]] = []
        cur = [boxes[0]] if boxes else []
        for y0, y1 in boxes[1:]:
            if y0 - cur[-1][1] < 12:
                cur.append((y0, y1))
            else:
                rows.append(cur)
                cur = [(y0, y1)]
        if cur:
            rows.append(cur)
        rows_px = [
            (max(0, min(r)[0] - 2), min(bw.shape[0], max(r)[1] + 2))
            for r in rows
            if max(r)[1] - min(r)[0] > 18
        ]

        cols_px = [0] + [int((c - AREA_PT[1]) * sf) for c in COLS_PT] + [bw.shape[1]]
        cols_px = sorted(set(max(0, min(x, bw.shape[1] - 1)) for x in cols_px))

        PAD, SCALE = 4, 2
        data: List[List[str]] = []
        for y0, y1 in rows_px:
            row_vals: list[str] = []
            for ci in range(len(cols_px) - 1):
                cs, ce = cols_px[ci], cols_px[ci + 1]
                cell = bw[y0:y1, cs:ce]
                if cell.size < 100:
                    row_vals.append("")
                    continue
                cell = cv2.copyMakeBorder(
                    cell, PAD, PAD, PAD, PAD, cv2.BORDER_CONSTANT, value=255
                )
                cell = cv2.resize(
                    cell, None, fx=SCALE, fy=SCALE, interpolation=cv2.INTER_LINEAR
                )
                txt = pytesseract.image_to_string(cell, config=_cfg_for(HEADER[ci]))
                row_vals.append(txt.strip().replace("\n", " "))
            if any(row_vals):
                data.append(row_vals)

        if not data:
            # fall back to PDFMiner‑based heuristic tables
            return pd.concat(fallback_tables(pdf_path), ignore_index=True)

        width = max(map(len, data))
        df = pd.DataFrame([r + [""] * (width - len(r)) for r in data]).iloc[
            :, : len(HEADER)
        ]
        df.columns = HEADER

        # numeric columns – raw clean
        for col in ("Цена у.е./$", "Цена ₽", "Цена $"):
            df[col] = df[col].map(_clean_number)
        df = _drop_garbage_rows(df)

        # column‑wise tidy‑up
        for c in df.columns:
            df[c] = df[c].map(lambda v, col=c: _clean_cell(v, col))

        return df.reset_index(drop=True)

    except Exception as exc:
        print("⛔  Uminers‑OCR error:", exc)
        return pd.DataFrame(columns=HEADER)
