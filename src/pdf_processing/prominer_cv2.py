from pathlib import Path

import pandas as pd
import cv2

import re
import numpy as np
import pdf2image
import pytesseract

from src.config import settings


# Configuration
TEMP_IMG_DIR = Path(settings.base_data_dir) / "temp_images"
TEMP_IMG_DIR.mkdir(parents=True, exist_ok=True)

# Promminer:
# top,left,bottom,right
PROM_TABULA_AREA = [180, 19, 637, 368]
# 4 dividers → 5 columns
PROM_TABULA_COLUMNS = [123, 183, 251, 320]
PROM_HEADER = [
    "Модель",
    "Хэшрейт",
    "Потребление",
    "Срок поставки",
    "Цена от",
]

UNIT_FIXES = {
    # NOTE: Leave just with 'h', gonna be replaced with 'Gh'/'Mh' later
    " h/s": " h/s",
    "Мh/s": "Mh/s",
    "Мх/s": "Mh/s",
}


# «В наличии»
AVAIL_PAT = re.compile(r"(?i)B\s*H[an]+u[yu]+n")

# «дней»
DAY_PLUR = re.compile(r"\b(\d+\s*-\s*\d+|\d+)\s+[A-Za-zА-Яа-я]{2,4}\b", re.I)


def _fix_hashrate_unit(txt: str) -> str:
    """
    If the number is followed by 'h/s' without a prefix ⇒ replace with
    Gh/s  (<1000)   or   Mh/s (≥1000 & <10000)
    """
    m = re.search(r"\b(\d+)\s*h/s\b", txt)
    if not m:
        return txt
    val = int(m.group(1))
    unit = "Gh/s" if val < 1000 else "Mh/s"
    return re.sub(r"h/s\b", unit, txt, count=1)


def _clean_cell(v: str) -> str:
    if not isinstance(v, str):
        return v

    # Remove leading vertical bars
    t = re.sub(r"^[\|Il]+\s*", "", v.strip())

    for wrong, right in UNIT_FIXES.items():
        t = t.replace(wrong, right)

    t = AVAIL_PAT.sub("В наличии", t)

    t = DAY_PLUR.sub(r"\1 дней", t)

    # Space between number and unit (9050Mh/s → 9050 Mh/s)
    t = re.sub(r"(\d)([A-Za-zА-Яа-я])", r"\1 \2", t)

    # Prices «3700 s / s1» → «3700 $»
    t = re.sub(r"\b(\d+)\s*s1?\b", r"\1 $", t)

    # Append Gh/s|Mh/s if letters were lost
    t = _fix_hashrate_unit(t)

    return re.sub(r"\s{2,}", " ", t)


def extract_promminer(pdf_path: Path, save_cells: bool = False) -> pd.DataFrame:
    """
    Extracts Promminer table from PDF using OpenCV.
    :param pdf_path: Path to the PDF file.
    :param save_cells: If True, saves each cell as a separate image for debugging.
    :return: DataFrame with the extracted table.
    """
    try:
        # PDF → image
        imgs = pdf2image.convert_from_path(pdf_path, dpi=300)
        if not imgs:
            return pd.DataFrame(columns=PROM_HEADER)
        img = np.array(imgs[0])
        cv2.imwrite(str(TEMP_IMG_DIR / f"{pdf_path.stem}_page.png"), img)

        # Table area
        sf = 300 / 72
        top, left, bottom, right = [int(v * sf) for v in PROM_TABULA_AREA]
        h, w = img.shape[:2]
        top, left = max(0, top), max(0, left)
        bottom, right = min(h, bottom), min(w, right)
        table = img[top:bottom, left:right]
        cv2.imwrite(str(TEMP_IMG_DIR / f"{pdf_path.stem}_table_raw.png"), table)

        gray = cv2.cvtColor(table, cv2.COLOR_BGR2GRAY)

        # Contrast
        clahe = cv2.createCLAHE(2.0, (8, 8))
        gray_en = clahe.apply(gray)
        cv2.imwrite(str(TEMP_IMG_DIR / f"{pdf_path.stem}_gray_en.png"), gray_en)

        # Binarization
        _, bw = cv2.threshold(gray_en, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        if np.mean(bw) < 127:
            bw = cv2.bitwise_not(bw)
        cv2.imwrite(str(TEMP_IMG_DIR / f"{pdf_path.stem}_bw.png"), bw)

        # Clasterization
        cnts, _ = cv2.findContours(255 - bw, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        boxes = [
            (y, y + h) for (_, y, _, h) in (cv2.boundingRect(c) for c in cnts) if h > 5
        ]
        boxes = sorted(boxes)
        clusters, cur = [], [boxes[0]] if boxes else []
        for y0, y1 in boxes[1:]:
            if y0 - cur[-1][1] < 10:
                cur.append((y0, y1))
            else:
                clusters.append(cur)
                cur = [(y0, y1)]
        if cur:
            clusters.append(cur)

        rows = [
            (max(0, min(c)[0] - 2), min(bw.shape[0], max(c)[1] + 2)) for c in clusters
        ]
        # fallback
        if len(rows) < 5:
            step = bw.shape[0] // 20
            rows = [(i * step, (i + 1) * step) for i in range(20)]

        # Draw lines for debugging
        dbg = cv2.cvtColor(bw, cv2.COLOR_GRAY2BGR)
        for y0, y1 in rows:
            cv2.line(dbg, (0, y0), (bw.shape[1], y0), (0, 255, 0), 1)
            cv2.line(dbg, (0, y1), (bw.shape[1], y1), (0, 255, 0), 1)

        # Columns
        cols = (
            [0]
            + [int((c - PROM_TABULA_AREA[1]) * sf) for c in PROM_TABULA_COLUMNS]
            + [bw.shape[1]]
        )
        cols = sorted(set(max(0, min(c, bw.shape[1] - 1)) for c in cols))
        for x in cols:
            cv2.line(dbg, (x, 0), (x, bw.shape[0]), (255, 0, 0), 1)
        cv2.imwrite(str(TEMP_IMG_DIR / f"{pdf_path.stem}_grid.png"), dbg)

        # OCR
        PAD, EXTRA, SCALE = 4, 2, 2
        data = []
        for ridx, (y0, y1) in enumerate(rows):
            row_img = bw[y0:y1, :]
            row = []
            for ci in range(len(cols) - 1):
                cs = max(0, cols[ci] - EXTRA)
                ce = min(cols[ci + 1] + EXTRA, bw.shape[1])
                cell = row_img[:, cs:ce]
                if cell.size < 80:
                    row.append("")
                    continue

                # Padding + resizing
                cell = cv2.copyMakeBorder(
                    cell, PAD, PAD, PAD, PAD, cv2.BORDER_CONSTANT, value=255
                )
                cell = cv2.resize(
                    cell, None, fx=SCALE, fy=SCALE, interpolation=cv2.INTER_LINEAR
                )
                cell = cv2.GaussianBlur(cell, (3, 3), 0)

                # Debugging
                if save_cells and ridx < 25:
                    cv2.imwrite(
                        str(TEMP_IMG_DIR / f"{pdf_path.stem}_r{ridx:02d}_c{ci}.png"),
                        cell,
                    )

                cfg_num = (
                    "--oem 3 --psm 7 -l rus+eng "
                    '-c tessedit_char_whitelist="0123456789$Th/sWwГгМмдней."'
                )
                cfg_txt = "--oem 3 --psm 6 -l rus+eng"
                cfg = cfg_num if (ce - cs) < 260 else cfg_txt
                txt = pytesseract.image_to_string(cell, config=cfg)
                row.append(txt.strip().replace("\n", " "))
            if any(row):
                data.append(row)

        if not data:
            return pd.DataFrame(columns=PROM_HEADER)

        # DataFrame + cleaning
        width = max(len(r) for r in data)
        data = [r + [""] * (width - len(r)) for r in data]
        df = pd.DataFrame(data).iloc[:, : len(PROM_HEADER)]
        df.columns = PROM_HEADER

        df = (
            df.query("Модель.str.len()>3", engine="python")
            .map(_clean_cell)
            .reset_index(drop=True)
        )
        return df

    except Exception as e:
        print("   ⛔ Ошибка:", e)
        return pd.DataFrame(columns=PROM_HEADER)

    finally:
        # Cleanup temp images
        for img in TEMP_IMG_DIR.glob("*.png"):
            img.unlink()
