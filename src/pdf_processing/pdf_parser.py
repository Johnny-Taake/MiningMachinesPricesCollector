from pathlib import Path
from typing import List

import pandas as pd
import tabula

from src.config import settings


# Configuration
BASE_INPUT_DIR = Path(settings.pdf_collector.get_pdf_save_dir(settings.base_data_dir))

OUTPUT_DIR = Path(settings.prepared_excels_dir)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Promminer:
# top,left,bottom,right
PROM_TABULA_AREA = [150, 19, 637, 368]
# 4 dividers → 5 columns
PROM_TABULA_COLUMNS = [123, 183, 251, 320]
PROM_HEADER = [
    "Модель",
    "Хэшрейт",
    "Потребление",
    "Срок поставки",
    "Цена от",
]

# IBMM:
# top,left,bottom,right
IBMM_TABULA_AREA = [118, 28, 800, 580]
# 6 dividers → 7 columns
IBMM_TABULA_COLUMNS = [140, 240, 340, 400, 470, 530]
IBMM_HEADER = [
    "Модель",
    "Хэшрейт",
    "Потребление",
    "Цена ₽ Москва",
    "Цена $ Москва",
    "Цена ₽ Китай",
    "Цена $ Китай",
]


# 0. Get the latest directory with PDFs
def get_input_dir() -> Path:
    """
    Return newest collection_* subdirectory inside BASE_INPUT_DIR.
    If none exists – return BASE_INPUT_DIR itself.
    """
    subdirs = [
        d for d in BASE_INPUT_DIR.iterdir()
        if d.is_dir() and d.name.startswith("collection_")
    ]
    if subdirs:
        latest = max(subdirs, key=lambda p: p.name)      # имя-дата новее
        print(f"✅  Выбрана свежая подборка: {latest.relative_to(BASE_INPUT_DIR)}")
        return latest
    print(f"⚠️  Подпапки collection_* не найдены – используем {BASE_INPUT_DIR}")
    return BASE_INPUT_DIR


# 1. Universal Tabula fallback
def fallback_tables(pdf_path: Path) -> List[pd.DataFrame]:
    """stream → lattice, возвращает список непустых DataFrame'ов."""
    for mode in ("stream", "lattice"):
        frames = tabula.read_pdf(
            pdf_path,
            pages="all",
            multiple_tables=True,
            lattice=(mode == "lattice"),
            stream=(mode == "stream"),
            guess=True,
        )
        frames = [
            df.dropna(axis=1, how="all").dropna(how="all")
            for df in frames
            if not df.dropna(how="all").empty
        ]
        if frames:
            print(f"   • найдено {len(frames)} табл. ({mode})")
            return frames
    return []


# 2. IBMM Profile
def _clean_cell(x):
    """'-', '–', '—', '' →  pd.NA; иначе trimmed str / число как есть."""
    if pd.isna(x):
        return pd.NA
    s = str(x).strip()
    return pd.NA if s in {"", "-", "–", "—"} else s


def extract_ibmm(pdf_path: Path) -> pd.DataFrame:
    tables = tabula.read_pdf(
        str(pdf_path),
        pages="all",
        multiple_tables=False,
        guess=False,
        stream=True,
        area=IBMM_TABULA_AREA,
        columns=IBMM_TABULA_COLUMNS,
        pandas_options={"dtype": str},
    )

    df = pd.concat([t.dropna(axis=1, how="all") for t in tables], ignore_index=True)

    # Normalization
    df = df.applymap(_clean_cell)

    # Remove repeated headers
    df = df[df.iloc[:, 0].str.lower() != "модель"]

    # Remove empty rows with model only
    only_model = df.iloc[:, 1:].isna().all(axis=1)
    df = df[~only_model]

    # Remove "broken" names (filled in the first and second columns, the rest NaN)
    split_name = df.iloc[:, 1].notna() & df.iloc[:, 2:].isna().all(axis=1)
    df = df[~split_name]

    # Width check + single header
    if df.shape[1] != len(IBMM_HEADER):
        raise ValueError(f"IBMM: ожидалось 7 колонок, получили {df.shape[1]}")
    df.columns = IBMM_HEADER

    return df.reset_index(drop=True)


# 3. Promminer Profile
def extract_promminer(pdf_path: Path) -> pd.DataFrame:
    """Чётко режем Promminer по area+columns (Tabula-py) и ставим кастомный header."""
    tables = tabula.read_pdf(
        str(pdf_path),
        pages="all",
        multiple_tables=False,
        guess=False,
        stream=True,
        area=PROM_TABULA_AREA,
        columns=PROM_TABULA_COLUMNS,
        pandas_options={"dtype": str},
    )

    df = tables[0].dropna(axis=1, how="all")

    # Set custom header
    df.columns = PROM_HEADER

    # If the first row is a header, remove it
    if df.iloc[0].str.contains(r"\d").sum() == 0:
        df = df[1:]

    return df.reset_index(drop=True)


# 4. Routing function
def route_extract(pdf_path: Path) -> pd.DataFrame:
    name = pdf_path.stem.lower()
    if name.startswith(("ibmm", "ibmm_", "ibm")):
        print("   → профиль IBMM")
        return extract_ibmm(pdf_path)
    if name.startswith(("promminer", "promminer_")):
        print("   → профиль Promminer")
        return extract_promminer(pdf_path)
    print("   → профиль DEFAULT (Tabula)")
    frames = fallback_tables(pdf_path)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def main():
    input_dir = get_input_dir()
    
    for pdf in input_dir.glob("*.pdf"):
        print(f"▶ {pdf.name}")
        df = route_extract(pdf)

        if df.empty:
            print("  ⛔ Не удалось извлечь таблицу")
            continue

        out = OUTPUT_DIR / f"{pdf.stem}.xlsx"
        df.to_excel(out, index=False)
        print("  ✔ Сохранено:", out)

    print("\nГотово! Файлы лежат в", OUTPUT_DIR.resolve())
