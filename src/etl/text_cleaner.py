"""
Script 1 de 2 — Limpieza de Texto para BERTopic / Clustering
==============================================================
Lee   : data/raw/jobs_rows.csv
Escribe: data/final/maestro_limpio.csv

QUÉ HACE ESTE SCRIPT
----------------------
Limpia el texto de las descripciones de empleo eliminando "ruido" técnico
(markdown, HTML, escapes, URLs) pero conservando el texto NATURAL completo,
que es exactamente lo que necesitan los transformers de BERTopic y los
algoritmos de clustering (K-Means, HDBSCAN).

El texto resultante en `descripcion_limpia` es la fuente de verdad de la
que parte el script 2 (02_nlp_preprocessor.py) para hacer el preprocesamiento
lingüístico adicional (stopwords, lematización).

COLUMNAS QUE GENERA
---------------------
  descripcion_raw  : descripción original sin tocar (auditoría)
  descripcion_limpia : texto limpio para BERTopic y clustering
  titulo_desc      : título + descripcion_limpia concatenados (texto enriquecido)
  n_palabras       : longitud de descripcion_limpia en palabras
  es_vacio         : True si no hay descripción utilizable
  idioma_det       : 'es', 'en', 'otro' — detectado con langdetect
  city             : ciudad extraída de raw_location
  state_code       : clave de estado (GUA, DIF, JAL…)
  region           : macro-región legible (Bajío, CDMX, Guadalajara, etc.)
  tiene_salario    : True si salary_min o salary_max no son nulos

POR QUÉ NO APLICAR STOPWORDS AQUÍ
------------------------------------
BERTopic usa sentence-transformers (all-MiniLM-L6-v2, paraphrase-multilingual,
etc.). Estos modelos fueron entrenados con oraciones COMPLETAS. Quitarles
stopwords degrada el embedding porque el modelo pierde contexto sintáctico.

  ✅ BERTopic  → usa `descripcion_limpia`  (texto completo y natural)
  ✅ K-Means   → usa `descripcion_limpia`  (embeddings de texto completo)
  🚫 Word cloud → NO usar aquí; usa `descripcion_nlp` del script 2
  🚫 TF-IDF    → NO usar aquí; usa `descripcion_nlp` del script 2

INSTALACIONES NECESARIAS (una sola vez)
-----------------------------------------
  pip install langdetect pandas

Uso
---
  python src/nlp/01_text_cleaner.py
  python src/nlp/01_text_cleaner.py --dry-run       # sin guardar
  python src/nlp/01_text_cleaner.py --sample 300    # probar con muestra
  python src/nlp/01_text_cleaner.py --input data/raw/jobs_rows.csv
"""

import re
import sys
import logging
import argparse
from pathlib import Path

import pandas as pd
from langdetect import detect, LangDetectException

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Rutas ─────────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent.parent.parent
RAW_DIR     = BASE_DIR / "data" / "raw"
FINAL_DIR   = BASE_DIR / "data" / "final"
INPUT_CSV   = RAW_DIR  / "jobs_rows.csv"
OUTPUT_CSV  = FINAL_DIR / "maestro_limpio.csv"

# ── Umbral mínimo de palabras para considerar descripción utilizable ──────────
MIN_PALABRAS = 15


# ═══════════════════════════════════════════════════════════════════════════════
# 1. MAPEO DE REGIONES
# ═══════════════════════════════════════════════════════════════════════════════
# Claves de estado (código de 2-3 letras en raw_location) → región macro.
# Permite agrupar los puestos geográficamente para visualizaciones y clustering.

REGION_MAP = {
    "GUA": "Bajío",           # Guanajuato: León, Irapuato, Celaya, Salamanca…
    "QUE": "Querétaro",
    "DIF": "CDMX",            # Ciudad de México (alcaldías)
    "MEX": "Estado de México",
    "JAL": "Guadalajara",
    "NLE": "Monterrey",
    "AGU": "Aguascalientes",
    "SLP": "San Luis Potosí",
    "ZAC": "Zacatecas",
    "HID": "Hidalgo",
    "PUE": "Puebla",
    "MOR": "Morelos",
    "BCN": "Baja California",
    "SON": "Sonora",
    "CHH": "Chihuahua",
    "COA": "Coahuila",
    "TAM": "Tamaulipas",
    "VER": "Veracruz",
    "YUC": "Yucatán",
    "ROO": "Quintana Roo",
    "TAB": "Tabasco",
    "MIC": "Michoacán",
    "GRO": "Guerrero",
    "OAX": "Oaxaca",
    "SIN": "Sinaloa",
    "COL": "Colima",
    "CAM": "Campeche",
    "NAY": "Nayarit",
    "DUR": "Durango",
    "TLA": "Tlaxcala",
    "CHP": "Chiapas",
}

# Ciudades clave dentro del Bajío para micro-segmentación
BAJIO_CITIES = {
    "irapuato", "léon", "leon", "silao", "celaya", "salamanca",
    "guanajuato", "villagrán", "villagran", "san francisco del rincón",
    "san francisco del rincon", "pénjamo", "penjamo", "acámbaro", "acambaro",
    "valle de santiago", "juventino rosas", "dolores hidalgo", "san miguel de allende",
}


# ═══════════════════════════════════════════════════════════════════════════════
# 2. PARSEO DE UBICACIÓN
# ═══════════════════════════════════════════════════════════════════════════════

def parsear_ubicacion(raw: str) -> dict:
    """
    Extrae city, state_code y region de raw_location.

    Formatos observados en el dataset:
      'León, GUA, MX'
      'Bosques del Pedregal, GUA, MX'
      'Desde casa'                          → remoto
      'New York, NY, US'                    → internacional
      NaN                                   → sin dato

    Retorna dict con keys: city, state_code, region
    """
    result = {"city": None, "state_code": None, "region": "Sin dato"}

    if not isinstance(raw, str) or raw.strip() == "":
        return result

    raw = raw.strip()

    # Detectar remoto
    if re.search(r"\bdesde casa\b|\bremoto\b|\bremote\b", raw, re.IGNORECASE):
        result["city"] = "Remoto"
        result["region"] = "Remoto"
        return result

    # Detectar patrón "Ciudad, ESTADO, PAÍS"
    m = re.match(r"^(.+?),\s*([A-Z]{2,3}),\s*([A-Z]{2})$", raw)
    if m:
        city      = m.group(1).strip()
        state_code = m.group(2)
        country   = m.group(3)

        result["city"] = city
        result["state_code"] = state_code

        if country != "MX":
            result["region"] = "Internacional"
        elif state_code in REGION_MAP:
            region = REGION_MAP[state_code]
            # Sub-clasificar ciudades del Bajío si es relevante
            if region == "Bajío":
                city_lower = city.lower()
                for bajio_city in BAJIO_CITIES:
                    if bajio_city in city_lower:
                        result["region"] = f"Bajío — {city}"
                        return result
            result["region"] = region
        else:
            result["region"] = state_code  # desconocido, usar código

        return result

    # Sin patrón claro: guardar raw como city
    result["city"] = raw
    result["region"] = "Sin dato"
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 3. LIMPIEZA DE TEXTO
# ═══════════════════════════════════════════════════════════════════════════════

# Patrones de ruido específicos de las descripciones de Indeed/LinkedIn
_RE_MARKDOWN_BOLD  = re.compile(r"\*\*(.+?)\*\*")           # **texto**
_RE_MARKDOWN_ITAL  = re.compile(r"\*(.+?)\*")               # *texto*
_RE_URL            = re.compile(r"https?://\S+|www\.\S+")
_RE_EMAIL          = re.compile(r"\S+@\S+\.\S+")
_RE_ESCAPED_CHARS  = re.compile(r"\\([ntr\-\.\*\[\]\(\)\\])")  # \n \- \. \* etc.
_RE_BULLETS        = re.compile(r"^[\s]*[\*\-•·]\s*", re.MULTILINE)
_RE_HASHTAG        = re.compile(r"#\w+")
_RE_MULTI_SPACE    = re.compile(r"[ \t]{2,}")
_RE_MULTI_NEWLINE  = re.compile(r"\n{3,}")
_RE_NUMBERS_MONEY  = re.compile(r"\$[\d,\.]+|[\d,\.]+\s*(MXN|USD|pesos?|dólares?)", re.IGNORECASE)
_RE_PHONE          = re.compile(r"\b\d{10,}\b|\(\d{2,3}\)\s*\d{3,4}[\-\s]\d{4}")


def limpiar_descripcion(texto: str) -> str:
    """
    Limpia el texto de una descripción de empleo para uso con BERTopic
    y modelos de embeddings.

    Orden de operaciones:
    1. Decodificar escapes literales (\\n → newline, \\- → -)
    2. Eliminar URLs, emails, teléfonos
    3. Eliminar markdown (negrita, cursiva, bullets)
    4. Normalizar espacios y saltos de línea
    5. Strip final

    NO se eliminan stopwords ni se lematiza — eso es trabajo del script 2.
    El texto resultante debe ser legible por un humano y parseable por
    un modelo de lenguaje.
    """
    if not isinstance(texto, str) or texto.strip() == "":
        return ""

    # 1. Decodificar escapes literales que vienen del scraper
    #    "\\n" (dos caracteres) → "\n" real, "\\-" → "-", etc.
    t = texto.replace("\\n", "\n").replace("\\t", " ").replace("\\r", "")
    t = re.sub(r"\\([.\-\*\[\]\(\)\\])", r"\1", t)

    # 2. Eliminar URLs, emails, teléfonos (no aportan semántica al tópico)
    t = _RE_URL.sub(" ", t)
    t = _RE_EMAIL.sub(" ", t)
    t = _RE_PHONE.sub(" ", t)

    # 3. Eliminar markdown conservando el texto interno
    t = _RE_MARKDOWN_BOLD.sub(r"\1", t)   # **texto** → texto
    t = _RE_MARKDOWN_ITAL.sub(r"\1", t)   # *texto* → texto
    t = _RE_BULLETS.sub("", t)            # eliminar viñetas
    t = _RE_HASHTAG.sub(" ", t)

    # 4. Eliminar cantidades monetarias y números de teléfono largos
    #    Conservamos números si están en contexto ("2 años de experiencia")
    t = _RE_NUMBERS_MONEY.sub(" ", t)

    # 5. Normalizar espacios y newlines
    t = _RE_MULTI_SPACE.sub(" ", t)
    t = _RE_MULTI_NEWLINE.sub("\n\n", t)

    return t.strip()


# ═══════════════════════════════════════════════════════════════════════════════
# 4. DETECCIÓN DE IDIOMA
# ═══════════════════════════════════════════════════════════════════════════════

def detectar_idioma(texto: str) -> str:
    """
    Detecta el idioma dominante del texto con langdetect.

    langdetect usa modelos estadísticos de n-gramas de caracteres (port de
    Google's language-detection). Funciona bien con textos cortos y mezclas
    de términos técnicos en inglés dentro de texto en español.

    Retorna: 'es', 'en', u 'otro'

    Casos especiales:
    - Textos < 20 caracteres → 'otro' (demasiado corto para ser confiable)
    - Excepciones de langdetect (texto muy ambiguo) → 'otro'
    """
    if not isinstance(texto, str) or len(texto.strip()) < 20:
        return "otro"
    try:
        lang = detect(texto)
        return lang if lang in ("es", "en") else "otro"
    except LangDetectException:
        return "otro"


# ═══════════════════════════════════════════════════════════════════════════════
# 5. PIPELINE PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════════

def procesar(df: pd.DataFrame) -> pd.DataFrame:
    total = len(df)
    log.info(f"  Total registros cargados: {total:,}")

    # ── 5.1 Parsear ubicación ─────────────────────────────────────────────────
    log.info("\n  [1/5] Parseando ubicaciones...")
    loc_parsed = df["raw_location"].apply(parsear_ubicacion)
    df["city"]       = loc_parsed.apply(lambda x: x["city"])
    df["state_code"] = loc_parsed.apply(lambda x: x["state_code"])
    df["region"]     = loc_parsed.apply(lambda x: x["region"])

    region_dist = df["region"].value_counts().head(10)
    log.info("  Top 10 regiones:")
    for region, cnt in region_dist.items():
        log.info(f"    {region:<30} {cnt:>5,}")

    # ── 5.2 Flag de salario ───────────────────────────────────────────────────
    log.info("\n  [2/5] Calculando flags de salario...")
    df["tiene_salario"] = df["salary_min"].notna() | df["salary_max"].notna()
    con_salario = df["tiene_salario"].sum()
    log.info(f"  Registros con salario: {con_salario:,} / {total:,} ({con_salario/total*100:.1f}%)")

    # ── 5.3 Limpiar descripción ───────────────────────────────────────────────
    log.info("\n  [3/5] Limpiando descripciones...")
    # Guardar original
    df["descripcion_raw"] = df["description"]

    # LinkedIn jobs no tienen description en el CSV — marcamos como vacíos
    df["descripcion_limpia"] = df["description"].apply(limpiar_descripcion)

    # Calcular longitud
    df["n_palabras"] = df["descripcion_limpia"].str.split().str.len().fillna(0).astype(int)

    # Marcar vacíos/demasiado cortos
    df["es_vacio"] = (df["descripcion_limpia"].str.strip() == "") | (df["n_palabras"] < MIN_PALABRAS)

    vacios = df["es_vacio"].sum()
    log.info(f"  Descripciones vacías o muy cortas (< {MIN_PALABRAS} palabras): {vacios:,} ({vacios/total*100:.1f}%)")
    log.info(f"  Descripciones utilizables: {total - vacios:,}")

    # ── 5.4 Texto enriquecido: título + descripción ────────────────────────────
    log.info("\n  [4/5] Construyendo campo titulo_desc (título + descripción)...")
    # Combinar título + descripción para embeddings más ricos.
    # El título contiene señales comprimidas del puesto que la descripción
    # a veces diluye con texto genérico de empresa.
    df["titulo_desc"] = df.apply(
        lambda row: (
            f"{row['title']}. {row['descripcion_limpia']}"
            if not row["es_vacio"]
            else row["title"]
        ),
        axis=1,
    )

    # ── 5.5 Detección de idioma ───────────────────────────────────────────────
    log.info("\n  [5/5] Detectando idioma...")
    mask_util = ~df["es_vacio"]
    df["idioma_det"] = "otro"
    df.loc[mask_util, "idioma_det"] = (
        df.loc[mask_util, "descripcion_limpia"].apply(detectar_idioma)
    )

    utiles = mask_util.sum()
    dist_lang = df.loc[mask_util, "idioma_det"].value_counts()
    for lang, cnt in dist_lang.items():
        log.info(f"    {lang:<6} {cnt:>5,}  ({cnt/utiles*100:.1f}%)")

    return df


def imprimir_resumen(df: pd.DataFrame) -> None:
    utiles = df[~df["es_vacio"]]
    log.info("\n" + "═" * 60)
    log.info("  RESUMEN — maestro_limpio.csv")
    log.info("═" * 60)
    log.info(f"  Total registros          : {len(df):>7,}")
    log.info(f"  Con descripción útil     : {len(utiles):>7,}  ({len(utiles)/len(df)*100:.1f}%)")
    log.info(f"  Sin descripción (vacíos) : {df['es_vacio'].sum():>7,}")
    log.info(f"  Con salario reportado    : {df['tiene_salario'].sum():>7,}")

    log.info("\n  Palabras por carrera (mediana, descripciones útiles):")
    stats = (
        utiles.groupby("career")["n_palabras"]
              .median()
              .sort_values(ascending=False)
              .head(10)
    )
    for career, med in stats.items():
        log.info(f"    {career:<40} {med:>5.0f} palabras")

    log.info("\n  Columnas en maestro_limpio.csv listas para:")
    log.info("    `descripcion_limpia`  → BERTopic, K-Means (embeddings)")
    log.info("    `titulo_desc`         → BERTopic con contexto enriquecido")
    log.info("    `region`              → visualización por procedencia")
    log.info("    `career`              → etiqueta de carrera (ground truth)")
    log.info("    `industry_niche`      → etiqueta de nicho")
    log.info("    `idioma_det`          → filtrar por idioma")
    log.info("    `tiene_salario`       → análisis salarial")
    log.info("\n  → Para BoW, TF-IDF, n-gramas y word cloud:")
    log.info("    ejecuta python src/nlp/02_nlp_preprocessor.py")


def main(dry_run: bool = False, sample: int = None, input_path: Path = None) -> None:
    log.info("═" * 60)
    log.info("  SCRIPT 1/2 — LIMPIEZA DE TEXTO (BERTopic / Clustering)")
    log.info("═" * 60)

    csv_path = Path(input_path) if input_path else INPUT_CSV
    if not csv_path.exists():
        log.error(f"No encontrado: {csv_path}")
        sys.exit(1)

    log.info(f"Leyendo {csv_path.name}...")
    df = pd.read_csv(csv_path, dtype=str, low_memory=False)

    # Convertir tipos
    for col in ["salary_min", "salary_max"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["is_premium"] = df["is_premium"].map({"True": True, "False": False}).fillna(False)
    df["n_palabras"] = 0  # se sobreescribe en procesar()

    if sample:
        df = df.sample(sample, random_state=42).reset_index(drop=True)
        log.info(f"  [--sample] Usando muestra de {sample} registros")

    df = procesar(df)
    imprimir_resumen(df)

    # Seleccionar y ordenar columnas de salida
    cols_meta = [
        "id", "site", "job_url",
        "title", "company",
        "career", "industry_niche",
        "job_type", "is_premium",
        "salary_min", "salary_max", "currency",
        "tiene_salario",
        "raw_location", "city", "state_code", "region",
        "date_posted", "date_scraped",
        "idioma_det",
        "es_vacio",
        "n_palabras",
        "descripcion_raw",
        "descripcion_limpia",
        "titulo_desc",
        "status",
    ]
    df_out = df[[c for c in cols_meta if c in df.columns]]

    if dry_run:
        log.info("\n  [--dry-run] No se guardó ningún archivo.")
        log.info("  Vista previa (3 filas):")
        log.info(df_out.head(3).to_string())
        return

    FINAL_DIR.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    log.info(f"\n  ✅ Guardado: {OUTPUT_CSV.relative_to(BASE_DIR)}")
    log.info(f"     {len(df_out):,} filas  ×  {len(df_out.columns)} columnas")
    log.info("═" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Limpieza de texto para BERTopic/Clustering")
    parser.add_argument("--dry-run", action="store_true", help="No guardar archivos")
    parser.add_argument("--sample", type=int, default=None, help="Procesar solo N filas")
    parser.add_argument("--input", type=str, default=None, help="Ruta alternativa al CSV de entrada")
    args = parser.parse_args()
    main(dry_run=args.dry_run, sample=args.sample, input_path=args.input)