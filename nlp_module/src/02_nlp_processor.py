"""
Script 2 de 2 — Preprocesamiento NLP para BoW / TF-IDF / N-Gramas / LDA
=========================================================================
Lee   : data/final/maestro_limpio.csv   (generado por 01_text_cleaner.py)
Escribe: data/final/maestro_nlp.csv

QUÉ HACE ESTE SCRIPT Y POR QUÉ
---------------------------------
El script 1 generó `descripcion_limpia`: texto limpio pero completo,
ideal para transformers (BERTopic, embeddings, K-Means).

Este script hace preprocesamiento LINGÜÍSTICO para las técnicas clásicas
de NLP que trabajan con frecuencias de tokens:

  Técnica               | Campo a usar         | Por qué
  ----------------------|----------------------|-----------------------------
  BERTopic              | descripcion_limpia   | Transformers = texto natural
  K-Means + coseno      | descripcion_limpia   | Embeddings = texto completo
  Word cloud            | descripcion_nlp      | Sin stopwords, lemas reales
  Bag of Words (BoW)    | descripcion_nlp      | Sin stopwords, sin ruido
  TF-IDF                | descripcion_nlp      | Sin stopwords, conteos puros
  N-gramas              | descripcion_nlp      | Colocaciones significativas
  LDA                   | descripcion_nlp      | Distribución sobre términos
  Nube de palabras      | descripcion_nlp      | Legibilidad = lemas, no stems

COLUMNAS QUE AGREGA
---------------------
  descripcion_nlp     : descripcion_limpia sin stopwords + lematizada
                        → lista para BoW, TF-IDF, n-gramas, LDA, wordcloud
  titulo_nlp          : título del puesto preprocesado igual
  texto_nlp           : titulo_nlp + descripcion_nlp (campo combinado)
  n_palabras_nlp      : longitud de descripcion_nlp tras stopwords
  es_vacio_nlp        : True si quedó vacío/muy corto tras stopwords
                        (sucede con puestos cuyos textos eran solo frases
                         genéricas como "tiempo completo, sueldo competitivo")

STEMMING vs LEMATIZACIÓN
--------------------------
  Stemming     : recorte mecánico del sufijo.
                 Rápido. Produce raíces a veces ilegibles.
                 "playas" → "play", "automatización" → "automat"
                 ❌ Malo para word clouds (palabras no reales)

  Lematización : mapea a la forma canónica del diccionario.
                 "playas" → "playa", "automatización" → "automatización"
                 ✅ Produce palabras reales, legibles para humanos
                 Usamos simplemma (sin descargas, tablas incluidas)

POR QUÉ SIMPLEMMA Y NO SPACY
-------------------------------
  spaCy (es_core_news_lg): ~600 MB de modelo, requiere descarga,
  mucho más lento (~5–10 min para 2400 docs).

  simplemma: ~2 MB de tablas por idioma, sin descargas, ~40 seg para
  2400 docs. Precisión suficiente para visualizaciones y clustering.
  Para producción o resultados de investigación, considera spaCy.

INSTALACIONES NECESARIAS (una sola vez)
-----------------------------------------
  pip install langdetect simplemma pandas

Uso
---
  python src/nlp/02_nlp_preprocessor.py
  python src/nlp/02_nlp_preprocessor.py --dry-run
  python src/nlp/02_nlp_preprocessor.py --sample 300
"""

import re
import sys
import logging
import argparse
from pathlib import Path

import pandas as pd
import simplemma

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Rutas ─────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent.parent.parent
FINAL_DIR  = BASE_DIR / "data" / "final"
INPUT_CSV  = FINAL_DIR / "maestro_limpio.csv"
OUTPUT_CSV = FINAL_DIR / "maestro_nlp.csv"

# ── Umbral mínimo de tokens post-stopwords ────────────────────────────────────
MIN_TOKENS_NLP = 5


# ═══════════════════════════════════════════════════════════════════════════════
# 1. STOPWORDS
# ═══════════════════════════════════════════════════════════════════════════════
# Conjunto amplio ES + EN + términos genéricos de ofertas de empleo.
#
# Por qué lista manual y no nltk:
#   La lista de nltk es buena pero no cubre terminología laboral
#   genérica como "tiempo completo", "sueldo", "empresa", "funciones".
#   Estos términos aparecen en el 90 %+ de las ofertas y contaminan
#   las visualizaciones (TF-IDF los pondera alto pero no dicen nada del puesto).
#
# Para reemplazar con nltk si lo tienes disponible:
#   from nltk.corpus import stopwords
#   SW_ES = set(stopwords.words("spanish")) | STOPWORDS_EMPLEO_ES
#   SW_EN = set(stopwords.words("english")) | STOPWORDS_EMPLEO_EN

STOPWORDS_ES = {
    # Artículos
    "el", "la", "los", "las", "un", "una", "unos", "unas",
    # Preposiciones
    "a", "ante", "bajo", "con", "contra", "de", "desde", "durante",
    "en", "entre", "hacia", "hasta", "mediante", "para", "por", "según",
    "sin", "sobre", "tras", "versus",
    # Conjunciones
    "y", "e", "ni", "o", "u", "pero", "sino", "aunque", "porque",
    "pues", "que", "si", "como", "cuando", "donde", "mientras",
    # Pronombres
    "yo", "tu", "tú", "él", "ella", "nosotros", "vosotros", "ellos",
    "ellas", "me", "te", "se", "nos", "os", "le", "les", "lo", "la",
    "mi", "mis", "sus", "su", "nuestro", "nuestra", "nuestros", "nuestras",
    "este", "esta", "estos", "estas", "ese", "esa", "esos", "esas",
    "aquel", "aquella", "aquellos", "aquellas", "esto", "eso", "aquello",
    # Verbos auxiliares y copulativos
    "es", "son", "era", "fue", "ser", "estar", "tiene", "tener",
    "hay", "había", "haber", "hace", "hacer", "han", "ha", "he",
    "sido", "estado", "tenido", "hecho", "va", "ir", "pueda", "puede",
    "deben", "debe", "requiere", "requerimos", "buscamos", "necesitamos",
    # Adverbios comunes
    "no", "sí", "si", "también", "tampoco", "solo", "sólo", "muy",
    "más", "menos", "bien", "mal", "aquí", "ahí", "allí", "allá",
    "ahora", "antes", "después", "ya", "aún", "todavía", "siempre",
    "nunca", "jamás", "casi", "bastante", "demasiado", "tan", "tanto",
    "así", "además", "incluyendo",
    # Cuantificadores
    "todo", "toda", "todos", "todas", "mucho", "mucha", "muchos", "muchas",
    "poco", "poca", "pocos", "pocas", "otro", "otra", "otros", "otras",
    "mismo", "misma", "mismos", "mismas", "cada", "cual", "cuales",
    "quien", "quienes",
    # Números escritos
    "uno", "dos", "tres", "cuatro", "cinco", "primer", "primera",
    "segundo", "segunda",
    # ── Stopwords específicas de OFERTAS DE EMPLEO ──────────────────────────
    # Aparecen en casi el 100% de las descripciones y no diferencian puestos
    "empresa", "trabajo", "empleo", "puesto", "cargo", "perfil",
    "candidato", "candidata", "vacante", "oportunidad",
    "funciones", "actividades", "responsabilidades", "tareas",
    "requisitos", "requerimientos", "habilidades", "competencias",
    "beneficios", "ofrecemos", "ofrecemos",
    "sueldo", "salario", "remuneración", "compensación",
    "tiempo", "completo", "parcial", "contrato", "permanente", "temporal",
    "meses", "años", "año", "mes", "experiencia", "requerida",
    "favor", "postular", "aplicar", "enviar", "cv", "currículum",
    "lugar", "trabajo", "empleo", "presencial", "semipresencial",
    "tipo", "puesto",
}

STOPWORDS_EN = {
    # Articles
    "a", "an", "the",
    # Prepositions
    "in", "on", "at", "by", "for", "with", "about", "as", "into",
    "through", "during", "before", "after", "above", "below", "from",
    "to", "of", "up", "down", "out", "off", "over", "under",
    # Conjunctions
    "and", "but", "or", "nor", "so", "yet", "both", "either",
    "neither", "not", "only", "whether", "because", "if", "when",
    "while", "although", "though", "since", "unless",
    # Pronouns
    "i", "you", "he", "she", "we", "they", "it", "me", "him",
    "her", "us", "them", "my", "your", "his", "its", "our",
    "their", "this", "that", "these", "those",
    # Auxiliaries
    "is", "are", "was", "were", "be", "been", "being", "have",
    "has", "had", "do", "does", "did", "will", "would", "shall",
    "should", "may", "might", "must", "can", "could",
    # Common adverbs
    "very", "really", "also", "just", "more", "most", "much",
    "well", "even", "still", "already", "now", "here", "there",
    "always", "never", "too", "quite", "including",
    # ── Stopwords específicas de JOB POSTINGS ───────────────────────────────
    "company", "position", "role", "candidate", "opportunity", "job",
    "responsibilities", "requirements", "qualifications", "skills",
    "experience", "required", "preferred", "benefits", "salary",
    "apply", "send", "resume", "cv", "full", "time", "part",
    "contract", "permanent", "work", "place", "office",
    "years", "year", "months", "month",
    "please", "join", "team", "looking", "seeking",
}

STOPWORDS_TODAS = STOPWORDS_ES | STOPWORDS_EN


# ═══════════════════════════════════════════════════════════════════════════════
# 2. PREPROCESAMIENTO LINGÜÍSTICO
# ═══════════════════════════════════════════════════════════════════════════════

def _tokenizar(texto: str) -> list[str]:
    """
    Tokeniza texto en minúsculas por espacios y elimina puntuación suelta.

    No usamos un tokenizador sofisticado (spaCy, NLTK) a propósito:
    los términos técnicos del dominio (AutoCAD, APQP, IATF16949, MLOps)
    son tokens válidos y un tokenizador general podría partirlos.
    """
    if not isinstance(texto, str):
        return []
    texto = texto.lower()
    # Eliminar caracteres de puntuación sueltos, conservar hyphens internos
    # (e.g., "full-stack", "end-to-end" son términos técnicos válidos)
    texto = re.sub(r"[^\w\s\-]", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto.split()


def preprocesar_nlp(texto: str, idioma: str) -> str:
    """
    Convierte descripcion_limpia en descripcion_nlp:

    1. Tokeniza (lowercase, elimina puntuación)
    2. Elimina stopwords según el idioma detectado
    3. Elimina tokens muy cortos (≤ 2 chars) y puramente numéricos
    4. Lematiza con simplemma
    5. Devuelve tokens reunidos en string

    Ejemplo (descripción de AI Engineer):
      entrada : "Buscamos ingeniero con experiencia en modelos de lenguaje natural"
      sin SW  : "ingeniero experiencia modelos lenguaje natural"
      lemas   : "ingeniero experiencia modelo lenguaje natural"

    Por qué lemas y no stems:
      Un stem puede producir "automat" de "automatización", que es ilegible
      en una word cloud. El lema produce "automatización" → válido.
      simplemma incluye tablas de lemas para ES y EN sin necesidad de
      descargar modelos grandes.

    Términos técnicos (SQL, Python, AutoCAD, APQP):
      simplemma los retorna sin cambios al no encontrarlos en su tabla,
      lo cual es exactamente el comportamiento correcto para siglas y
      tecnologías.
    """
    if not isinstance(texto, str) or texto.strip() == "":
        return ""

    tokens = _tokenizar(texto)

    if idioma == "es":
        stops = STOPWORDS_ES
        lang  = "es"
    elif idioma == "en":
        stops = STOPWORDS_EN
        lang  = "en"
    else:
        # Textos mixtos o de idioma desconocido: aplicar ambas listas
        # y usar lematizador español (mayoría del corpus es ES con tecnicismos EN)
        stops = STOPWORDS_TODAS
        lang  = "es"

    procesados = []
    for token in tokens:
        if token in stops:      continue   # es stopword
        if len(token) <= 2:     continue   # token muy corto
        if token.isdigit():     continue   # número puro (años, cantidades)

        # Lematizar — si simplemma no reconoce el token lo retorna igual
        lema = simplemma.lemmatize(token, lang=lang)
        procesados.append(lema)

    return " ".join(procesados)


def preprocesar_titulo(titulo: str, idioma: str) -> str:
    """
    Aplica el mismo preprocesamiento NLP al título del puesto.

    Los títulos son muy cortos pero muy informativos:
    "Senior Machine Learning Engineer" → "senior machine learning engineer"
    (aquí casi no hay stopwords a eliminar en títulos técnicos)
    """
    return preprocesar_nlp(titulo, idioma)


# ═══════════════════════════════════════════════════════════════════════════════
# 3. PIPELINE PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════════

def procesar(df: pd.DataFrame) -> pd.DataFrame:
    total = len(df)
    mask_util = ~df["es_vacio"]
    utiles = mask_util.sum()

    log.info(f"  Total registros: {total:,}  |  Con descripción: {utiles:,}")
    log.info("  (registros vacíos conservan columnas con string vacío)")

    # ── 3.1 descripcion_nlp ───────────────────────────────────────────────────
    log.info("\n  [1/3] Aplicando stopwords + lematización → descripcion_nlp...")
    df["descripcion_nlp"] = ""
    df.loc[mask_util, "descripcion_nlp"] = df.loc[mask_util].apply(
        lambda row: preprocesar_nlp(
            row["descripcion_limpia"],
            row.get("idioma_det", "otro"),
        ),
        axis=1,
    )

    # ── 3.2 titulo_nlp ────────────────────────────────────────────────────────
    log.info("  [2/3] Preprocesando títulos → titulo_nlp...")
    df["titulo_nlp"] = df.apply(
        lambda row: preprocesar_titulo(
            row["title"],
            row.get("idioma_det", "otro"),
        ),
        axis=1,
    )

    # ── 3.3 texto_nlp (combinado) ─────────────────────────────────────────────
    log.info("  [3/3] Construyendo texto_nlp (titulo_nlp + descripcion_nlp)...")
    df["texto_nlp"] = df.apply(
        lambda row: (
            f"{row['titulo_nlp']} {row['descripcion_nlp']}".strip()
        ),
        axis=1,
    )

    # ── 3.4 Longitud post-stopwords ───────────────────────────────────────────
    df["n_palabras_nlp"] = (
        df["descripcion_nlp"].str.split().str.len().fillna(0).astype(int)
    )

    # Marcar registros que quedaron vacíos DESPUÉS de stopwords
    # (textos que eran casi puros artículos/frases genéricas de RRHH)
    quedaron_vacios = mask_util & (df["n_palabras_nlp"] < MIN_TOKENS_NLP)
    if quedaron_vacios.sum() > 0:
        n_vacios = quedaron_vacios.sum()
        log.info(
            f"\n  ⚠  {n_vacios} registros quedaron con < {MIN_TOKENS_NLP} tokens "
            f"tras stopwords → es_vacio_nlp = True"
        )
    df["es_vacio_nlp"] = quedaron_vacios | df["es_vacio"]

    return df


def imprimir_resumen(df: pd.DataFrame) -> None:
    utiles_nlp = df[~df["es_vacio_nlp"]]
    log.info("\n" + "═" * 60)
    log.info("  RESUMEN — maestro_nlp.csv")
    log.info("═" * 60)
    log.info(f"  Total registros          : {len(df):>7,}")
    log.info(f"  Útiles para NLP          : {len(utiles_nlp):>7,}  ({len(utiles_nlp)/len(df)*100:.1f}%)")
    log.info(f"  Vacíos (NLP)             : {df['es_vacio_nlp'].sum():>7,}")

    log.info("\n  Palabras por carrera (mediana, post-stopwords):")
    stats = (
        utiles_nlp.groupby("career")["n_palabras_nlp"]
                  .median()
                  .sort_values(ascending=False)
                  .head(10)
    )
    for career, med in stats.items():
        log.info(f"    {career:<40} {med:>5.0f} tokens")

    log.info("\n  Distribución por idioma (registros útiles NLP):")
    for lang, cnt in utiles_nlp["idioma_det"].value_counts().items():
        log.info(f"    {lang:<6} {cnt:>5,}  ({cnt/len(utiles_nlp)*100:.1f}%)")

    log.info("\n  Ejemplo ANTES → DESPUÉS:")
    muestras = utiles_nlp[utiles_nlp["idioma_det"] == "es"].sample(
        min(3, len(utiles_nlp[utiles_nlp["idioma_det"] == "es"])),
        random_state=42,
    )
    for _, row in muestras.iterrows():
        log.info(f"\n  [{row['career']} / {row.get('city', '?')}]")
        log.info(f"  LIMPIO : {str(row['descripcion_limpia'])[:120]}")
        log.info(f"  NLP    : {str(row['descripcion_nlp'])[:120]}")

    log.info("\n  Guía de uso de columnas:")
    log.info("  ┌─────────────────────┬──────────────────────────────────────┐")
    log.info("  │ Campo               │ Usar para                            │")
    log.info("  ├─────────────────────┼──────────────────────────────────────┤")
    log.info("  │ descripcion_limpia  │ BERTopic, embeddings, K-Means        │")
    log.info("  │ titulo_desc         │ BERTopic con contexto de título       │")
    log.info("  │ descripcion_nlp     │ BoW, TF-IDF, n-gramas, LDA           │")
    log.info("  │ titulo_nlp          │ Análisis de títulos, ranking          │")
    log.info("  │ texto_nlp           │ Word cloud, TF-IDF combinado          │")
    log.info("  │ n_palabras_nlp      │ Filtro de calidad, estadísticas       │")
    log.info("  │ region              │ Visualización por procedencia         │")
    log.info("  │ career              │ Etiqueta para clustering supervisado  │")
    log.info("  └─────────────────────┴──────────────────────────────────────┘")

    log.info("\n  Próximos pasos sugeridos:")
    log.info("  1. BoW / TF-IDF:")
    log.info("       from sklearn.feature_extraction.text import TfidfVectorizer")
    log.info("       corpus = df[~df.es_vacio_nlp]['descripcion_nlp']")
    log.info("       tfidf = TfidfVectorizer(ngram_range=(1,3), max_features=5000)")
    log.info("       X = tfidf.fit_transform(corpus)")
    log.info("")
    log.info("  2. BERTopic:")
    log.info("       from bertopic import BERTopic")
    log.info("       docs = df[~df.es_vacio]['descripcion_limpia'].tolist()")
    log.info("       model = BERTopic(language='multilingual')")
    log.info("       topics, probs = model.fit_transform(docs)")
    log.info("")
    log.info("  3. LDA:")
    log.info("       from sklearn.decomposition import LatentDirichletAllocation")
    log.info("       from sklearn.feature_extraction.text import CountVectorizer")
    log.info("       bow = CountVectorizer(max_features=3000)")
    log.info("       X_bow = bow.fit_transform(corpus)")
    log.info("       lda = LatentDirichletAllocation(n_components=10)")
    log.info("       lda.fit(X_bow)")
    log.info("")
    log.info("  4. K-Means con distancia coseno:")
    log.info("       from sentence_transformers import SentenceTransformer")
    log.info("       from sklearn.cluster import KMeans")
    log.info("       encoder = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')")
    log.info("       embeddings = encoder.encode(docs)")
    log.info("       km = KMeans(n_clusters=10).fit(embeddings)")


def main(dry_run: bool = False, sample: int = None) -> None:
    log.info("═" * 60)
    log.info("  SCRIPT 2/2 — PREPROCESAMIENTO NLP (BoW / TF-IDF / LDA)")
    log.info("═" * 60)

    if not INPUT_CSV.exists():
        log.error(f"No encontrado: {INPUT_CSV}")
        log.error("  → Ejecuta primero: python src/nlp/01_text_cleaner.py")
        sys.exit(1)

    log.info(f"Leyendo {INPUT_CSV.name}...")
    df = pd.read_csv(INPUT_CSV, dtype=str, low_memory=False)

    # Restaurar tipos
    for col in ["salary_min", "salary_max", "n_palabras"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["is_premium"]  = df["is_premium"].map({"True": True, "False": False}).fillna(False)
    df["tiene_salario"] = df["tiene_salario"].map({"True": True, "False": False}).fillna(False)
    df["es_vacio"]    = df["es_vacio"].map({"True": True, "False": False}).fillna(True)
    log.info(f"  {len(df):,} registros cargados")

    if sample:
        df = df.sample(sample, random_state=42).reset_index(drop=True)
        log.info(f"  [--sample] Usando muestra de {sample} registros")

    df = procesar(df)
    imprimir_resumen(df)

    if dry_run:
        log.info("\n  [--dry-run] No se guardó ningún archivo.")
        return

    FINAL_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    log.info(f"\n  ✅ Guardado: {OUTPUT_CSV.relative_to(BASE_DIR)}")
    log.info(f"     {len(df):,} filas  ×  {len(df.columns)} columnas")
    log.info("═" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Preprocesamiento NLP para BoW/TF-IDF/LDA")
    parser.add_argument("--dry-run", action="store_true", help="No guardar archivos")
    parser.add_argument("--sample", type=int, default=None, help="Procesar solo N filas")
    args = parser.parse_args()
    main(dry_run=args.dry_run, sample=args.sample)