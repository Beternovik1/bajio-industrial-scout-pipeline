#!/bin/bash
echo "Iniciando el pipeline del Bajio Industrial Scout..."

# mueve la terminal a la carpeta exacta donde está guardado este archivo
cd "$(dirname "$0")"

# activamos el entorno virtual
source .venv/bin/activate
python src/main.py

echo "Pipeline terminado. Ya puedes cerrar esta ventana."