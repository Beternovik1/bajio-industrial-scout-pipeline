@echo off
cd /d "C:\Users\ASUS\Documents\proyectos\bajio-industrial-scout-pipeline"
call venv\Scripts\activate
python src/pipeline.py
python src/export.py
:: No pause here, so the window closes and frees up memory