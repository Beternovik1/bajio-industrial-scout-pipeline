@echo off
cd /d "C:\Users\ASUS\Documents\proyectos\bajio-industrial-scout-pipeline"
call venv\Scripts\activate
python src/pipeline.py
python src/export.py
python src/reporter.py
echo Done! Press any key to close...
pause  