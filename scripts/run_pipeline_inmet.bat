@echo off

type NUL > D:\Pipeline_Inmet\scripts\log.txt

cd /d D:\Pipeline_Inmet

echo [%date% %time%] Iniciando pipeline... >> D:\Pipeline_Inmet\scripts\log.txt

docker-compose build >> D:\Pipeline_Inmet\scripts\log.txt 2>&1

docker-compose up -d >> D:\Pipeline_Inmet\scripts\log.txt 2>&1

echo [%date% %time%] Pipeline finalizado. >> D:\Pipeline_Inmet\scripts\log.txt