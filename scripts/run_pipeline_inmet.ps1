Set-Location "$PSScriptRoot/.."

docker-compose build

docker-compose up -d

$date = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
"$date - Pipeline Inmet executado" | Out-File -FilePath "$PSScriptRoot/log.txt" -Append
