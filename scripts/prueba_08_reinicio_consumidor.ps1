# Prueba de aceptación 8: reiniciar el consumidor sin duplicar efectos ni perder mensajes (ENUNCIADO §11).
# Ejecutar desde la raíz del repositorio:  .\scripts\prueba_08_reinicio_consumidor.ps1
$ErrorActionPreference = "Stop"
$id = "test-reinicio-" + [guid]::NewGuid().ToString("N").Substring(0, 6)
$env_ = @("-e", "POSTGRES_HOST=postgres", "-e", "KAFKA_BOOTSTRAP_SERVERS=kafka:9092")

function Contar($eid) {
    $out = docker exec @env_ observatorio-producer python tests/reinicio_helper.py count $eid
    $h = @{}
    foreach ($p in ($out -split " ")) { $kv = $p -split "="; $h[$kv[0]] = [int]$kv[1] }
    return $h
}

Write-Host "1) Estado ANTES" -ForegroundColor Cyan
$antes = Contar $id
Write-Host "   evento_actual=$($antes.ACTUAL) evento_version=$($antes.VERSIONES)"

Write-Host "2) Paro el consumidor y publico un evento NUEVO mientras esta caido" -ForegroundColor Cyan
docker stop observatorio-consumer | Out-Null
docker exec @env_ observatorio-producer python tests/reinicio_helper.py send $id
Start-Sleep -Seconds 3
$caido = Contar $id
Write-Host "   con el consumidor parado: evento en BD = $($caido.EVENTO) (esperado 0: aun no se ha procesado)"

Write-Host "3) Arranco el consumidor y espero a que este healthy" -ForegroundColor Cyan
docker start observatorio-consumer | Out-Null
for ($i = 0; $i -lt 40; $i++) {
    $s = docker inspect --format "{{.State.Health.Status}}" observatorio-consumer
    if ($s -eq "healthy") { break }
    Start-Sleep -Seconds 3
}
Start-Sleep -Seconds 15

Write-Host "4) Estado DESPUES" -ForegroundColor Cyan
$despues = Contar $id
Write-Host "   evento_actual=$($despues.ACTUAL) evento_version=$($despues.VERSIONES) | evento nuevo: filas=$($despues.EVENTO), versiones=$($despues.VERSIONES_EVENTO)"

# El feed live puede aportar eventos reales entre ambas medidas: se comprueba el EVENTO de la prueba,
# que debe aparecer exactamente una vez (reanuda sin perder ni duplicar), y que sus versiones sean 1.
$ok = ($caido.EVENTO -eq 0) -and ($despues.EVENTO -eq 1) -and ($despues.VERSIONES_EVENTO -eq 1)
if ($ok) {
    Write-Host "RESULTADO: PASA - el consumidor reanudo, proceso el mensaje pendiente exactamente una vez" -ForegroundColor Green
} else {
    Write-Host "RESULTADO: FALLA" -ForegroundColor Red
    exit 1
}
