$HostAddr = $env:HOST
if ([string]::IsNullOrWhiteSpace($HostAddr)) { $HostAddr = "0.0.0.0" }
$Port = $env:PORT
if ([string]::IsNullOrWhiteSpace($Port)) { $Port = "5000" }
python web_app.py --host $HostAddr --port $Port --debug false
