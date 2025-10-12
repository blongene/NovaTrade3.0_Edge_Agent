# --- NovaTrade Edge: one-click launcher (Windows PowerShell) ---
# Save as run_edge.ps1 and run by double-clicking run_edge.bat (below)

# 1) EDIT THESE ONCE (or keep reading from a .env later)
$env:CLOUD_BASE_URL      = "https://novatrade3-0.onrender.com"
$env:AGENT_ID            = "edge-cb-1"          # allowlisted on Render
$env:EDGE_SECRET         = "3f36e385d5b3c83e66209cdac0d815788e1459b49cc67b6a6159cfa4de34511b8"
$env:EDGE_MODE           = "dryrun"             # "live" when ready
$env:EDGE_HOLD           = "false"              # keep "true" for first live canary
$env:EDGE_POLL_SECS      = "10"

# Venue creds (fill when going live)
# Coinbase Advanced
$env:COINBASE_API_KEY     = "46214f1c-1f1c-4c00-9665-ca84095871f6"
$env:COINBASE_API_SECRET  = "O3IOe86PB9/5secn10+MlB/pwcHxf3J6Hnwzj3DT2k9k0FkYykrnzJutfcj3uchiR4P/fqnMFOq9S711/jir4A=="
# $env:COINBASE_API_PASSPHRASE = ""

# Binance.US
$env:BINANCEUS_API_KEY    = "l47FMjyCxrkYvf9BaqXxzpBaP0rQr0wjlQc71ljiExwzrzey5EKlan3zOcYpamX2"
$env:BINANCEUS_API_SECRET = "37n7IR1dAeDfqSCl8AbJwXBVKwWR35c6mWl6ZfSsgzT9xMgWzlrkZI1p5EtVNy5h"

# Kraken (only if you plan to use it)
$env:KRAKEN_KEY           = "esMldWnp3gu8TVpW0/P2MBXABn4lF2UP/ogzlhUh+lWvjEMCiU5H4GXa"
$env:KRAKEN_SECRET        = "LrmfhH3B9Q/WZk3K59f0nr/VhEwX0WeAYTD5UDJ+3sVo9E1TCkbkAiDHlpBsSBQVbdPmPx2yTBuBLMA9zmGMww=="

# 2) Activate venv if you use one (optional)
# if (Test-Path ".\.venv\Scripts\Activate.ps1") { . .\.venv\Scripts\Activate.ps1 }

# 3) Start the Edge Agent in a new console window
$edgeCmd = "cd `"$pwd`"; python edge_agent.py"
Start-Process powershell -ArgumentList "-NoExit","-Command",$edgeCmd

# 4) Optional: quick signed enqueue for a dry-run smoke (COINBASE $25 BUY)
#   Uncomment to test automatically.
# python ops_sign_and_enqueue.py --base $env:CLOUD_BASE_URL --secret $env:EDGE_SECRET `
#   --agent $env:AGENT_ID --venue COINBASE --symbol BTC/USDC --side BUY --amount 25

# 5) Open useful debug pages
Start-Process "$($env:CLOUD_BASE_URL)/ops/debug/dbinfo"
Start-Process "$($env:CLOUD_BASE_URL)/ops/debug/cmds?status=pending"
Start-Process "$($env:CLOUD_BASE_URL)/ops/debug/cmds?status=in_flight"
Start-Process "$($env:CLOUD_BASE_URL)/ops/debug/cmds?status=done"

Write-Host "`nNovaTrade Edge launcher executed. Check the new console window for agent logs." -ForegroundColor Green
