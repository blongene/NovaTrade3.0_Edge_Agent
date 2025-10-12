
NovaTrade Edge Agent — Phase-2 (Command Bus)
- edge_agent.py: polls cloud /api/commands, executes (dryrun by default), ACKs with HMAC
- mexc_executor.py: live MEXC spot executor (market/IOC), idempotent clientOrderId
Env:
  CLOUD_BASE_URL=...
  AGENT_ID=edge-nl-1
  EDGE_SECRET=...
  EDGE_MODE=dryrun|live
  EDGE_HOLD=false
  MEXC_KEY=...  MEXC_SECRET=...
