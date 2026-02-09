# Sozo MVP

Backend-first MVP for person/household intelligence with Azure-connected APIs.

## Implemented priorities
1. Azure OpenAI provisioning instructions and env vars
2. Backend APIs
- `GET /api/health` checks SQL, Search, Storage, Service Bus, and OpenAI
- `POST /api/chat` provides Azure OpenAI chat with SQL/Search tool-routing skeleton
- `GET /api/dashboard/summary` returns dashboard summary cards + artifacts
3. Minimal chat-first UI
- Renders answer text
- Renders citations
- Renders chart/table payload JSON

## Environment
Copy `.env.example` to `.env.local` and fill values.

## Development
```bash
npm run dev
```
Open [http://localhost:3000](http://localhost:3000).
