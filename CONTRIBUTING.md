# Contributing to Sozo

## Branch Workflow

```
main ── protected (production: sozo-app.azurewebsites.net)
 └── dev ── integration (dev: sozo-app-dev.azurewebsites.net)
      ├── feature/your-feature-name
      ├── fix/bug-description
      └── chore/task-description
```

### Rules
- **Never push directly to `main`** — branch policies require a Pull Request with 1 reviewer
- Always branch from `dev` for new work
- Open a PR from your feature branch → `dev`
- When `dev` is stable, open a PR from `dev` → `main` (triggers production deploy)

### Daily Workflow

```bash
# 1. Start from latest dev
git checkout dev
git pull origin dev

# 2. Create your feature branch
git checkout -b feature/my-feature

# 3. Do your work, commit often
git add -A && git commit -m "Add awesome feature"

# 4. Push and open a PR to dev
git push origin feature/my-feature
# Then open a PR in Azure DevOps: feature/my-feature → dev

# 5. After PR is approved and merged to dev, the dev site auto-deploys
# 6. When dev is tested, a PR from dev → main triggers production deploy
```

## Environments

| Environment | URL | Database | Branch |
|------------|-----|----------|--------|
| **Production** | https://sozo-app.azurewebsites.net | `sozoapp` (Standard 20 DTU) | `main` |
| **Development** | https://sozo-app-dev.azurewebsites.net | `sozoapp-dev` (Basic 5 DTU) | `dev` |
| **Local** | http://localhost:3000 | Uses `.env.local` config | any |

## Local Setup

```bash
# 1. Clone the repo
git clone https://purefreedom-devops@dev.azure.com/purefreedom-devops/Data%20Analytics/_git/Data%20Analytics sozo
cd sozo

# 2. Install dependencies
npm install

# 3. Get .env.local from a team member (contains Azure credentials)
# Never commit this file

# 4. Run dev server
npm run dev
```

## Environment Variables

Copy `.env.local` from a team member. Key variables:

| Variable | Description |
|----------|-------------|
| `SOZO_SQL_HOST` | Azure SQL server |
| `SOZO_SQL_DB` | Database name (`sozoapp` for prod, `sozoapp-dev` for dev) |
| `SOZO_SQL_USER` | SQL username |
| `SOZO_SQL_PASSWORD` | SQL password |
| `SOZO_OPENAI_*` | Azure OpenAI config |
| `SOZO_SEARCH_*` | Azure AI Search config |
| `AUTH_*` | Microsoft Entra ID auth config |

## CI/CD Pipeline

Defined in `azure-pipelines.yml`:
- Push to `main` → builds and deploys to **production**
- Push to `dev` → builds and deploys to **development**

### First-Time Pipeline Setup
1. Go to Azure DevOps → Pipelines → New Pipeline
2. Select "Azure Repos Git" → "Data Analytics" repo
3. Select "Existing Azure Pipelines YAML file" → `/azure-pipelines.yml`
4. Save and run

## Code Review Checklist

Before approving a PR:
- [ ] Does the code build? (`npm run build`)
- [ ] No hardcoded credentials or secrets
- [ ] SQL queries use parameterized inputs (no string concatenation)
- [ ] New serving views added to `sql-guard.ts` ALLOWED_TABLES
- [ ] New views documented in `schema-context.ts`
