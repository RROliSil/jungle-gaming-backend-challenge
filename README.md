# 🦧 Jungle Gaming — Distributed Wagering Processor

Serviço financeiro distribuído para processamento de transações de apostas (*wagering*) de múltiplos provedores de jogos (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`), construído com **Bun 1.x**, **TypeScript (strict mode)**, **NestJS**, **MikroORM**, **PostgreSQL**, **AWS SQS (LocalStack)**, **CRUD Completo** e **WebSockets em Tempo Real**.

Documentação detalhada da arquitetura, invariantes, escolhas técnicas e trade-offs: consulte o arquivo [**ARCHITECTURE.md**](./ARCHITECTURE.md).

---

## 1. Requisitos Atendidos

- **Precisão Monetária (`Money`)**: Uso exclusivo de `Decimal` (`decimal.js`) com escala fixa de 2 casas decimais (`"25.00"`) e colunas `NUMERIC(20, 2)` no PostgreSQL. Zero uso de `number`, `float` ou `double` para dinheiro.
- **Concorrência por `walletId`**: Controle de concorrência pessimista (`SELECT ... FOR UPDATE` / `LockMode.PESSIMISTIC_WRITE`) por carteira + controle de versão (`version` inicia em `1` e incrementa apenas quando o saldo muda), permitindo múltiplas instâncias simultâneas sem lost updates ou saldo negativo.
- **Idempotência Persistente e JSON Canônico**: Deduplicação por `Idempotency-Key` no PostgreSQL com `payloadHash` SHA-256 sobre JSON canônico ordenado. Replays idênticos retornam o resultado original (incluindo o saldo observado no momento do processamento) com `idempotentReplay: true`; payloads divergentes retornam `409 Conflict`.
- **Ledger Imutável + Partidas Dobradas**: Lançamentos `DEBIT`/`CREDIT` validados aritmeticamente na aplicação e por constraints `CHECK` + trigger SQL `trg_immutable_ledger` que bloqueia `UPDATE` e `DELETE` na tabela `wallet_ledger_entries`. Inclui projeção de *Double-Entry Bookkeeping*.
- **Operações Fora de Ordem (`PENDING_REFERENCE`)**: `REFUND` e `ROLLBACK` recebidos antes da transação referenciada são persistidos como `PENDING_REFERENCE` (`202 Accepted`) e reprocessados pelo `PendingReferenceWorker` com backoff exponencial até resolução (`PROCESSED`) ou expiração (`REJECTED` com `REFERENCE_NOT_FOUND`).
- **SQS Consumer + Transactional Inbox & Outbox**: Consumidor da fila `wager-transactions.fifo` reutilizando o mesmo caso de uso do HTTP, deduplicando via `inbox_messages`, fazendo `ack` somente após o commit SQL, com retry exponencial, roteamento para `wager-transactions-dlq.fifo` e graceful shutdown em `SIGTERM`.
- **Publicação Concorrente de Outbox**: `OutboxWorker` com `FOR UPDATE SKIP LOCKED` publicando `WalletBalanceChanged`, `WagerTransactionProcessed`, `WagerTransactionRejected` e `WagerTransactionPendingReference`.
- **CRUD Completo**: Endpoints completos de criação, listagem, consulta, atualização e remoção/encerramento seguro para **Providers** (`/providers`), **Games** (`/games`) e **Wallets** (`/wallets`).
- **WebSockets em Tempo Real (`/ws/wagering`)**: Gateway Socket.IO que transmite eventos financeiros confirmados **estritamente após o commit da transação SQL**, com suporte a salas por `walletId`, `playerId`, `roundId` e `providerId`, além de dashboard interativo em `/dashboard`.
- **Observabilidade**: Logs estruturados em JSON (com `correlationId`, `messageId`, `transactionId`, `walletId`, `providerId` e redação automática de dados sensíveis), métricas Prometheus e JSON em `/metrics`, e health checks `/health/live` e `/health/ready`.

---

## 2. Como Executar o Projeto

### 2.1 Subir Infraestrutura via Docker Compose
```bash
# Sobe PostgreSQL 16, LocalStack (SQS FIFO + DLQ) e Keycloak (OIDC IdP)
docker compose up -d postgres localstack keycloak

# (Opcional) Subir cluster com 3 instâncias simultâneas da aplicação nas portas 3001, 3002 e 3003
docker compose --profile cluster up --build -d
```

### 2.2 Instalar Dependências e Rodar Localmente com Bun
```bash
# Instalar dependências
bun install

# Executar migrations (up / down)
bun run migration:up

# Iniciar servidor em modo desenvolvimento (porta 3001 -> http://localhost:3001)
bun run start:dev
```

---

## 3. Como Executar os Testes

A suíte de testes suporta execução tanto conectada aos containers Docker (`PostgreSQL` na porta `5432` e `LocalStack` na porta `4566`) quanto de forma autônoma (iniciando automaticamente o engine PostgreSQL 16 `PGlite` e broker HTTP SQS caso os containers não estejam ativos na máquina):

```bash
# Rodar todos os testes (Unidade + Integração + Concorrência)
bun test

# Apenas testes de unidade (Domínio, Money, Wallet, WagerTransaction, Canonical Hash, Outbox, CRUD)
bun run test:unit

# Apenas testes de integração (Migrations, Triggers, Constraints, HTTP, SQS, Inbox/Outbox, DLQ, CRUD, WebSockets)
bun run test:integration

# Apenas testes de concorrência (Os 8 cenários obrigatórios em paralelismo real + invariante final do ledger)
bun run test:concurrency

# Diferencial opcional: Teste de carga com relatório de throughput, p50/p95/p99 e reconciliação
bun run test:load
```

---

## 4. Endpoints da API HTTP

### 4.1 Wallets (Desafio + CRUD)
- `POST /wallets` — Cria uma wallet (gera transação interna `OPENING` e lançamento `CREDIT` na mesma transação SQL quando `initialBalance > 0`).
- `GET /wallets` — Lista wallets com filtros opcionais (`?playerId=...&currency=BRL&status=ACTIVE`).
- `GET /wallets/:walletId` — Consulta saldo e versão da wallet.
- `PATCH /wallets/:walletId` — Atualiza status administrativo (`ACTIVE`, `FROZEN`) ou `metadata`.
- `DELETE /wallets/:walletId` — Encerra a wallet (`CLOSED`) de forma segura caso o saldo seja `0.00`, preservando todo o ledger.
- `GET /wallets/:walletId/ledger?cursor=...&limit=50` — Consulta lançamentos do ledger com cursor estável e opaco.
- `POST /wallets/:walletId/reconciliation` — Reconcilia `storedBalance` contra o saldo reconstruído pelo ledger.

### 4.2 Wagering Transactions
- `POST /wagering/transactions` (Header obrigatório: `Idempotency-Key`) — Submete operação (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`).
- `GET /wagering/transactions/:transactionId` — Consulta transação pelo ID interno.
- `GET /providers/:providerId/wagering/transactions/:externalTransactionId` — Consulta transação pela chave do provedor.

### 4.3 Providers & Games CRUD
- `POST /providers`, `GET /providers`, `GET /providers/:providerId`, `PATCH /providers/:providerId`, `PUT /providers/:providerId`, `DELETE /providers/:providerId`
- `POST /games`, `GET /games`, `GET /games/:gameId`, `PATCH /games/:gameId`, `PUT /games/:gameId`, `DELETE /games/:gameId`

### 4.4 Observabilidade & Dashboard em Tempo Real
- `GET /health/live` — Liveness probe (público).
- `GET /health/ready` — Readiness probe verificando PostgreSQL e SQS (público).
- `GET /metrics` — Métricas em formato Prometheus (ou JSON via `/metrics?format=json`).
- `GET /dashboard` — Painel HTML interativo em `http://localhost:3001/dashboard` conectado via WebSocket (`/ws/wagering`).

---

## 5. WebSockets (`/ws/wagering`)

Conecte-se via Socket.IO ao namespace `/ws/wagering`:
- **Mensagens aceitas pelo Gateway**:
  - `subscribe`: `{ walletId?: string, playerId?: string, roundId?: string, providerId?: string, all?: boolean }`
  - `unsubscribe`: `{ walletId?: string, playerId?: string, roundId?: string, providerId?: string }`
  - `ping`: retorna `{ event: "pong", timestamp: "..." }`
- **Eventos transmitidos pós-commit**:
  - `WalletBalanceChanged`
  - `WagerTransactionProcessed`
  - `WagerTransactionRejected`
  - `WagerTransactionPendingReference`
  - `WalletReconciliationCompleted`
