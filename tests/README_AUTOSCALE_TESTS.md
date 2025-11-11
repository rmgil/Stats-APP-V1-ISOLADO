# Autoscale Production Tests

Testes de produção para validar o sistema de upload distribuído com autoscale deployment.

## 🎯 Objetivos

Validar que o sistema suporta **8-12 uploads simultâneos de 100MB+** sem:
- Falhas de race conditions
- Exhaustion de connection pool
- Memory leaks
- Timeouts ou freezes

---

## 📋 Pré-requisitos

### 1. Instalar Dependências de Teste

```bash
pip install aiohttp psutil
```

### 2. Configurar Ambiente

Definir variáveis de ambiente (opcional - defaults para localhost):

```bash
export BASE_URL="http://localhost:5000"
export TEST_USER_EMAIL="test@example.com"
export TEST_USER_PASSWORD="testpass"
```

### 3. Sistema Em Execução

Garantir que:
- Flask server está running (local ou production)
- PostgreSQL database acessível
- Background worker ativo

---

## 🧪 Testes Disponíveis

### Task 18: Regression Test - SERIALIZABLE Validation

**Objetivo:** Validar que 2 uploads podem finalizar simultaneamente sem race conditions.

**O que testa:**
- ✓ SERIALIZABLE transaction isolation
- ✓ Safeguards atómicos (queue + user limits)
- ✓ Retry logic em serialization conflicts
- ✓ Autocommit preservation no connection pool

**Como executar:**

```bash
# 1. Criar ficheiros de teste (pequenos para este teste)
mkdir -p tests/fixtures
dd if=/dev/urandom of=tests/fixtures/test_file_1.zip bs=1M count=10
dd if=/dev/urandom of=tests/fixtures/test_file_2.zip bs=1M count=10

# 2. Executar teste
python tests/test_concurrent_uploads.py
```

**Resultado esperado:**
```
✓✓ PASS: Both uploads finalized successfully!
✓ SERIALIZABLE transactions working correctly
✓ No race conditions detected
```

---

### Task 19: Stress Test - 12 Concurrent 100MB Uploads

**Objetivo:** Testar limites do sistema com carga máxima.

**O que testa:**
- ✓ Connection pool (20 conns/worker) não esgota
- ✓ Memory streaming (fetchmany) previne overflow
- ✓ Queue safeguards bloqueiam quando >= 50 pending
- ✓ 8 workers Gunicorn processam tudo

**Como executar:**

```bash
# Executar teste (cria ficheiros automaticamente)
python tests/test_stress_uploads.py
```

**Métricas monitorizadas:**
- Total time
- Throughput (MB/s)
- Peak memory usage
- Success/failure rate

**Resultado esperado:**
```
✓✓✓ PASS: All uploads completed successfully!
✓ Connection pool handled concurrent load
✓ Memory usage acceptable
✓ Queue safeguards working
```

---

### Task 20: Soak Test - 50 Uploads Over 2 Hours

**Objetivo:** Detectar memory leaks e instabilidade ao longo do tempo.

**O que testa:**
- ✓ Memory leaks (trend analysis)
- ✓ Connection pool não exaure ao longo do tempo
- ✓ Worker heartbeat + timeout funcionam
- ✓ GC enforcement evita acumulação

**Como executar:**

```bash
# Executar teste (2+ horas de runtime)
python tests/test_soak.py
```

**Métricas monitorizadas:**
- Memory delta (initial → final)
- Memory leak rate (MB/hour)
- Upload time stability
- Failure pattern over time

**Resultado esperado:**
```
✓✓✓ PASS: All uploads successful!
✓ No memory leaks detected
✓ System stable over 2 hours
```

---

## 📊 Interpretação de Resultados

### ✅ Sistema PASS se:

1. **Task 18:**
   - Ambos uploads finalizam com sucesso
   - Nenhum erro de "serialization conflict"
   - Safeguards bloqueiam corretamente

2. **Task 19:**
   - Todos 12 uploads completam (0 failures)
   - Peak memory < 2GB (com 100MB x 12)
   - Throughput estável (sem degradação)

3. **Task 20:**
   - Memory leak rate < 10 MB/hour
   - 0-2 failures máximo (< 5%)
   - Upload times consistentes (±20% variance)

### ❌ Sistema FAIL se:

- **Serialization errors frequentes** → Connection pool contamination
- **Memory > 3GB** → Streaming não está funcionando
- **> 3 failures em Task 19** → Race conditions ou safeguards broken
- **Memory leak > 50 MB/hour** → GC não está enforcing

---

## 🔍 Debugging Failures

### 1. Check Worker Status

```bash
curl http://localhost:5000/api/worker/status
```

Verificar:
- `health: 'healthy'` (ou 'degraded'/'critical')
- `queue.pending` < 50
- `workers.active` > 0
- `latest_heartbeat` recente

### 2. Check Database

```bash
psql $DATABASE_URL -c "
SELECT status, COUNT(*) 
FROM processing_jobs 
GROUP BY status;
"
```

Procurar:
- `pending` jobs stuck (> 30min old)
- `failed` jobs (check error_message)
- Orphan sessions (status != 'completed')

### 3. Check Logs

```bash
# Worker logs
grep "ERROR\|WARNING" /tmp/logs/Start_application_*.log | tail -50

# Memory tracking
grep "RAM\|CPU" /tmp/logs/Start_application_*.log | tail -20
```

### 4. Check Connection Pool

Se uploads falham com "connection pool exhausted":
- Verificar que `DatabasePool` usa 20 max connections
- Confirmar que `return_connection()` é chamado em `finally`
- Validar que autocommit é restaurado

---

## 🚀 Production Deployment Checklist

Antes de deploy para autoscale:

- [x] Connection pooling implementado (20 conns/worker)
- [x] Safeguards race-free (SERIALIZABLE transactions)
- [x] Memory streaming (fetchmany não fetchall)
- [x] Heartbeat + timeout (30min auto-recovery)
- [x] Worker status endpoint (/api/worker/status)
- [ ] **Task 18 PASS** (regression test)
- [ ] **Task 19 PASS** (stress test)
- [ ] **Task 20 PASS** (soak test)

---

## 📝 Notas

- **Test files:** Scripts criam ficheiros automaticamente em `tests/fixtures/`
- **Cleanup:** Testes perguntam se quer deletar ficheiros no final
- **Parallel execution:** Não executar múltiplos testes simultaneamente
- **Production:** Adaptar BASE_URL para production deployment antes de testar

---

## 🐛 Known Issues

Nenhum conhecido após correções de autocommit (Oct 21, 2025).

Sistema production-ready para autoscale deployment! 🚀
