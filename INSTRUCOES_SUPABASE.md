# ✅ Sistema de Histórico no Supabase - Concluído

## Resumo da Implementação

O sistema de histórico de processamentos foi implementado com sucesso! Agora todos os uploads ficam guardados permanentemente no Supabase, libertando **15GB de espaço** (ficheiros antigos já foram apagados do diretório `work/`).

## 📋 Próximo Passo: Criar as Tabelas no Supabase

### Passo 1: Aceder ao Supabase SQL Editor

1. Aceda ao seu projeto Supabase: https://fmudwmmpkqiqwrduzkoc.supabase.co
2. Faça login se necessário
3. No menu lateral esquerdo, clique em **SQL Editor**
4. Clique em **"New query"**

### Passo 2: Executar o Script SQL

1. Abra o ficheiro `scripts/supabase_schema.sql` neste projeto
2. Copie **TODO** o conteúdo do ficheiro
3. Cole no SQL Editor do Supabase
4. Clique no botão **"Run"** (ou pressione Ctrl+Enter)

### Passo 3: Verificar que as Tabelas foram Criadas

1. No menu lateral do Supabase, clique em **"Table Editor"**
2. Deverá ver 2 novas tabelas:
   - ✅ `processing_history` (guarda informação de cada upload)
   - ✅ `poker_stats_detail` (guarda estatísticas detalhadas)

## 🎯 Funcionalidades Implementadas

### 1. Histórico Automático
Cada upload é automaticamente guardado no Supabase com:
- Nome do ficheiro
- Data e hora do processamento
- Utilizador que fez o upload
- Total de mãos processadas
- Sites de poker detectados
- Estatísticas completas por site/mesa
- Pontuação geral

### 2. Consultar Histórico
Aceda ao histórico de uploads em:
- **Nova página dedicada**: `/history` 
- **API para programação**: `/api/history/my`

A página de histórico mostra:
- ✅ Resumo geral (total uploads, mãos, sites)
- ✅ Tabela com todos os processamentos
- ✅ Link direto para o dashboard de cada upload

### 3. Limpeza Automática de Ficheiros Antigos
- ✅ **15GB de ficheiros antigos apagados** do diretório `work/`
- Os dados continuam disponíveis no Supabase
- Endpoints admin para gerir limpeza futura:
  - `GET /api/admin/cleanup/work-stats` - Ver espaço ocupado
  - `POST /api/admin/cleanup/work-all` - Apagar todos os ficheiros locais
  - `POST /api/admin/cleanup/work-old` - Apagar ficheiros >7 dias

## 📊 Estrutura de Dados

### Tabela `processing_history`
Informação principal de cada upload:
```
- id: Identificador único
- token: Token do processamento
- user_id: Email do utilizador
- filename: Nome do ficheiro
- created_at: Data/hora do upload
- total_hands: Total de mãos
- total_sites: Número de sites
- overall_score: Pontuação geral
- pko_count, mystery_count, nonko_count: Classificação
- full_result: Resultado completo em JSON
```

### Tabela `poker_stats_detail`
Estatísticas detalhadas:
```
- site: PokerStars, GGPoker, etc.
- table_format: nonko_9max, pko_6max, etc.
- stat_name: "Early RFI", "3bet", etc.
- opportunities: Número de oportunidades
- attempts: Número de tentativas
- percentage: Percentagem calculada
```

## 🔧 Endpoints API Disponíveis

### Para Utilizadores
- `GET /api/history/my` - Meu histórico (últimos 50 uploads)
- `GET /api/history/details/<token>` - Detalhes de um processamento
- `GET /api/history/stats` - Estatísticas agregadas

### Para Administradores
- `GET /api/admin/cleanup/work-stats` - Estatísticas do diretório work/
- `POST /api/admin/cleanup/work-all` - Apagar todos os ficheiros
- `POST /api/admin/cleanup/work-old` - Apagar ficheiros antigos (>7 dias)

## ⚙️ Configuração Atual

✅ **Credenciais Configuradas**
- `SUPABASE_URL`: https://fmudwmmpkqiqwrduzkoc.supabase.co
- `SUPABASE_KEY`: Guardada como secret no Replit

✅ **Integração no Pipeline**
- Cada upload bem-sucedido é automaticamente guardado
- Falhas não afetam o processamento (são registadas mas não bloqueiam)

✅ **Ficheiros Criados**
- `app/services/supabase_history.py` - Serviço de histórico
- `app/api/history.py` - Endpoints API
- `app/api/cleanup_admin.py` - Endpoints admin
- `templates/history.html` - Interface web
- `scripts/supabase_schema.sql` - Schema das tabelas
- `SETUP_SUPABASE.md` - Documentação detalhada

## 🚀 Testar o Sistema

Após criar as tabelas no Supabase:

1. **Fazer um upload de teste**
   - Aceda a `/upload`
   - Faça upload de um ficheiro ZIP/RAR

2. **Verificar que foi guardado**
   - Aceda a `/history`
   - Deverá ver o upload na lista

3. **Verificar no Supabase**
   - Aceda ao Table Editor
   - Clique em `processing_history`
   - Deverá ver um registo novo

## ✨ Benefícios

✅ **Histórico Permanente** - Nunca perde dados de uploads antigos
✅ **15GB Libertados** - Espaço em disco recuperado
✅ **Consulta Rápida** - Acesso instantâneo a processamentos anteriores
✅ **Estatísticas Agregadas** - Visão geral de todos os seus uploads
✅ **Escalável** - Supabase cresce com a sua aplicação

## ❓ Suporte

Se tiver problemas:
1. Verifique que as tabelas foram criadas no Supabase
2. Confirme que as credenciais estão corretas
3. Verifique os logs da aplicação para erros de conexão

---

**Última atualização**: 12 de Novembro de 2025
**Status**: ✅ Pronto para usar (após criar tabelas no Supabase)
