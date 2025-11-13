# File Processing Tools Application

## 📋 Visão Geral

Aplicação Flask com duas ferramentas principais:
1. **Filtro de Ficheiros TXT/XML**: Separa ficheiros de arquivos ZIP/RAR baseado em palavras-chave (Mystery/PKO/Non-KO)
2. **Fusão de CSV**: Combina dados de 4 ficheiros CSV numa única tabela consolidada

## 🚀 Como Executar Localmente

### Via Interface Web (Drag-and-Drop)

1. Iniciar o servidor:
```bash
python main.py
# ou
gunicorn --bind 0.0.0.0:5000 main:app
```

2. Abrir no browser: `http://localhost:5000`

3. Arrastar e soltar o arquivo ZIP/RAR na zona de upload

4. O download do resultado (`nome_original_separada.zip`) inicia automaticamente

### Via CLI (Linha de Comando)

Nova funcionalidade para processamento batch sem interface gráfica:

```bash
# Processar diretório
python -m app.classify input_dir=./uploads out_zip=./resultado.zip

# Processar arquivo ZIP/RAR
python -m app.classify input_dir=./arquivo.zip out_zip=./output.zip

# Modo DRY-RUN (apenas manifesto, sem ZIP)
python -m app.classify input_dir=./dados out_zip=./saida.zip --dry-run
```

## 📁 Estrutura de Saída

Ambos os métodos (Web e CLI) produzem a mesma estrutura:

```
resultado.zip/
├── PKO/                          # Ficheiros com palavras PKO/bounty/knockout
│   ├── ficheiro1.txt
│   └── ficheiro2.xml
├── MYSTERIES/                    # Ficheiros com mystery/mysteries
│   └── ficheiro3.txt
├── NON-KO/                       # Ficheiros sem palavras especiais
│   └── ficheiro4.txt
├── PKO.txt                       # Compilado de todos os PKO
├── NON-KO.txt                    # Compilado de todos os NON-KO
├── MYSTERIES.txt                 # Compilado de todos os mysteries
└── classification_manifest.json  # Manifesto detalhado (NOVO)
```

## 📊 Manifesto de Classificação

### Onde Encontrar

O `classification_manifest.json` é gerado automaticamente em cada execução e incluído no ZIP de saída. Contém:

```json
{
  "run_id": "uuid-único",
  "started_at": "2025-09-03T10:45:00",
  "finished_at": "2025-09-03T10:45:05",
  "totals": {
    "PKO": 10,
    "mystery": 5,
    "non-KO": 20,
    "unknown": 2
  },
  "files": [
    {
      "input": "torneio.txt",
      "output_class": "PKO",
      "detector": {
        "reason": "'bounty' in content",
        "score": 1.0
      },
      "encoding": "utf-8",
      "bytes": 1234
    }
  ]
}
```

### Informações do Manifesto

- **run_id**: Identificador único da execução
- **totals**: Contagem por categoria
- **files**: Detalhes de cada ficheiro processado
- **detector.reason**: Palavra-chave que determinou a classificação
- **encoding**: Codificação detectada (usa chardet)
- **output_class**: Pode ser "PKO", "mystery", "non-KO" ou "unknown"

## 🧪 Modo DRY-RUN

### Ativar via CLI
```bash
python -m app.classify input_dir=./teste out_zip=./saida.zip --dry-run
```

### Ativar via Variável de Ambiente
```bash
export DRY_RUN=true
python main.py
```

No modo DRY-RUN:
- ✅ Processa e classifica todos os ficheiros
- ✅ Gera o manifesto JSON com resultados
- ❌ NÃO cria o ficheiro ZIP
- Útil para testar classificações rapidamente

## 🔧 Detalhes Técnicos

### Palavras-Chave de Classificação

**Mystery** (vão para pasta MYSTERIES):
- mystery
- mysteries

**PKO** (vão para pasta PKO):
- bounty / bounties
- progressive
- pko
- ko
- knockout

**Non-KO** (vão para pasta NON-KO):
- Todos os outros ficheiros válidos
- Ficheiros vazios ou binários (marcados como "unknown" no manifesto)

### Deteção de Encoding

O sistema usa `chardet` para detetar automaticamente a codificação:
1. Analisa o ficheiro e sugere encoding com confiança
2. Se confiança < 70%, tenta: utf-8, latin1, cp1252, iso-8859-1
3. Usa `errors='replace'` para evitar crashes
4. Ficheiros com >30% caracteres inválidos são marcados como "unknown"

## 🏗️ Arquitetura

### Importante: Código NÃO Foi Reescrito

A implementação atual **mantém toda a lógica original** de classificação. Apenas foi:

1. **Encapsulada** em módulos reutilizáveis (`app/classify/api.py`)
2. **Exposta** via CLI para uso programático
3. **Enriquecida** com metadados (manifesto JSON)
4. **Melhorada** na deteção de encoding (chardet)

### Módulos Principais

```
main.py                    # Aplicação Flask original (inalterada na lógica)
├── process_txt_tree()     # Função de classificação (agora com manifesto)
├── unpack_any()          # Extração recursiva de arquivos
└── /upload               # Rota drag-and-drop (usa as mesmas funções)

app/classify/
├── __init__.py
├── __main__.py           # CLI que reutiliza funções do main.py
└── api.py               # Wrapper para classificação reutilizável
    ├── classify_tournament_text()
    └── classify_file()
```

### Garantias de Compatibilidade

- ✅ Interface web continua idêntica
- ✅ Mesmas 3 pastas de saída (PKO, NON-KO, MYSTERIES)
- ✅ Mesmos nomes de ficheiros compilados
- ✅ Mesma lógica de classificação por regex
- ✅ Teste de regressão garante 100% compatibilidade

## 📦 Dependências

```bash
pip install flask chardet rarfile python-magic gunicorn
```

## 🧪 Testes

Executar teste de regressão para garantir compatibilidade:

```bash
python tests/test_regression.py
```

O teste verifica que a distribuição de ficheiros por pasta é idêntica entre a versão antiga e nova.

## 📝 Notas de Desenvolvimento

- Ficheiros "unknown" (vazios/binários) vão para NON-KO mas aparecem como "unknown" no manifesto
- Suporta arquivos aninhados até 5 níveis de profundidade
- Processa apenas ficheiros .txt e .xml
- Limite de upload web: 50MB direto, até 500MB com chunked upload

## 🎯 Módulo de Parsing de Mãos (NOVO)

### Visão Geral

Sistema completo para extrair dados estruturados de históricos de mãos de poker. Suporta múltiplas salas (PokerStars, GGPoker, WPN, Winamax, 888poker) com detecção automática de formato.

### Estrutura do Parser

O sistema agora inclui um módulo completo de parsing de históricos de mãos de poker:

```
app/parse/
├── schemas.py           # Modelos Pydantic (Hand, Player, Action)
├── interfaces.py        # Protocol SiteParser
├── utils.py            # Helpers de texto e regex
├── site_generic.py     # Parser genérico com funções robustas
├── site_pokerstars.py  # Parser PokerStars
├── site_gg.py         # Parser GGPoker
├── site_wpn.py        # Parser WPN/ACR
├── site_winamax.py    # Parser Winamax
├── site_888.py        # Parser 888poker
└── runner.py          # Orquestrador principal
```

### Funcionalidades do Parser

#### Delimitação Robusta de Mãos
- Detecta início de mãos por múltiplos padrões:
  - Headers específicos: `PokerStars Hand #`, `Poker Hand #`, `Winamax Poker`
  - Marcadores de street: `*** HOLE CARDS ***`, `*** FLOP ***`
  - Fallback para tabelas: `Table '...' 9-max`, `Seat 1:`

#### Extração de Offsets
- Localiza posições exatas de cada seção no texto
- Suporta click-through para texto original
- Identifica: HOLE CARDS, FLOP, TURN, RIVER, SHOWDOWN, SUMMARY

#### Estrutura de Dados
```python
from app.parse import Hand, parse_file

# Parse arquivo
hands = parse_file('torneio.txt')

# Cada Hand contém:
# - site: 'pokerstars', 'gg', 'wpn', etc.
# - tournament_id: ID do torneio
# - players: Lista de jogadores com stacks
# - streets: Ações por street (preflop, flop, turn, river)
# - raw_offsets: Posições no texto original
```

### Uso do Parser

```python
# Import direto
from app.parse import parse_file, parse_directory

# Parse arquivo único
hands = parse_file('pokerstars_tournament.txt')
print(f'Parsed {len(hands)} hands')

# Parse diretório completo
results = parse_directory('./PKO')
for filename, hands in results.items():
    for hand in hands:
        print(f'Hand #{hand.tournament_id}: {len(hand.players)} players')
```

### Configuração do Hero

O arquivo `app/config/hero_aliases.json` define os nomes do hero por sala:

```json
{
  "global": ["MyUsername", "AlternativeName"],  // Aplicado a todas as salas
  "pokerstars": ["PSUsername"],                  // Específico do PokerStars
  "gg": ["GGPokerName"],                        // Específico do GGPoker
  "wpn": ["WPNUsername"],                       // Winning Poker Network
  "winamax": ["WinamaxUser"],                   // Winamax
  "888": ["888User"]                            // 888poker
}
```

**Como editar:**
1. Crie ou edite `app/config/hero_aliases.json`
2. Adicione seus usernames na seção `global` (vale para todas as salas)
3. Adicione usernames específicos por sala se necessário
4. O parser detecta automaticamente quando você é o hero baseado em:
   - Padrão "Dealt to [nome]" no texto
   - Match com os aliases configurados

### Como Usar o Parser

#### 1. CLI Direta
```bash
# Processar pastas classificadas (PKO/, non-KO/, mystery/)
python -m app.parse.runner --in ./CLASSIFIED --out ./parsed/hands.jsonl --aliases ./app/config/hero_aliases.json

# Com log detalhado
python -m app.parse.runner --in ./CLASSIFIED --out ./output.jsonl --verbose

# Exemplo de output:
# Files processed: 150
# Hands extracted: 7500
# By site: pokerstars: 5000, gg: 2500
# By folder: PKO: 4000, non-KO: 3500
```

#### 2. API REST
```bash
# Endpoint POST /api/parse - processa diretório classificado
curl -X POST http://localhost:5000/api/parse \
  -H "Content-Type: application/json" \
  -d '{
    "classified_dir": "./CLASSIFIED",
    "aliases": {
      "global": ["MyHeroName"],
      "pokerstars": ["MyPSName"]
    }
  }'
```

Retorna:
```json
{
  "success": true,
  "run_id": "abc123",
  "output_file": "parsed/abc123/hands.jsonl",
  "stats": {
    "files": 100,
    "hands": 5000,
    "by_site": {"pokerstars": 3000, "gg": 2000},
    "by_folder": {"PKO": 3000, "non-KO": 2000},
    "errors": [],
    "timestamp": "2025-09-03T11:30:00"
  }
}
```

#### 3. Integração Automática no Upload
```bash
# Via variável de ambiente
export ENABLE_PARSER=true
curl -X POST http://localhost:5000/upload -F "file=@torneios.zip"

# Via parâmetro do form (backoffice)
curl -X POST http://localhost:5000/upload \
  -F "file=@torneios.zip" \
  -F "enable_parser=true"
```

Quando ativo, o ZIP de resultado inclui:
- `parsed/hands.jsonl` - Todas as mãos em formato JSONL
- `parsed/parse_stats.json` - Estatísticas do parsing
- `parsed/parse_errors.log` - Erros críticos (mãos sem button/preflop)

### Estrutura do hands.jsonl (JSONL)

Cada linha do arquivo `hands.jsonl` é um objeto JSON independente representando uma mão completa:

```json
{
  "site": "pokerstars",
  "tournament_id": "123456789",
  "file_id": "PKO/torneio_01.txt",
  "button_seat": 3,
  "hero": "MyUsername",
  "players": [
    {"seat": 1, "name": "Player1", "stack_chips": 1500.0}
  ],
  "players_dealt_in": ["Player1", "MyUsername", "Player3"],
  "streets": {
    "preflop": {
      "actions": [
        {"type": "POST_SB", "actor": "Player1", "amount": 10.0},
        {"type": "POST_BB", "actor": "Player2", "amount": 20.0},
        {"type": "RAISE", "actor": "MyUsername", "amount": 60.0},
        {"type": "FOLD", "actor": "Player1"},
        {"type": "CALL", "actor": "Player2", "amount": 60.0}
      ]
    },
    "flop": {
      "actions": [...],
      "board": ["Kh", "7d", "2c"]
    }
  },
  "any_allin_preflop": false,
  "players_to_flop": 2,
  "heads_up_flop": true,
  "raw_offsets": {
    "hand_start": 0,
    "hole_cards": 245,
    "flop": 380,
    "turn": 450,
    "river": 520,
    "summary": 600,
    "hand_end": 750
  }
}
```

**Campos principais:**
- `site`: Sala detectada (pokerstars, gg, wpn, winamax, 888)
- `hero`: Nome do hero se detectado
- `players_dealt_in`: Lista de jogadores que receberam cartas
- `streets`: Ações organizadas por street (preflop, flop, turn, river)
- `actions`: Lista de ações com tipo normalizado (FOLD, CHECK, CALL, BET, RAISE, ALLIN)
- `any_allin_preflop`: Se houve all-in no pré-flop
- `players_to_flop`: Quantidade de jogadores que viram o flop
- `heads_up_flop`: Se o flop foi heads-up (2 jogadores)
- `raw_offsets`: Posições no arquivo original para click-through futuro

### Limitações Atuais (Fase 3 Pendente)

1. **Posições relativas não implementadas**: 
   - Classificação EP (Early Position), MP (Middle Position), LP (Late Position)
   - Identificação automática de CO, BTN, SB, BB baseada em quantidade de jogadores
   - Está planejado para Fase 3 do desenvolvimento

2. **Pot size tracking**: 
   - Cálculo incremental do pot por street não implementado
   - Rake e side pots ainda não processados

3. **Hand winners**: 
   - Parsing do vencedor final parcialmente implementado
   - Showdown com múltiplos jogadores ainda em desenvolvimento

4. **Multi-table**: 
   - Cada arquivo é processado independentemente
   - Não há correlação entre mesas do mesmo torneio

5. **Formatos especiais**: 
   - Torneios Zoom/Rush podem ter formato não reconhecido
   - Bounty values em PKOs ainda não extraídos

### Logs e Debugging

O parser gera logs detalhados:
- **Por arquivo**: Número de mãos, % all-in preflop, distribuição HU/MW
- **parse_errors.log**: Mãos com erros críticos (sem button, sem ações preflop)
- **Estatísticas**: Total por sala, por pasta (PKO/non-KO/mystery)

## 📊 Fase 4 — Partições

Sistema de particionamento de mãos para organização mensal e agrupamento por estratégia.

### CLI

```bash
python -m app.partition.runner --in parsed/hands_enriched.jsonl --out partitions/
```

### Endpoints

```bash
# Construir partições
POST /api/partition 
{ "in_jsonl": "...", "out_dir": "partitions", "validate": true }

# Obter contagens
GET /api/partition/counts?path=partitions/partition_counts.json

# Debug info
GET /api/partition/debug?counts_path=partitions/partition_counts.json

# Validação standalone
POST /api/partition/validate
{ "counts_path": "...", "hands_jsonl": "..." }
```

### Validador

```python
from app.partition.validator import validate_partitions

# Validação simples
result = validate_partitions("partitions/partition_counts.json", "parsed/hands_enriched.jsonl")

# Validação com resumo
from app.partition.validator import validate_with_summary
result = validate_with_summary("partitions/partition_counts.json", "parsed/hands_enriched.jsonl")
```

### Estrutura de Saída

```
partitions/
  partition_counts.json         # Contagens por mês × grupo
  nonko_combined.json          # Somatório NON-KO por mês
  validation_report.json       # Relatório de integridade
  index/
    2025-06__nonko_9max_pref.ids   # IDs de mãos NON-KO 9max
    2025-06__nonko_6max_pref.ids   # IDs de mãos NON-KO 6max
    2025-06__pko_pref.ids          # IDs de mãos PKO
    2025-06__postflop_all.ids     # IDs de mãos que viram flop
```

### Grupos de Particionamento

- **nonko_9max_pref**: Mãos NON-KO em mesas 9-max, pré-flop only
- **nonko_6max_pref**: Mãos NON-KO em mesas 6-max, pré-flop only
- **pko_pref**: Mãos PKO (qualquer formato), pré-flop only
- **postflop_all**: Todas as mãos que viram flop (qualquer tipo)

### Formato dos Arquivos

**partition_counts.json**:
```json
{
  "input": "parsed/hands_enriched.jsonl",
  "totals": {
    "nonko_9max_pref": 5000,
    "nonko_6max_pref": 3000,
    "pko_pref": 4000,
    "postflop_all": 8000
  },
  "counts": {
    "2025-01": {
      "nonko_9max_pref": {"hands": 500},
      "pko_pref": {"hands": 400}
    }
  }
}
```

**nonko_combined.json**:
```json
{
  "2025-01": {
    "hands_nonko_9max_pref": 500,
    "hands_nonko_6max_pref": 300,
    "hands_nonko_pref_total": 800
  }
}
```

**validation_report.json**:
```json
{
  "counts_path": "partitions/partition_counts.json",
  "hands_jsonl": "parsed/hands_enriched.jsonl",
  "differences": [],
  "ok": true,
  "summary": {
    "validation_status": "PASSED",
    "total_months": 12,
    "total_hands_in_counts": 20000,
    "groups_with_data": ["nonko_9max_pref", "pko_pref", "postflop_all"]
  }
}
```

## 📈 Fase 5 — DSL & Executor

Sistema de cálculo de estatísticas usando DSL (Domain Specific Language) para definir métricas de poker.

### Editando o Catálogo DSL

O arquivo `app/stats/dsl/stats.yml` define as estatísticas calculadas:

```yaml
stats:
  - id: RFI_EARLY
    label: "Early RFI"
    family: "RFI"
    applies_to_groups: ["nonko_9max_pref", "nonko_6max_pref"]
    filters:
      heads_up_only: true      # HU à entrada do flop
      pot_type: ["SRP"]
      eff_stack_min_bb: 16
    opportunity:
      all:
        - eq: ["hero_pos_group", "EP"]
        - is_true: "unopened_pot"
    attempt:
      is_true: "hero_raised_first_in"
```

**Operadores suportados:**
- `eq`: Igualdade (`["campo", "valor"]`)
- `gte/lte/gt/lt`: Comparações numéricas
- `is_true/is_false`: Booleanos
- `all/any`: Combinação lógica
- `in`: Pertence a lista

### CLI de Execução

```bash
# Calcular estatísticas
python -m app.stats.runner --in parsed/hands_enriched.jsonl --out stats/ -v

# Com DSL customizado
python -m app.stats.runner --in hands.jsonl --dsl custom_stats.yml --out output/
```

### Endpoints API

```bash
# 1. Construir estatísticas
POST /api/stats/build
{
  "in_jsonl": "parsed/hands_enriched.jsonl",
  "dsl_path": "app/stats/dsl/stats.yml",
  "out_dir": "stats"
}

# 2. Obter sumário
GET /api/stats/summary?path=stats/stat_counts.json

# 3. Obter hand IDs para click-through
GET /api/stats/hands?month=2025-06&group=nonko_9max_pref&stat=RFI_EARLY&type=opps
```

### Estrutura de Output

```
stats/
├── stat_counts.json          # Manifest com percentagens
├── stats_errors.log          # Erros de processamento (se houver)
└── index/
    ├── 2025-06__nonko_9max_pref__RFI_EARLY__opps.ids      # IDs das oportunidades
    └── 2025-06__nonko_9max_pref__RFI_EARLY__attempts.ids  # IDs das tentativas
```

**Formato do stat_counts.json:**
```json
{
  "generated_at": "2025-09-03T14:30:00Z",
  "hands_processed": 1000,
  "counts": {
    "2025-06": {
      "nonko_9max_pref": {
        "RFI_EARLY": {
          "opportunities": 50,
          "attempts": 25,
          "percentage": 50.0,
          "index_files": {
            "opps": "index/2025-06__nonko_9max_pref__RFI_EARLY__opps.ids",
            "attempts": "index/2025-06__nonko_9max_pref__RFI_EARLY__attempts.ids"
          }
        }
      }
    }
  }
}
```

### Notas Importantes

- **`heads_up_flop`**: Indica HU à entrada do flop (exclui multiway automaticamente)
- **`raw_offsets`**: Permite click-through para texto original (implementação futura)
- **Grupos aplicáveis**: Stats só calculam para grupos definidos em `applies_to_groups`
- **Filtros cumulativos**: Todos os filtros devem passar para contar como oportunidade

## 🚀 Próximas Fases

O código foi estruturado para facilitar:
- ✅ Parsing de mãos de múltiplos sites
- ✅ Extração de ações e offsets
- ✅ Particionamento mensal e por grupos estratégicos
- ✅ Validação de integridade de partições
- ✅ DSL para definição de estatísticas
- Integração com pipelines de ML
- Processamento batch automatizado
- APIs REST para classificação e parsing
- Análise estatística dos manifestos e mãos

---
*Última atualização: Setembro 2025 - Adicionado módulo completo de parsing*