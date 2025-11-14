# Fase 3 – Dashboard Global e Mensal

Este documento descreve a solução proposta para suportar o modo global e o modo mensal utilizando uma única base de mãos válidas por upload. O objetivo é garantir que o payload do dashboard seja sempre produzido pela mesma função de agregação, independentemente do subconjunto de mãos escolhido (global, mês específico ou "mês desconhecido"), mantendo os mesmos cálculos e downloads utilizados atualmente.

## Visão Geral

1. **Fonte única de dados** – Após o parse dos ficheiros de hand history é criada uma base de dados de mãos válidas (`valid_hands.db`). Cada registo contém:
   - `hand_id` (hash estável da mão crua);
   - `timestamp_utc` normalizado para ISO 8601;
   - `month_bucket` (`YYYY-MM` ou `unknown` quando a data não é parseável);
   - `site`, `tournament_type`, `table_format`, `group` (nonko_9max, nonko_6max, pko, postflop);
   - contribuição por estatística (oportunidade/attempt) para todos os stats pré-flop e pós-flop;
   - referências às amostras gravadas em `hands_by_stat` (ficheiro + offsets).

   A base é gravada em disco dentro de `work/<token>/valid_hands.parquet` (versão compacta) e replicada para storage remoto juntamente com o resto dos artefactos do upload.

2. **Função genérica de agregação** – Introduz-se `build_dashboard_from_hands(hands_subset, artifacts)`:
   ```python
   def build_dashboard_from_hands(hands_subset: Iterable[HandRecord], artifacts: DashboardArtifacts) -> dict:
       ...
   ```
   - Recebe um iterável de `HandRecord` (tipagem pydantic/dataclass) e uma estrutura `DashboardArtifacts` com ponteiros para scorecard, configurações de pesos e metadados de downloads.
   - Reconstrói os contadores de oportunidades/tentativas por stat utilizando os contributos guardados por mão.
   - Agrupa resultados por grupo lógico (NON-KO 9-max, NON-KO 6-max, PKO, POSTFLOP).
   - Calcula percentagens, scores, notas e pesos através dos módulos existentes (`ScoringCalculator`, `build_scorecard`, `calculate_weighted_scores_from_groups`).
   - Gera a secção de downloads cruzando os `hand_id` do subset com os ficheiros em `hands_by_stat`.
   - Devolve sempre o mesmo payload JSON já consumido pelo frontend.

3. **Reutilização nos endpoints** – O endpoint Flask passa a carregar a base única de mãos, filtrar e delegar na função genérica:
   ```python
   all_hands = hand_repository.load(token)
   if month_param is None:
       payload = build_dashboard_from_hands(all_hands, artifacts)
   else:
       subset = [h for h in all_hands if h.month_bucket == month_param]
       payload = build_dashboard_from_hands(subset, artifacts)
   ```
   - `/dashboard/<token>` usa o conjunto completo.
   - `/dashboard/<token>?month=YYYY-MM` filtra pelo mês desejado.
   - `/dashboard/<token>?month=unknown` devolve as mãos sem timestamp válido.

4. **Distribuição de meses** – A mesma base alimenta uma função utilitária:
   ```python
   def hands_per_month(hands: Iterable[HandRecord]) -> dict[str, int]:
       counter = Counter(hand.month_bucket for hand in hands)
       return dict(counter)
   ```
   - Utilizada para preencher o dropdown de meses no payload.
   - Validada contra `len(all_hands)` antes de gerar o dashboard para garantir que `sum(hands_per_month.values()) == total_valid_hands`.

## Componentes Técnicos

### 1. Repositório de mãos (`app/hands/repository.py`)

Responsável por persistir e carregar a base única:

- **Persistência**: durante o pipeline, depois de `HandCollector` produzir as amostras, consolidamos os contributos por mão e gravamos o ficheiro Parquet. Estrutura simplificada:
  ```python
  @dataclass
  class HandRecord:
      hand_id: str
      timestamp_utc: Optional[str]
      month_bucket: str
      site: str
      tournament_type: str
      table_format: str
      group: str
      stats: dict[str, StatContribution]
      samples: dict[str, SampleRef]
  ```
  `StatContribution` guarda `opportunity: bool`, `attempt: bool` e ponteiro opcional para o ficheiro das amostras.

- **Carregamento**: interface `load(token: str) -> Iterable[HandRecord]` que usa Parquet/Arrow para streams eficientes. Fornece também `iter_month(token, month)` que devolve um gerador filtrado.

### 2. Consolidação dos contributos por mão

Os módulos `PreflopStats` e `PostflopCalculatorV3` já dispõem do contexto da mão quando incrementam oportunidades/tentativas. A implementação adiciona um `StatContributionCollector` opcional que recebe `hand_id`, `stat_name`, `is_attempt` e guarda no registo atual. O fluxo é:

1. Antes de analisar a mão, `StatContributionCollector.start_hand(hand_context)` inicializa o buffer.
2. Sempre que uma oportunidade/attempt é detetada, chama `collector.record(stat_name, opportunity=True/False, attempt=True/False)`.
3. No final da mão, `collector.finalize()` devolve o dicionário consolidado a quem chamou (pipeline de agregação), que depois é anexado ao `HandRecord` juntamente com metadados (site, timestamp, grupo).

### 3. Artefactos partilhados (`DashboardArtifacts`)

Estrutura carregada uma única vez por request a partir de `work/<token>` ou storage remoto:

- `score_config` (`app/score/config.yml`);
- `scorecard` (para notas e ideais);
- `hands_by_stat_index`: mapa `{stat_name: {group: SampleFileInfo}}` contendo caminhos e total de mãos.

Permite que `build_dashboard_from_hands` gere notas, pesos e URLs de download sem tocar no disco repetidamente.

### 4. Função de agregação

Passos internos:

1. **Filtragem e sanidade** – remove mãos cujo `group` não esteja na lista esperada. Se o subset estiver vazio devolve payload vazio com `has_data=False` para todos os grupos.
2. **Cálculo dos contadores** – para cada mão e para cada `StatContribution`, acumula oportunidades/tentativas numa estrutura `group -> stat -> {'opportunities': int, 'attempts': int}`. Este passo substitui a leitura direta de `stat_counts.json`.
3. **Scores e notas** – utiliza os pesos e ideais carregados de `scorecard` para calcular `percentage`, `score`, `ideal` e `overall_score` por grupo. O mesmo cálculo é usado no modo global e mensal.
4. **Subgrupos Postflop** – agrega contributos marcados como `group == 'postflop_all'` para construir `postflop_all` com a mesma lógica do modo global.
5. **Downloads** – cruza os `hand_id` do subset com `hands_by_stat_index` para contar amostras disponíveis por stat e construir as URLs (`/api/download/hands_by_stat/<token>/<group>/<file>?month=...`).
6. **Payload final** – monta o dicionário consumido pelo frontend (`overall`, `groups`, `weighted_scores`, `months`, `hands_per_month`, `discard_stats`, etc.).

### 5. Endpoint `/dashboard/<token>`

Atualizações principais:

- Carrega a base única uma vez por request.
- Calcula `hands_per_month` e inclui no payload (`dropdown.months`, `month_summary`).
- Chama `build_dashboard_from_hands` com o subset adequado.
- Mantém compatibilidade com os dados legados (progress tracker, manifest, storage uploads).

### 6. Endpoint `/dashboard/<token>/months`

Passa a usar diretamente `hands_per_month(hands)` para devolver a lista de meses disponíveis, ordenados e com `unknown` no final.

## Garantias de Integridade

- **Total de mãos** – após carregar as mãos, o pipeline valida `len(all_hands)` contra `pipeline_result['valid_hands']` e o contador de descarte (`total = valid + discarded`).
- **Distribuição mensal** – `sum(hands_per_month.values())` deve ser igual a `len(all_hands)`. Qualquer divergência dispara `AssertionError` durante o pipeline.
- **Consistência de downloads** – os ficheiros em `hands_by_stat` continuam a ser gerados no pipeline original. A camada de dashboard apenas referencia estes ficheiros; não há duplicação.

## Roadmap de Implementação

1. Instrumentar `PreflopStats` e `PostflopCalculatorV3` com `StatContributionCollector`.
2. Criar `HandRecord` + repositório Parquet e garantir que o pipeline grava o ficheiro por upload.
3. Implementar `build_dashboard_from_hands` reutilizando as funções de cálculo existentes.
4. Atualizar endpoints `/dashboard` e `/dashboard/<token>/months`.
5. Ajustar testes para cobrir cenários global, mensal específico e `unknown`.

Esta abordagem garante:
- Uma única fonte de verdade para as mãos válidas.
- Mesma lógica de filtros, scores e downloads entre global e mensal.
- Facilidade para adicionar novos filtros (por exemplo, stacked filters) ao reusar `HandRecord` como unidade básica.
