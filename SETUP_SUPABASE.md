# Configuração do Supabase para Histórico de Processamentos

## Passo 1: Criar as Tabelas no Supabase

1. Aceda ao seu projeto Supabase: https://fmudwmmpkqiqwrduzkoc.supabase.co

2. No menu lateral, clique em **SQL Editor**

3. Clique em "New query"

4. Copie e cole o conteúdo completo do ficheiro: `scripts/supabase_schema.sql`

5. Clique em **Run** para executar o SQL

6. Verifique que as tabelas foram criadas:
   - Vá para **Table Editor** no menu lateral
   - Deverá ver as tabelas:
     - `processing_history`
     - `poker_stats_detail`

## Passo 2: Verificar as Credenciais

As credenciais já estão configuradas como secrets no Replit:
- ✅ SUPABASE_URL: https://fmudwmmpkqiqwrduzkoc.supabase.co
- ✅ SUPABASE_KEY: (configurada como secret)

## Passo 3: Testar a Integração

Após configurar as tabelas:

1. Faça um upload de teste na aplicação
2. O histórico será automaticamente guardado no Supabase
3. Aceda a `/api/history/my` para ver o seu histórico
4. Verifique as tabelas no Supabase para confirmar que os dados foram guardados

## Estrutura das Tabelas

### processing_history
Guarda informação geral de cada processamento:
- `token`: Identificador único
- `user_id`: Email do utilizador
- `filename`: Nome do ficheiro processado
- `total_hands`: Total de mãos processadas
- `total_sites`: Número de sites detectados
- `overall_score`: Pontuação geral
- `sites_processed`: Lista de sites (JSON)
- `full_result`: Resultado completo (JSON)

### poker_stats_detail
Guarda estatísticas detalhadas:
- `processing_id`: Referência ao processamento
- `site`: Nome do site de poker
- `table_format`: Formato da mesa (ex: nonko_9max)
- `stat_name`: Nome da estatística (ex: "Early RFI")
- `opportunities`: Número de oportunidades
- `attempts`: Número de tentativas
- `percentage`: Percentagem calculada

## Benefícios

✅ **Histórico Persistente**: Todos os uploads ficam guardados permanentemente
✅ **Consulta Rápida**: Acesso instantâneo a processamentos anteriores
✅ **Estatísticas Agregadas**: Visão geral de todos os uploads por utilizador
✅ **Espaço Libertado**: Ficheiros locais podem ser apagados sem perder dados
