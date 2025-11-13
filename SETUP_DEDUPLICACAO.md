# 🚀 Sistema de Deduplicação e Armazenamento de Ficheiros

## ✅ O Que Foi Implementado

### 1. Deduplicação Automática
- **Detecção de ficheiros duplicados** usando hash SHA256
- **Reutilização de resultados** - se o ficheiro já foi processado, retorna os resultados antigos
- **Poupança de recursos** - não processa o mesmo ficheiro duas vezes
- **Verificação por utilizador** - cada utilizador tem o seu próprio histórico

### 2. Armazenamento de Ficheiros no Supabase
- **Upload automático** dos ficheiros originais para Supabase Storage
- **Organização por utilizador** - ficheiros guardados em `uploads/{user_id}/{token}/`
- **Permanência** - ficheiros sempre disponíveis para re-download

### 3. Re-acesso a Dashboards Antigos
- **Links diretos** para dashboards de uploads anteriores
- **Sem re-upload** - acede a análises antigas instantaneamente
- **Histórico completo** - todos os uploads guardados com metadados

## 📋 Passos para Ativar o Sistema

### Passo 1: Atualizar Schema da Base de Dados

Execute este SQL no **SQL Editor do Supabase**:

```sql
-- Adicionar colunas para deduplicação e storage
ALTER TABLE processing_history 
ADD COLUMN IF NOT EXISTS file_hash VARCHAR(64);

ALTER TABLE processing_history 
ADD COLUMN IF NOT EXISTS storage_path VARCHAR(500);

-- Criar índice para pesquisa rápida
CREATE INDEX IF NOT EXISTS idx_processing_history_file_hash 
ON processing_history(file_hash);

-- Comentários
COMMENT ON COLUMN processing_history.file_hash IS 'SHA256 hash of uploaded file for deduplication';
COMMENT ON COLUMN processing_history.storage_path IS 'Path to original file in Supabase Storage';
```

**Ou simplesmente:**
1. Abra o ficheiro `scripts/supabase_schema_update_hash.sql`
2. Copie todo o conteúdo
3. Cole no SQL Editor do Supabase
4. Clique em "Run"

### Passo 2: Criar Bucket no Supabase Storage

1. Aceda ao seu projeto Supabase
2. Menu lateral → **Storage**
3. Clique em **"New bucket"**
4. Nome do bucket: `poker-uploads`
5. **Desmarque** "Public bucket" (manter privado)
6. Clique em **"Create bucket"**

### Passo 3: Reiniciar a Aplicação

A aplicação já está configurada! Basta reiniciar para ativar as novas funcionalidades.

## 🎯 Como Funciona

### Fluxo de Upload Normal (Ficheiro Novo)

1. Utilizador faz upload de `hands.zip`
2. Sistema calcula hash SHA256: `a3b5c7...`
3. Verifica na base de dados: **não encontrado**
4. ✅ Processa o ficheiro normalmente
5. 📤 Upload do ficheiro original para Supabase Storage
6. 💾 Guarda resultados + hash + caminho no Supabase
7. Retorna dashboard com resultados

### Fluxo de Upload Duplicado

1. Utilizador faz upload do **mesmo** `hands.zip`
2. Sistema calcula hash SHA256: `a3b5c7...`
3. Verifica na base de dados: **✅ encontrado!**
4. 🔄 Retorna resultados do processamento anterior
5. Mensagem: "Ficheiro já processado anteriormente!"
6. **Não processa novamente** - poupa tempo e recursos

## 💡 Benefícios

✅ **Não Perde Tempo** - Ficheiros duplicados retornam resultados instantaneamente
✅ **Poupa Recursos** - Não processa o mesmo ficheiro duas vezes
✅ **Histórico Completo** - Todos os uploads guardados permanentemente
✅ **Re-acesso Fácil** - Links diretos para dashboards antigos
✅ **Ficheiros Seguros** - Originais guardados no Supabase Storage
✅ **Organizado** - Ficheiros organizados por utilizador

## 📊 Dados Guardados

Para cada upload, o sistema guarda:
- **File Hash (SHA256)**: Identificador único do ficheiro
- **Storage Path**: Localização no Supabase Storage
- **Metadados**: Utilizador, data, tamanho, nome
- **Resultados Completos**: Todas as estatísticas geradas
- **Link para Dashboard**: Token para re-aceder

## 🔍 Exemplo Prático

### Primeira vez:
```
Upload: pokerstars_2024.zip
Hash: a3b5c7d9e2f4...
Status: Novo ficheiro
Ação: ✅ Processar (15 segundos)
Resultado: Dashboard gerado
```

### Segunda vez (mesmo ficheiro):
```
Upload: pokerstars_2024.zip
Hash: a3b5c7d9e2f4...
Status: ⚡ Duplicado detectado!
Ação: 🔄 Reutilizar resultados
Resultado: Dashboard retornado instantaneamente (<1 segundo)
```

## 🛠️ Ficheiros Criados

- **app/services/file_hash.py** - Cálculo de hash SHA256
- **app/services/supabase_storage.py** - Upload/download de ficheiros
- **app/services/supabase_history.py** - Atualizado com deduplicação
- **app/api/simple_upload.py** - Atualizado com verificação de duplicados
- **scripts/supabase_schema_update_hash.sql** - Update do schema

## ⚙️ Configuração Técnica

### Algoritmo de Hash
- **SHA256** - Seguro e amplamente usado
- **Único** - Probabilidade de colisão praticamente zero
- **Rápido** - Processa grandes ficheiros em segundos

### Verificação de Duplicados
- Busca por `file_hash` + `user_id`
- Apenas ficheiros com `status='completed'`
- Retorna o processamento mais recente

### Armazenamento
- **Bucket privado** no Supabase Storage
- **Organização**: `uploads/{user_id}/{token}/{filename}`
- **Acesso seguro** - apenas o utilizador dono pode aceder

## 🎉 Pronto para Usar!

Após executar os passos acima, o sistema estará totalmente funcional:
- ✅ Deduplicação automática ativa
- ✅ Ficheiros guardados no Supabase
- ✅ Re-acesso a dashboards antigos
- ✅ Histórico completo disponível

---

**Última atualização**: 12 de Novembro de 2025
**Status**: ✅ Implementado e pronto para usar
