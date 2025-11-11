# Sistema de Autenticação

Este documento explica em detalhe como o sistema de autenticação está implementado no projeto, desde a inicialização até ao fluxo de sessão, gestão de perfis, permissões e recuperação de palavra‑passe.

---
## Visão Geral
O projeto utiliza **Supabase Auth** (flow PKCE) + **Pinia** para gestão de estado. Há separação entre:
- Credenciais e sessão (mantidas pelo Supabase)
- Perfil de utilizador (tabela `users`, enriquecida com campos de negócio: `nickname`, `type`, `team`, `profit_share`, saldos e makeup)

A sessão autenticada é mantida no `localStorage` (chaves `sb-<project>-auth-token`) e auto‑refrescada pelo Supabase (`autoRefreshToken: true`). O store `auth` orquestra a sincronização entre Supabase e o perfil na BD.

---
## Componentes Principais
| Componente | Ficheiro | Responsabilidade |
|------------|----------|------------------|
| Cliente Supabase | `src/services/supabase.js` | Cria cliente principal e (opcional) cliente admin (Service Role) |
| API Auth Wrapper | `src/services/api.js` (`authAPI`) | Abstrai chamadas de login, signup, logout, reset/update password |
| API Users Wrapper | `src/services/api.js` (`usersAPI`) | CRUD de perfis na tabela `users` |
| Store de Autenticação | `src/stores/auth.js` | Estado reativo (utilizador, perfil, sessão), fluxos de login/logout/init |
| Guardas de Rotas | `src/router/index.js` | Protege rotas com `requiresAuth` e `requiresAdmin` |
| Inicialização Global | `src/App.vue` | Chama `initializeAuth()` no mount |

---
## Estrutura de Dados do Perfil (`users`)
Campos relevantes: `id` (igual ao auth user id), `email`, `first_name`, `last_name`, `nickname`, `type` (ex: `player`, `admin`), `team`, `profit_share`, `makeup_eur`, `makeup_usd`, `initial_balance_eur`, `initial_balance_usd`, `total_balance`, `status`.

O campo `type` define permissões elevadas (admin). O cálculo de saldos e makeup é feito noutros serviços mas refletido aqui para dashboards.

---
## Fluxo de Inicialização
1. `App.vue` chama `authStore.initializeAuth()` no `onMounted`.
2. `initializeAuth()`:
   - Chama `supabase.auth.getSession()`.
   - Se existir `currentSession.user`: armazena `user` e `session` no store e invoca `fetchUserProfile(user.id)`.
   - Caso contrário: `clearAuthState()`.
   - Marca `initialized = true` para que o router não bloqueie indefinidamente.
3. Paralelamente, há um listener `supabase.auth.onAuthStateChange` que mantém sincronização em eventos: `SIGNED_IN`, `SIGNED_OUT`, `TOKEN_REFRESHED`.

### Timeouts / Espera no Router
O router (`beforeEach`) espera até 5s (`MAX_AUTH_WAIT_TIME`) pelo `initialized`. Se exceder, prossegue para evitar lock.

---
## Fluxo de Login (Sign In)
1. UI recolhe `email` + `password` e chama `authStore.signIn(email, password)`.
2. `signIn` -> `authAPI.signIn` -> `supabase.auth.signInWithPassword`.
3. Resposta contém `{ user, session }`:
   - Guarda em `user` e `session`.
   - `fetchUserProfile(user.id)` obtém perfil na tabela `users`.
   - Se não existir perfil, fluxos posteriores criam (ver abaixo).
4. Retorna `{ success: true }` à UI.

### Erros
- Erros de Supabase passam por `handleApiError` que devolve `error.message` ou mensagem genérica.

---
## Fluxo de Registo (Sign Up)
1. UI chama `authStore.signUp(email, password, metadata)`.
2. `authAPI.signUp` usa `supabase.auth.signUp` com `options.data = metadata` (armazenado em `auth.users.user_metadata`).
3. Após sucesso: atribui `user`, `session` e constrói `profileData` com defaults (ex: `type: 'player'`).
4. Tenta inserir perfil (`usersAPI.createUserProfile`). Se falhar (condição de corrida ou duplicado), volta a `fetchUserProfile`.
5. Retorna `{ success: true }`.

### Observações
- O email redirect (emailRedirectTo) está `undefined` — o fluxo atual não exige verificação de email para avançar.
- Possível melhoria: ativar confirmação de email para camadas extra de segurança.

---
## Criação Automática de Perfil
`fetchUserProfile`:
1. Faz `select * from users where id = userId`.
2. Se encontrar: normaliza campos `makeup_eur`, `makeup_usd` (garante 0 se nulos) e guarda em `userProfile`.
3. Se não encontrar: tenta criar perfil default.
4. Em caso de erro por `duplicate key`: refaz fetch (condição de corrida mitigada).
5. Se tudo falhar: cria perfil fallback mínimo para não bloquear a UI.

---
## Logout (Sign Out)
1. UI chama `authStore.signOut()`.
2. `authAPI.signOut` -> `supabase.auth.signOut({ scope: 'global' })` (remove refresh tokens em todos os dispositivos com esse refresh token).
3. `clearAuthState()` limpa `user`, `userProfile`, `session` e remove chaves `sb-*auth-token` do `localStorage`.

---
## Refresh de Sessão
- Gerido automaticamente pelo Supabase (`autoRefreshToken: true`).
- Evento `TOKEN_REFRESHED` atualiza `user` e `session` no store.
- O store não revalida o perfil a cada refresh (poupa chamadas). Se necessário, chamar manualmente `refreshUserData()`.

---
## Atualização de Perfil
- Método `updateProfile(updates)` chama `usersAPI.updateUserProfile` e substitui `userProfile`.
- Não mexe em credenciais (isso é separado, via `updatePassword`).

---
## Recuperação e Reset de Password
1. `forgotPassword(email)` -> `authAPI.resetPassword` -> `supabase.auth.resetPasswordForEmail(email, { redirectTo })`.
2. O utilizador recebe email com link (redirect definido via `getPasswordResetUrl()`).
3. No front, a rota `ResetPassword` permite definir nova password via `resetPassword(newPass)` -> `supabase.auth.updateUser({ password })`.

---
## Guardas de Rotas & Permissões
- `requiresAuth`: redireciona para `Login` se `!isAuthenticated`.
- `requiresAdmin`: redireciona para `Dashboard` se `!isAdmin`.
- Rotas de autenticação (`Login`, `Register`, `ForgotPassword`) redirecionam para `Dashboard` se já autenticado.
- Rota pública adicional: `ResetPassword`.

### Determinação de Admin
`isAdmin = userProfile.type === 'admin'`. (Nota: no código atual era `userProfile?.type === 'admin'`; confirmar consistência se existirem outros papéis.)

---
## Estados Reativos Principais
| Campo | Descrição |
|-------|-----------|
| `user` | Objeto auth retornado pelo Supabase (id, email, metadata) |
| `session` | Tokens e info de expiração |
| `userProfile` | Perfil de negócio na tabela `users` |
| `initialized` | Impede corrida entre router e fetch inicial |
| `loading` | Flag usada em operações (login, signup, etc.) |

---
## Listener de Estado (`onAuthStateChange`)
Eventos tratados:
- `SIGNED_IN`: se novo utilizador (ou id mudou) carrega perfil.
- `SIGNED_OUT`: limpa estado.
- `TOKEN_REFRESHED`: atualiza `user` e `session`.

Mitigação de corrida: só substitui `user` se `user.value` não existir ou id for diferente.

---
## Funções Utilitárias Relevantes
| Função | Papel |
|--------|------|
| `clearAuthState()` | Limpa store + tokens localStorage |
| `fetchUserProfile(id)` | Obtém ou cria perfil consistindo makeup & defaults |
| `refreshUserData()` | Recarrega perfil após operações externas (ex: updates de saldo) |

---
## Tratamento de Erros
`handleApiError(error)` centraliza mensagens. Erros não previstos mostram "Ocorreu um erro inesperado".

Logs de erros críticos (ex: falha ao criar perfil) são enviados a `console.error` — pode evoluir para observabilidade externa.

---
## Segurança & Boas Práticas (Situação Atual vs Recomendações)
| Tema | Atual | Recomendações |
|------|-------|---------------|
| Confirmação de Email | Desativada | Ativar para evitar spam e hijacking |
| Tokens em localStorage | Sim | Avaliar `sessionStorage` ou rotacionar mais agressivamente conforme risco |
| Service Role no Front | Carregado apenas se chave presente (cuidado) | Garantir que a chave Service Role NUNCA chega ao bundle de produção |
| Admin Check | Baseado em `userProfile.type` | Complementar com Policies (RLS) no Supabase |
| Rate Limiting | Não implementado no front | Usar RLS + limites lado servidor |
| Recuperação Password | Padrão Supabase | Validar origem do redirect e usar deep link seguro |

---
## Sequência Textual (Login)
```
[UI] -> authStore.signIn -> authAPI.signIn -> Supabase
   Supabase => { user, session }
[Store] user=session.user; session=session
[Store] fetchUserProfile(user.id) -> usersAPI.getUserProfile
   (se não existir) -> usersAPI.createUserProfile
UI navega para rota protegida
```

## Sequência Textual (Primeiro Acesso / Recarregar Página)
```
App.vue onMounted -> initializeAuth
initializeAuth -> supabase.auth.getSession
  Se sessão existe: set user+session -> fetchUserProfile
  Senão: clearAuthState
Router aguarda initialized antes de decidir redirecionar
```

---
## Erros / Edge Cases
| Caso | Comportamento |
|------|---------------|
| Falha ao buscar perfil (no rows) | Cria perfil default |
| Corrida: dois creates simultâneos | Captura duplicate key e refaz fetch |
| Sessão expirada silenciosamente | Supabase refresh => `TOKEN_REFRESHED` evento |
| LocalStorage inacessível | Try/catch em `clearAuthState` evita crash |
| Timeout init (>5s) | Router segue, podendo redirecionar depois |

---
## Próximos Passos Sugeridos
1. Ativar verificação de email e fluxo de reenviar confirmação.
2. Implementar RLS Policies robustas para a tabela `users` e demais recursos.
3. Adicionar refresh periódico do perfil (ex: a cada N minutos) ou canal de realtime.
4. Criar hook/serviço de auditoria para logar logins e resets de password.
5. Abstrair Service Role para backend próprio (edge functions) se operações administrativas crescerem.

---
## Resumo
O sistema é centrado no Supabase Auth para credenciais e num perfil enriquecido na tabela `users` para dados de negócio. O Pinia coordena estado e sincronização via `initializeAuth` + listener de eventos, enquanto o router aplica regras de acesso. A arquitetura é extensível e beneficiará de reforços em verificação de email, políticas RLS e práticas de segurança adicionais conforme o produto evolui.
