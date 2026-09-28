# DiMeX — Exclusão Mútua Distribuída e Snapshots de Chandy-Lamport

**Disciplina:** Sistemas Distribuídos  
**Turma:** 320 - 2026/2 - PUCRS - Escola Politécnica  
**Professora:** Eduarda Rodrigues Monteiro (eduarda.monteiro@pucrs.br)

---

## Estrutura do Projeto

```
dimex-sd/
├── README.md                  
└── SD/
    ├── PP2PLink/
    │   └── PP2PLink.go        # Módulo Perfect Point-to-Point Link (sobre TCP)
    ├── DIMEX/
    │   └── DIMEX-Template.go  # Módulo DIMEX com suporte a Snapshots de Chandy-Lamport
    ├── useDIMEX.go            # Aplicação básica de teste (sem arquivo)
    ├── useDIMEX-f.go          # Aplicação de teste com escrita em arquivo e disparo de snapshots
    ├── checker.go             # Ferramenta de verificação de invariantes sobre os snapshots
    ├── go.mod                 # Configuração do módulo Go
    ├── .gitignore             
```

## Pré-requisitos

- [Go](https://go.dev/dl/) instalado (versão 1.18 ou superior)

---

## Como Executar Passo a Passo

Abra o terminal na pasta `SD/`:

```powershell
cd SD
```

### Passo 1 — Compilar a Aplicação

Compile o executável do DIMEX:

```powershell
go build -o dimex-f.exe useDIMEX-f.go
```

---

### Passo 2 — Lançar os 3 Processos

Abra **3 terminais separados** (todos na pasta `SD`), e execute os comandos abaixo **rapidamente** (dentro da janela inicial de 3 segundos):

**Terminal 1:**
```powershell
.\dimex-f.exe 0 127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002
```

**Terminal 2:**
```powershell
.\dimex-f.exe 1 127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002
```

**Terminal 3:**
```powershell
.\dimex-f.exe 2 127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002
```

> **Importante:** Os 3 processos devem ser iniciados em até 3 segundos entre si. O código possui um `time.Sleep(3s)` inicial para dar tempo de todos abrirem seus sockets TCP antes de começarem a trocar mensagens.

---

### Passo 3 — Coleta Automática dos Snapshots

Enquanto os processos disputam e acessam a Seção Crítica (SC), o **Processo 0** automaticamente dispara snapshots sucessivos em background:
1. Inicia uma sequência de **300 snapshots distribuídos** (`snId = 1, 2, ..., 300`) usando o algoritmo de **Chandy-Lamport**.
2. Cada processo grava seus estados e mensagens de canal em arquivos individuais:
   - `snapshot_proc_0.jsonl`
   - `snapshot_proc_1.jsonl`
   - `snapshot_proc_2.jsonl`

Aguarde até o Processo 0 exibir a mensagem:
```
[ SNAPSHOT ] Processo 0 concluiu o disparo de todos os 300 snapshots!
```

Aguarde cerca de **2 a 3 segundos** adicionais para que os últimos marcadores em trânsito cheguem a todos os processos e, em seguida, encerre os 3 terminais com `Ctrl+C`.

---

### Passo 4 — Verificar a Exclusão Mútua no Arquivo (`mxOUT.txt`)

Durante a execução, cada processo escreve `|` ao entrar na SC e `.` ao sair dela no arquivo `mxOUT.txt`. O arquivo deve conter exclusivamente a sequência alternada `|.|.|.|.|.|.`.

Para validar automaticamente que não houve invasão simultânea nem saída prematura, rode no PowerShell:

```powershell
# 1. Contar total de entradas e saídas (devem ser iguais)
$txt = Get-Content mxOUT.txt -Raw
Write-Host "Total de entradas (|):" ([regex]::Matches($txt, "\|").Count)
Write-Host "Total de saídas (.):" ([regex]::Matches($txt, "\.").Count)

# 2. Procurar por duas entradas seguidas (deve retornar 0)
Write-Host "Violações de entrada dupla (||):" ([regex]::Matches($txt, "\|\|").Count)

# 3. Procurar por duas saídas seguidas (deve retornar 0)
Write-Host "Violações de saída dupla (..):" ([regex]::Matches($txt, "\.\.").Count)
```

**Resultado esperado:**
```
Total de entradas (|): 107
Total de saídas (.): 107
Violações de entrada dupla (||): 0
Violações de saída dupla (..): 0
```

---

### Passo 5 — Avaliar a Consistência dos Snapshots (`checker.go`)

Com os arquivos `snapshot_proc_*.jsonl` gerados, execute a ferramenta de verificação:

```powershell
go run checker.go
```

A ferramenta reconstrói o estado global $\Sigma = (\sigma_0, \sigma_1, \sigma_2)$ e os estados dos canais $x_{j, i}$ para cada snapshot `SnId`, testando as seguintes **6 Invariantes do Sistema**:

| Invariante | Definição | O que testa |
|---|---|---|
| **Inv 1: Exclusão Mútua** | $\le 1$ processo com `st == inMX` | Garante que no máximo um processo acessa a SC por vez |
| **Inv 2: Repouso Global** | Se todos `st == noMX` $\implies$ todos `waiting` são false e canais vazios | Coerência de repouso no sistema |
| **Inv 3: Coerência de Espera** | Se $p$ tem $waiting[q] == true \implies p$ está em `inMX` ou `wantMX` | Processo fora da SC não retém permissões |
| **Inv 4: Conservação de Permissões** | Para $q$ em `wantMX`: $nbrResps + respOK_{trânsito} + \sum waiting + reqEntry_{trânsito} = N - 1$ | Conservação estrita de mensagens e permissões |
| **Inv 5: Permissões da SC** | Se $p$ está em `inMX` $\implies nbrResps == N-1$ e ninguém tem $p$ em waiting | Processo na SC obteve todas as permissões |
| **Inv 6: Consistência Temporal** | Para $p$ em `wantMX` $\implies reqTs_p \le lcl_p$ | Monotonicidade dos relógios lógicos locais |

---

## Exemplo Real de Saída do Avaliador

```
================================================================================
    AVALIADOR DE SNAPSHOTS DISTRIBUÍDOS — ALGORITMO DE CHANDY-LAMPORT          
================================================================================
Arquivos de snapshot detectados (3 processos):
  - snapshot_proc_0.jsonl
  - snapshot_proc_1.jsonl
  - snapshot_proc_2.jsonl

Total de Snapshots registrados pelo iniciador: 200
Número de processos no sistema (N): 3

--------------------------------------------------------------------------------
                            RESULTADO DA AVALIAÇÃO                              
--------------------------------------------------------------------------------
Snapshots analisados (completos) : 172
Snapshots válidos (sem violação) : 172 (100.00%)
Snapshots com violações          : 0
Snapshots incompletos (descarte) : 28

Diversidade de Estados Globais Capturados:
  - Nenhum processo na SC (todos fora)    : 146 snapshots
  - Exatamente 1 processo na SC           : 26 snapshots
  - Mensagens em trânsito nos canais      : 160 snapshots
  - Processos aguardando na fila (waiting): 136 snapshots

Status: SUCESSO! Todos os estados globais obtidos são consistentes.
Todas as invariantes (Inv 1 a Inv 6) foram satisfeitas em 100% dos snapshots!
```

---

## Propriedades Garantidas pelo DIMEX

- **DMX1 (Não-postergação):** se um processo solicita acesso, ele será eventualmente concedido.
- **DMX2 (Exclusão Mútua):** no máximo um processo acessa a seção crítica por vez.
