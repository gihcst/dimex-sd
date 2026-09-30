# DiMeX & DiMaD — Exclusão Mútua Distribuída, Snapshots de Chandy-Lamport e Injeção de Falhas

**Disciplina:** Sistemas Distribuídos  
**Turma:** 320 - 2026/2 - PUCRS - Escola Politécnica  
**Professora:** Eduarda Rodrigues Monteiro (eduarda.monteiro@pucrs.br)  

---

## Estrutura do Projeto

```
dimex-sd/
├── README.md                  # Documentação completa do projeto
└── SD/
    ├── PP2PLink/
    │   └── PP2PLink.go        # Módulo Perfect Point-to-Point Link (sobre TCP com framing)
    ├── DIMEX/
    │   └── DIMEX-Template.go  # Módulo DIMEX correto com Chandy-Lamport integrado
    ├── DIMAD/
    │   └── DIMAD.go           # Módulo DIMAD: versão com injeção proposital de falhas
    ├── useDIMEX.go            # Aplicação básica de teste em memória
    ├── useDIMEX-f.go          # Aplicação de teste com escrita em arquivo e disparo de snapshots
    ├── checker.go             # Ferramenta de verificação de invariantes sobre os snapshots
    ├── go.mod                 # Configuração do módulo Go
    └── .gitignore             
```

---

## Pré-requisitos

- [Go](https://go.dev/dl/) instalado (versão 1.18 ou superior)
- Terminal Bash (Linux/macOS) ou PowerShell (Windows)

---

## Arquitetura e Módulos do Sistema

O sistema implementa o algoritmo de **Ricart & Agrawala** para exclusão mútua distribuída, acoplado ao algoritmo de **Chandy-Lamport** para coleta concorrente de snapshots de cortes consistentes:

1. **PP2PLink (`PP2PLink/PP2PLink.go`):** Enlace confiável ponto a ponto sobre sockets TCP persistentes, com enquadramento de mensagens via prefixo de 4 dígitos para delimitação de tamanho.
2. **DIMEX (`DIMEX/DIMEX-Template.go` e `useDIMEX-f.go`):** Implementação íntegra e sem falhas do protocolo. Garante as propriedades **DMX1 (Não-postergação)** e **DMX2 (Exclusão Mútua)**.
3. **DIMAD (`DIMAD/DIMAD.go`):** Versão propositalmente defeituosa (*DIMEX com Anomalias e Defeitos*) desenvolvida para avaliar a capacidade do algoritmo de Chandy-Lamport de detectar violações de invariantes:
   - **Modo `violate`:** Concede `respOK` incondicionalmente ao receber `reqEntry`, permitindo invasão simultânea da Seção Crítica (violação de *Safety*).
   - **Modo `block`:** Retém permissões colocando na fila `waiting` incondicionalmente sem dar `respOK`, gerando espera circular e *deadlock* (violação de *Liveness*).
   - **Modo `random`:** Alterna aleatoriamente entre concessão indevida e bloqueio indevido.
4. **Checker (`checker.go`):** Reconstrói o estado global $\Sigma = (\sigma_0, \dots, \sigma_{N-1}, X)$ a partir dos arquivos `.jsonl` de cada processo e avalia **8 Invariantes Formais**.

---

## Tabela de Invariantes Verificadas (`checker.go`)

| Invariante | Definição Formal | O que testa | Comportamento Esperado |
|---|---|---|---|
| **Inv 1: Exclusão Mútua** | $\le 1$ processo com `st == inMX` | Garante que no máximo um processo acessa a SC por vez | Falha no modo `violate` |
| **Inv 2: Repouso Global** | Se todos `st == noMX` $\implies$ todos `waiting` são false e canais vazios | Coerência de repouso no sistema | Válido no repouso legítimo |
| **Inv 3: Coerência de Espera** | Se $p$ tem $waiting[q] == true \implies p \in \{\text{inMX}, \text{wantMX}\}$ | Processo fora da SC não pode reter permissões | Falha em bloqueio fora da SC |
| **Inv 4: Conservação de Permissões** | Para $q \in \text{wantMX}$: $nbrResps + respOK_{\text{trânsito}} + \sum waiting + reqEntry_{\text{trânsito}} = N - 1$ | Conservação estrita de mensagens e permissões | Válido sob canais confiáveis |
| **Inv 5: Permissões da SC** | Se $p \in \text{inMX} \implies nbrResps == N-1$ e ninguém tem $p$ em waiting | Processo na SC obteve todas as permissões legítimas | Falha em entrada prematura |
| **Inv 6: Consistência Temporal** | Para $p \in \text{wantMX} \implies reqTs_p \le lcl_p$ | Monotonicidade dos relógios lógicos locais | Válido sob relógios de Lamport |
| **Inv 7: Ausência de Bloqueio Circular** | $\neg (waiting_p[q] \land waiting_q[p])$ para $p \neq q$ | Detecta *Deadlock* (espera recíproca mútua) | **Falha no modo `block`** |
| **Inv 8: Respeito à Prioridade** | Se $p, q \in \text{wantMX}$ e $p$ pediu antes de $q \implies \neg waiting_q[p]$ | Garante que processo com prioridade temporal não seja bloqueado | **Falha no modo `block`** |

---

## Guia de Execução Passo a Passo

Entre na pasta `SD/`:
```bash
cd SD
```

---

### Cenário 1: Execução Correta (DIMEX Original — Referência de Sucesso)

Valida o funcionamento ideal do protocolo, onde 100% das invariantes são satisfeitas.

#### 1. Compilar e Limpar Arquivos
```bash
go build -o dimex-f useDIMEX-f.go
rm -f snapshot_proc_*.jsonl mxOUT.txt
```

#### 2. Lançar os 3 Processos (em 3 terminais separados dentro de 3s)
* **Terminal 1:** `./dimex-f 0 127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002`
* **Terminal 2:** `./dimex-f 1 127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002`
* **Terminal 3:** `./dimex-f 2 127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002`

#### 3. Avaliar
Aguarde a mensagem `Processo 0 concluiu o disparo de todos os 300 snapshots!`, encerre com `Ctrl+C` e execute:
```bash
go run checker.go
```
* **Resultado:** `100.00% válidos` | `0 violações` | **Status: SUCESSO!**

---

### Cenário 2: Injeção de Falha — Violação da Seção Crítica (`violate`)

Neste modo, processos concedem `respOK` incondicionalmente, causando colisões simultâneas na SC.

#### 1. Compilar o DIMAD e Limpar
```bash
go build -o dimad DIMAD/DIMAD.go
rm -f snapshot_proc_*.jsonl mxOUT.txt
```

#### 2. Lançar os 3 Processos com o parâmetro `violate`
* **Terminal 1:** `./dimad 0 violate 127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002`
* **Terminal 2:** `./dimad 1 violate 127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002`
* **Terminal 3:** `./dimad 2 violate 127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002`

#### 3. Verificar Violações no Log e no Checker
Aguarde o término dos disparos e encerre com `Ctrl+C`.

* **Checagem de Entradas Duplas (`||`) no `mxOUT.txt`:**
  ```bash
  grep -o "||" mxOUT.txt | wc -l
  ```
  *(Retorna centenas de violações de invasão simultânea).*

* **Executar o Avaliador de Snapshots:**
  ```bash
  go run checker.go
  ```
  *(Detecta violação da **Inv 1: Exclusão Mútua**, capturando 2 e até 3 processos em `inMX` simultaneamente).*

---

### Cenário 3: Injeção de Falha — Bloqueio Indevido / Deadlock (`block`)

Neste modo, processos retêm indevidamente as permissões na fila `waiting`, gerando espera circular e travamento (*deadlock*).

#### 1. Limpar Snapshots Anteriores
```bash
rm -f snapshot_proc_*.jsonl mxOUT.txt
```

#### 2. Lançar os 3 Processos com o parâmetro `block`
* **Terminal 1:** `./dimad 0 block 127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002`
* **Terminal 2:** `./dimad 1 block 127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002`
* **Terminal 3:** `./dimad 2 block 127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002`

#### 3. Avaliar
Os processos ficam travados aguardando permissão (o `mxOUT.txt` não progride), mas a goroutine de snapshots do Processo 0 registra em background o estado do sistema travado. Ao concluir os 200 snapshots, encerre com `Ctrl+C` e execute:

```bash
go run checker.go
```
*(Detecta violação da **Inv 7: Ausência de Bloqueio Circular** e **Inv 8: Respeito à Prioridade**, comprovando o bloqueio indevido e o deadlock via corte consistente).*

---

## Exemplos Reais de Saída do Avaliador (`checker.go`)

### Saída Real 1 — Modo Correto (`DIMEX`)
```text
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

Status: SUCESSO! Todos os estados globais obtidos são consistentes.
Todas as invariantes foram satisfeitas em 100% dos snapshots!
```

---

### Saída Real 2 — Modo Falha na Seção Crítica (`DIMAD violate`)
```text
--------------------------------------------------------------------------------
                            RESULTADO DA AVALIAÇÃO                              
--------------------------------------------------------------------------------
Snapshots analisados (completos) : 200
Snapshots válidos (sem violação) : 60 (30.00%)
Snapshots com violações          : 140

[ATENÇÃO] Violações encontradas:
  [Snapshot 1] Inv 1 (Exclusão Mútua): Mais de um processo na SC ao mesmo tempo: processos [1 0]
  [Snapshot 3] Inv 1 (Exclusão Mútua): Mais de um processo na SC ao mesmo tempo: processos [0 1]
  [Snapshot 13] Inv 1 (Exclusão Mútua): Mais de um processo na SC ao mesmo tempo: processos [0 1 2]
  [Snapshot 15] Inv 1 (Exclusão Mútua): Mais de um processo na SC ao mesmo tempo: processos [0 1 2]
  ... e mais 125 violações adicionais.

Status: FALHA (foram encontradas inconsistências nos snapshots).
```

---

### Saída Real 3 — Modo Falha de Bloqueio Indevido (`DIMAD block`)
```text
--------------------------------------------------------------------------------
                            RESULTADO DA AVALIAÇÃO                              
--------------------------------------------------------------------------------
Snapshots analisados (completos) : 200
Snapshots válidos (sem violação) : 0 (0.00%)
Snapshots com violações          : 200

[ATENÇÃO] Violações encontradas:
  [Snapshot 1] Inv 7 (Ausência de Bloqueio Circular / Deadlock): Bloqueio Circular (Deadlock) detectado: processo 0 e processo 1 estão em espera mútua (waiting[0][1]=true e waiting[1][0]=true)
  [Snapshot 1] Inv 8 (Respeito à Prioridade de Requisição): Bloqueio Indevido: processo 0 pediu antes de 1 (reqTs: 1790733842873419790 < 1790733843320391487), mas processo 1 reteve permissão indevidamente (waiting[0]=true)
  ... e mais 385 violações adicionais.

Status: FALHA (foram encontradas inconsistências nos snapshots).
```

---

## Conclusão Teórica

* **DMX2 (Exclusão Mútua — Propriedade de Segurança / Safety):** No modo correto, garante que nunca ocorrem dois acessos simultâneos. No modo `violate`, essa propriedade é quebrada e detectada tanto no arquivo físico (`mxOUT.txt`) quanto em tempo real via corte consistente de Chandy-Lamport (**Inv 1**).
* **DMX1 (Não-postergação — Propriedade de Vivacidade / Liveness):** No modo `block`, processos sofrem bloqueio indevido mútuo gerando deadlock. O algoritmo de Chandy-Lamport captura o corte desse estado, e o verificador comprova a existência de ciclos no grafo de espera (**Inv 7**) e retenção contrária à ordem total de timestamps (**Inv 8**).

