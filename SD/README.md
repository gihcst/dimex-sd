# DiMeX — Exclusão Mútua Distribuída

**Disciplina:** Sistemas Distribuídos  
**Turma:** 320 - 2026/2 - PUCRS - Escola Politécnica  
**Professora:** Eduarda Rodrigues Monteiro (eduarda.monteiro@pucrs.br)

## Estrutura do Projeto

```
SD/
├── PP2PLink/
│   └── PP2PLink.go            # Módulo Perfect Point-to-Point Link (sobre TCP)
├── DIMEX/
│   └── DIMEX-Template.go      # Módulo de Exclusão Mútua Distribuída
├── useDIMEX.go                # Aplicação básica de teste (sem arquivo)
├── useDIMEX-f.go              # Aplicação de teste com escrita em arquivo
├── go.mod                     
└── README.md                  
```

## Pré-requisitos

- [Go](https://go.dev/dl/) instalado (versão 1.18 ou superior)

## Como Executar

### Passo 1 — Compilar

Abra um terminal na pasta `SD/SD/` e compile:

```powershell
cd SD\SD
go build -o dimex-f.exe useDIMEX-f.go
```

### Passo 2 — Lançar os 3 processos

Abra **3 terminais separados**, todos na pasta `SD\SD\`, e execute **rapidamente** (dentro de 3 segundos):

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

> **Importante:** os 3 processos devem ser lançados dentro de **3 segundos** entre si. O código tem um `time.Sleep(3s)` no início para aguardar todos os processos subirem seus listeners TCP antes de começar a trocar mensagens.


### Passo 3 — Aguardar execução

Deixe os processos rodando por **30 segundos a 1 minuto** para gerar dados suficientes. Depois, pare todos com `Ctrl+C` nos 3 terminais.

### Passo 4 — Verificar resultado

O arquivo `mxOUT.txt` será gerado na pasta `SD\SD\`. Ele deve conter apenas a sequência `|.|.|.|.|.|.` repetida.

Para verificar se houve violação da exclusão mútua:

```powershell
# Procurar por duas entradas seguidas (não deve retornar nada)
Select-String -Pattern "\|\|" -Path mxOUT.txt

# Procurar por duas saídas seguidas (não deve retornar nada)
Select-String -Pattern "\.\." -Path mxOUT.txt
```

Se **nenhum resultado** for retornado, a exclusão mútua está funcionando corretamente.

## Como Funciona

Cada processo, ao acessar a seção crítica, escreve:
- `|` ao **entrar** na seção crítica
- `.` ao **sair** da seção crítica

Se o algoritmo de exclusão mútua estiver correto, o arquivo `mxOUT.txt` conterá apenas a sequência `|.|.|.|.|.` — nunca `||` (duas entradas sem saída) ou `..` (duas saídas sem entrada).

## Parâmetros

```
.\dimex-f.exe <id> <endereço_0> <endereço_1> <endereço_2> [... <endereço_N>]
```

| Parâmetro | Descrição |
|-----------|-----------|
| `id` | Identificador único do processo (0, 1, 2, ...) |
| `endereço_N` | Endereço `ip:porta` de cada processo na mesma ordem |

O endereço na posição `id` é usado como endereço de escuta do próprio processo.

## Propriedades Garantidas

- **DMX1 (Não-postergação):** se um processo solicita acesso, ele será eventualmente concedido
- **DMX2 (Exclusão Mútua):** no máximo um processo acessa a seção crítica por vez
