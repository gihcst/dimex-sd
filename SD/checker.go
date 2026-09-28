/*
	Ferramenta de Verificação de Invariantes em Snapshots Distribuídos (Chandy-Lamport)
	Disciplina: Sistemas Distribuídos - PUCRS

	Lê os arquivos 'snapshot_proc_<id>.jsonl' gerados por cada processo DIMEX,
	reconstrói o estado global de cada Snapshot (SnId) e avalia as seguintes invariantes:

	  Inv 1: Exclusão Mútua (no máximo 1 processo na SC simultaneamente)
	  Inv 2: Repouso Global (se todos em noMX, nenhum waiting pode ser true e canais devem estar vazios)
	  Inv 3: Coerência de Espera (se q está em waiting de p, então p deve estar em inMX ou wantMX)
	  Inv 4: Conservação de Permissões (para q em wantMX: nbrResps + respOK_trânsito + waitings + reqEntry_trânsito = N-1)
	  Inv 5: Permissões da SC (se p está em inMX: nbrResps == N-1 e ninguém tem p em waiting)
	  Inv 6: Consistência Temporal (se p está em wantMX: reqTs <= lcl)
*/

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// ProcessSnapshot representa o estado local + canais de entrada gravado por um processo
type ProcessSnapshot struct {
	SnId     int                 `json:"snId"`
	Id       int                 `json:"id"`
	State    string              `json:"st"`       // "noMX", "wantMX", "inMX"
	Lcl      int                 `json:"lcl"`
	ReqTs    int                 `json:"reqTs"`
	NbrResps int                 `json:"nbrResps"`
	Waiting  []bool              `json:"waiting"`
	Channels map[int][]string    `json:"channels"` // mensagens recebidas de cada canal antes do marker
}

// GlobalSnapshot representa o estado global reconstruído para um determinado SnId
type GlobalSnapshot struct {
	SnId      int
	N         int
	Processes map[int]ProcessSnapshot
}

// InvariantFunc define a assinatura de uma função que testa uma invariante sobre um snapshot global
type InvariantFunc func(snap GlobalSnapshot) (bool, string)

// ------------------------------------------------------------------------------------
// ------- definição das invariantes do sistema
// ------------------------------------------------------------------------------------

// Inv1_MutualExclusion: no máximo um processo na SC (inMX) ao mesmo tempo
func Inv1_MutualExclusion(snap GlobalSnapshot) (bool, string) {
	inMXCount := 0
	var inMXProcs []int
	for id, proc := range snap.Processes {
		if proc.State == "inMX" {
			inMXCount++
			inMXProcs = append(inMXProcs, id)
		}
	}
	if inMXCount > 1 {
		return false, fmt.Sprintf("Mais de um processo na SC ao mesmo tempo: processos %v", inMXProcs)
	}
	return true, ""
}

// Inv2_AllNoMX: se todos os processos estão em "não quero a SC" (noMX),
// então todos os waitings devem ser false e nenhum canal pode conter mensagens em trânsito
func Inv2_AllNoMX(snap GlobalSnapshot) (bool, string) {
	for _, proc := range snap.Processes {
		if proc.State != "noMX" {
			return true, "" // Invariante aplica-se apenas quando TODOS estão em noMX
		}
	}

	// Verifica se algum waiting é true
	for id, proc := range snap.Processes {
		for wId, isWaiting := range proc.Waiting {
			if isWaiting {
				return false, fmt.Sprintf("Todos processos em noMX, mas processo %d marcou waiting[%d]=true", id, wId)
			}
		}
	}

	// Verifica se há mensagens em trânsito nos canais
	for id, proc := range snap.Processes {
		for chId, msgs := range proc.Channels {
			if len(msgs) > 0 {
				return false, fmt.Sprintf("Todos processos em noMX, mas canal %d->%d tem mensagens em trânsito: %v", chId, id, msgs)
			}
		}
	}
	return true, ""
}

// Inv3_WaitingValid: se um processo q está marcado como waiting em p,
// então p deve estar na SC (inMX) ou disputando a SC (wantMX)
func Inv3_WaitingValid(snap GlobalSnapshot) (bool, string) {
	for pId, proc := range snap.Processes {
		for qId, isWaiting := range proc.Waiting {
			if isWaiting {
				if proc.State != "inMX" && proc.State != "wantMX" {
					return false, fmt.Sprintf("Processo %d está em %s, mas tem waiting[%d]=true (só permitido em inMX ou wantMX)", pId, proc.State, qId)
				}
			}
		}
	}
	return true, ""
}

// Inv4_PermissionConservation: se um processo q quer a SC (wantMX, não entrou ainda),
// a conservação de mensagens/permissões exige que a soma de:
//   1. Respostas já recebidas por q (nbrResps)
//   2. Mensagens respOK em trânsito para q nos canais j -> q
//   3. Flags waiting para q marcadas como true em outros processos (waiting_j[q] == true)
//   4. Mensagens reqEntry de q ainda em trânsito nos canais q -> j
// seja EXATAMENTE igual a N - 1 (onde N é o total de processos)
func Inv4_PermissionConservation(snap GlobalSnapshot) (bool, string) {
	for qId, qProc := range snap.Processes {
		if qProc.State != "wantMX" {
			continue
		}

		// 1. Respostas OK já recebidas
		nbrResps := qProc.NbrResps

		// 2. Mensagens respOK em trânsito para q (nos canais de entrada de q)
		respOKInTransit := 0
		for _, msgs := range qProc.Channels {
			for _, m := range msgs {
				if strings.Contains(m, "respOK") {
					respOKInTransit++
				}
			}
		}

		// 3. Flags waiting para q em outros processos
		waitingCount := 0
		for pId, pProc := range snap.Processes {
			if pId != qId && len(pProc.Waiting) > qId && pProc.Waiting[qId] {
				waitingCount++
			}
		}

		// 4. Mensagens reqEntry de q ainda em trânsito para outros processos
		reqEntryInTransit := 0
		for pId, pProc := range snap.Processes {
			if pId != qId {
				msgsFromQ := pProc.Channels[qId]
				for _, m := range msgsFromQ {
					if strings.Contains(m, "reqEntry") {
						reqEntryInTransit++
					}
				}
			}
		}

		total := nbrResps + respOKInTransit + waitingCount + reqEntryInTransit
		expected := snap.N - 1
		if total != expected {
			return false, fmt.Sprintf("Processo %d em wantMX violou conservação: total=%d (esperado %d) [nbrResps=%d, respOK_trânsito=%d, waitings=%d, reqEntry_trânsito=%d]",
				qId, total, expected, nbrResps, respOKInTransit, waitingCount, reqEntryInTransit)
		}
	}
	return true, ""
}

// Inv5_InMXPermissions: se um processo está na SC (inMX),
// ele deve ter recebido todas as N-1 respostas e nenhum outro processo pode tê-lo na fila de espera
func Inv5_InMXPermissions(snap GlobalSnapshot) (bool, string) {
	for pId, proc := range snap.Processes {
		if proc.State == "inMX" {
			if proc.NbrResps != snap.N-1 {
				return false, fmt.Sprintf("Processo %d está na SC (inMX), mas nbrResps=%d (esperado %d)", pId, proc.NbrResps, snap.N-1)
			}
			for otherId, otherProc := range snap.Processes {
				if otherId != pId && len(otherProc.Waiting) > pId && otherProc.Waiting[pId] {
					return false, fmt.Sprintf("Processo %d está na SC (inMX), mas processo %d marcou waiting[%d]=true", pId, otherId, pId)
				}
			}
		}
	}
	return true, ""
}

// Inv6_ClockConsistency: se um processo está em wantMX, o timestamp de requisição deve ser <= relógio lógico
func Inv6_ClockConsistency(snap GlobalSnapshot) (bool, string) {
	for pId, proc := range snap.Processes {
		if proc.State == "wantMX" {
			if proc.ReqTs > proc.Lcl {
				return false, fmt.Sprintf("Processo %d em wantMX tem reqTs (%d) > lcl (%d)", pId, proc.ReqTs, proc.Lcl)
			}
		}
	}
	return true, ""
}

// ------------------------------------------------------------------------------------
// ------- carregamento e avaliação dos snapshots
// ------------------------------------------------------------------------------------

func main() {
	fmt.Println("================================================================================")
	fmt.Println("    AVALIADOR DE SNAPSHOTS DISTRIBUÍDOS — ALGORITMO DE CHANDY-LAMPORT          ")
	fmt.Println("================================================================================")

	// Encontra todos os arquivos de snapshot no diretório atual
	pattern := "snapshot_proc_*.jsonl"
	files, err := filepath.Glob(pattern)
	if err != nil || len(files) == 0 {
		fmt.Printf("Nenhum arquivo encontrado com o padrão '%s'.\n", pattern)
		fmt.Println("Certifique-se de executar o useDIMEX-f primeiro para coletar snapshots.")
		os.Exit(1)
	}

	fmt.Printf("Arquivos de snapshot detectados (%d processos):\n", len(files))
	for _, f := range files {
		fmt.Printf("  - %s\n", f)
	}
	fmt.Println()

	// Mapa: snId -> map[procId]ProcessSnapshot
	allSnapshots := make(map[int]map[int]ProcessSnapshot)
	procIdsSet := make(map[int]bool)

	for _, fileName := range files {
		file, err := os.Open(fileName)
		if err != nil {
			fmt.Printf("Erro ao abrir %s: %v\n", fileName, err)
			continue
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}

			var ps ProcessSnapshot
			if err := json.Unmarshal([]byte(line), &ps); err != nil {
				fmt.Printf("Aviso: falha ao deserializar linha em %s: %v\n", fileName, err)
				continue
			}

			procIdsSet[ps.Id] = true
			if allSnapshots[ps.SnId] == nil {
				allSnapshots[ps.SnId] = make(map[int]ProcessSnapshot)
			}
			allSnapshots[ps.SnId][ps.Id] = ps
		}
		file.Close()
	}

	totalProcesses := len(procIdsSet)
	if totalProcesses < 2 {
		fmt.Printf("Erro: detectados apenas %d processo(s). São necessários pelo menos 2 (recomendado >= 3).\n", totalProcesses)
		os.Exit(1)
	}

	// Ordena os IDs dos snapshots
	var snIds []int
	for snId := range allSnapshots {
		snIds = append(snIds, snId)
	}
	sort.Ints(snIds)

	fmt.Printf("Total de Snapshots registrados pelo iniciador: %d\n", len(snIds))
	fmt.Printf("Número de processos no sistema (N): %d\n\n", totalProcesses)

	// Registra as invariantes
	invariants := []struct {
		name string
		fn   InvariantFunc
	}{
		{"Inv 1 (Exclusão Mútua)", Inv1_MutualExclusion},
		{"Inv 2 (Repouso Global)", Inv2_AllNoMX},
		{"Inv 3 (Coerência de Espera)", Inv3_WaitingValid},
		{"Inv 4 (Conservação de Permissões)", Inv4_PermissionConservation},
		{"Inv 5 (Permissões da SC)", Inv5_InMXPermissions},
		{"Inv 6 (Consistência Temporal)", Inv6_ClockConsistency},
	}

	validSnapshots := 0
	incompleteSnapshots := 0
	violatedSnapshots := 0

	// Estatísticas sobre os estados capturados
	count0InMX := 0
	count1InMX := 0
	countWithTransit := 0
	countWithWait := 0

	type ViolationRecord struct {
		SnId    int
		InvName string
		Detail  string
	}
	var violations []ViolationRecord

	for _, snId := range snIds {
		procMap := allSnapshots[snId]

		// Verifica se o snapshot foi completado por todos os processos
		if len(procMap) < totalProcesses {
			incompleteSnapshots++
			continue
		}

		globalSnap := GlobalSnapshot{
			SnId:      snId,
			N:         totalProcesses,
			Processes: procMap,
		}

		// Coleta estatísticas
		inMXProcs := 0
		hasTransit := false
		hasWait := false

		for _, p := range procMap {
			if p.State == "inMX" {
				inMXProcs++
			}
			for _, isWait := range p.Waiting {
				if isWait {
					hasWait = true
				}
			}
			for _, msgs := range p.Channels {
				if len(msgs) > 0 {
					hasTransit = true
				}
			}
		}

		if inMXProcs == 0 {
			count0InMX++
		} else if inMXProcs == 1 {
			count1InMX++
		}
		if hasTransit {
			countWithTransit++
		}
		if hasWait {
			countWithWait++
		}

		// Avalia cada invariante
		snapOk := true
		for _, inv := range invariants {
			ok, detail := inv.fn(globalSnap)
			if !ok {
				snapOk = false
				violations = append(violations, ViolationRecord{
					SnId:    snId,
					InvName: inv.name,
					Detail:  detail,
				})
			}
		}

		if snapOk {
			validSnapshots++
		} else {
			violatedSnapshots++
		}
	}

	// --------------------------------------------------------------------------------
	// ------- relatório final
	// --------------------------------------------------------------------------------

	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println("                            RESULTADO DA AVALIAÇÃO                              ")
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Printf("Snapshots analisados (completos) : %d\n", validSnapshots+violatedSnapshots)
	fmt.Printf("Snapshots válidos (sem violação) : %d (%.2f%%)\n",
		validSnapshots,
		float64(validSnapshots)/float64(validSnapshots+violatedSnapshots)*100)
	fmt.Printf("Snapshots com violações          : %d\n", violatedSnapshots)
	if incompleteSnapshots > 0 {
		fmt.Printf("Snapshots incompletos (descarte) : %d\n", incompleteSnapshots)
	}

	fmt.Println("\nDiversidade de Estados Globais Capturados:")
	fmt.Printf("  - Nenhum processo na SC (todos fora)    : %d snapshots\n", count0InMX)
	fmt.Printf("  - Exatamente 1 processo na SC           : %d snapshots\n", count1InMX)
	fmt.Printf("  - Mensagens em trânsito nos canais      : %d snapshots\n", countWithTransit)
	fmt.Printf("  - Processos aguardando na fila (waiting): %d snapshots\n", countWithWait)

	if len(violations) > 0 {
		fmt.Println("\n[ATENÇÃO] Violações encontradas:")
		for i, v := range violations {
			if i < 15 { // Limita exibição das primeiras 15 violações
				fmt.Printf("  [Snapshot %d] %s: %s\n", v.SnId, v.InvName, v.Detail)
			}
		}
		if len(violations) > 15 {
			fmt.Printf("  ... e mais %d violações adicionais.\n", len(violations)-15)
		}
		fmt.Println("\nStatus: FALHA (foram encontradas inconsistências nos snapshots).")
		os.Exit(1)
	} else {
		fmt.Println("\nStatus: SUCESSO! Todos os estados globais obtidos são consistentes.")
		fmt.Println("Todas as invariantes (Inv 1 a Inv 6) foram satisfeitas em 100% dos snapshots!")
	}
}

func init() {
	// Apenas para evitar warnings de imports não utilizados se necessário
	_ = strconv.Itoa
}
