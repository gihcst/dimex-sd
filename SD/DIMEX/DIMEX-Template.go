/*  Construido como parte da disciplina: FPPD - PUCRS - Escola Politecnica
    Professor: Fernando Dotti  (https://fldotti.github.io/)
    Modulo representando Algoritmo de Exclusão Mútua Distribuída:
    Semestre 2023/1
	Aspectos a observar:
	   mapeamento de módulo para estrutura
	   inicializacao
	   semantica de concorrência: cada evento é atômico
	   							  módulo trata 1 por vez
	Q U E S T A O
	   Além de obviamente entender a estrutura ...
	   Implementar o núcleo do algoritmo ja descrito, ou seja, o corpo das
	   funcoes reativas a cada entrada possível:
	   			handleUponReqEntry()  // recebe do nivel de cima (app)
				handleUponReqExit()   // recebe do nivel de cima (app)
				handleUponDeliverRespOk(msgOutro)   // recebe do nivel de baixo
				handleUponDeliverReqEntry(msgOutro) // recebe do nivel de baixo
*/

package DIMEX

import (
	PP2PLink "SD/PP2PLink"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// ------------------------------------------------------------------------------------
// ------- principais tipos
// ------------------------------------------------------------------------------------

// State são os estados possiveis do processo
type State int

const (
	noMX   State = iota // processo NÃO quer acessar a SC (estado inicial)
	wantMX              // processo QUER acessar a SC, aguardando permissões
	inMX                // processo ESTÁ na SC (acessando o recurso)
)

// dmxReq representa os tipos de requisição que a aplicação pode fazer ao DIMEX
type dmxReq int

const (
	ENTER dmxReq = iota // aplicação quer entrar na SC
	EXIT                // aplicação quer sair da SC
)

// dmxResp é o sinal que o DIMEX envia para a aplicação quando ela pode entrar na SC
type dmxResp struct{}

// ------------------------------------------------------------------------------------
// ------- tipos para Chandy-Lamport Distributed Snapshot
// ------------------------------------------------------------------------------------

// ProcessSnapshot representa o estado gravado por este processo para um determinado SnId
type ProcessSnapshot struct {
	SnId     int              `json:"snId"`
	Id       int              `json:"id"`
	State    string           `json:"st"`       // "noMX", "wantMX", "inMX"
	Lcl      int              `json:"lcl"`
	ReqTs    int              `json:"reqTs"`
	NbrResps int              `json:"nbrResps"`
	Waiting  []bool           `json:"waiting"`
	Channels map[int][]string `json:"channels"` // mensagens recebidas em cada canal de entrada antes do marker
}

// SnapshotState armazena o progresso de coleta de um snapshot específico
type SnapshotState struct {
	SnId            int
	RecordedLocal   bool
	LocalState      ProcessSnapshot
	RecordingFrom   map[int]bool // quais canais estamos ativamente gravando mensagens
	MarkersReceived map[int]bool // de quais processos já recebemos o marker deste SnId
}

// DIMEX_Module é a estrutura principal do módulo de exclusão mútua distribuída
type DIMEX_Module struct {
	Req       chan dmxReq  // canal para receber pedidos da aplicacao (REQ e EXIT)
	Ind       chan dmxResp // canal para informar aplicacao que pode acessar
	processes []string     // endereco de todos, na mesma ordem
	id        int          // identificador do processo - é o indice no array de enderecos acima
	st        State        // estado deste processo na exclusao mutua distribuida
	waiting   []bool       // processos aguardando tem flag true
	lcl       int          // relogio logico local, "saber a ordem dos pedidos, quem pediu primeiro"
	reqTs     int          // timestamp local da ultima requisicao deste processo
	nbrResps  int
	dbg       bool

	Pp2plink *PP2PLink.PP2PLink // acesso aa comunicacao enviar por PP2PLinq.Req e receber por PP2PLinq.Ind

	// Chandy-Lamport Distributed Snapshot
	SnapReq         chan int               // canal para solicitar início de snapshot
	activeSnapshots map[int]*SnapshotState // snId -> estado do snapshot em andamento
	snapFile        *os.File               // arquivo onde os snapshots deste processo são salvos
}

// ------------------------------------------------------------------------------------
// ------- inicialização
// ------------------------------------------------------------------------------------

// NewDIMEX cria e inicializa um novo módulo DIMEX.
// Recebe a lista de endereços de todos os processos, o id deste processo, e flag de debug.
// Internamente cria o PP2PLink (camada de rede) e inicia a goroutine principal.
func NewDIMEX(_addresses []string, _id int, _dbg bool) *DIMEX_Module {

	// Cria o módulo de comunicação PP2PLink, que escuta no endereço deste processo
	p2p := PP2PLink.NewPP2PLink(_addresses[_id], _dbg)

	// Cria/trunca o arquivo onde este processo gravará seus snapshots
	snapFileName := fmt.Sprintf("snapshot_proc_%d.jsonl", _id)
	sFile, err := os.OpenFile(snapFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("Erro ao abrir arquivo de snapshot %s: %v\n", snapFileName, err)
	}

	dmx := &DIMEX_Module{
		Req:             make(chan dmxReq, 1),
		Ind:             make(chan dmxResp, 1),
		processes:       _addresses,
		id:              _id,
		st:              noMX,                          // começa sem querer a SC
		waiting:         make([]bool, len(_addresses)), // ninguém esperando inicialmente
		lcl:             0,
		reqTs:           0,
		dbg:             _dbg,
		Pp2plink:        p2p,
		SnapReq:         make(chan int, 100),
		activeSnapshots: make(map[int]*SnapshotState),
		snapFile:        sFile,
	}

	// Inicializa todos os flags de espera como false
	for i := 0; i < len(dmx.waiting); i++ {
		dmx.waiting[i] = false
	}
	dmx.Start()
	dmx.outDbg(" ---->>> Init DIMEX! <<<---- ")
	return dmx
}

// ------------------------------------------------------------------------------------
// ------- núcleo do funcionamento (event loop)
// ------------------------------------------------------------------------------------

// Start inicia a goroutine principal do DIMEX.
// Essa goroutine fica em loop infinito aguardando eventos:
//   - canal Req: pedidos da aplicação (ENTER ou EXIT)
//   - canal SnapReq: pedidos para disparar um snapshot (Chandy-Lamport)
//   - canal Pp2plink.Ind: mensagens de outros processos (reqEntry, respOK, marker)
//
// O select garante que apenas UM evento é tratado por vez (atomicidade).
func (module *DIMEX_Module) Start() {

	go func() {
		for {
			select {
			case dmxR := <-module.Req: // evento vindo da APLICAÇÃO (camada de cima)
				if dmxR == ENTER {
					module.outDbg(" ---->>> PEDI PARA USAR A SC <<<---- ")
					module.handleUponReqEntry() // trata pedido de entrada na SC

				} else if dmxR == EXIT {
					module.outDbg(" ---->>> SAI DA SC <<<---- ")
					module.handleUponReqExit() // trata pedido de saída da SC
				}

			case snId := <-module.SnapReq: // pedido da aplicação para iniciar snapshot
				module.handleInitiateSnapshot(snId)

			case msgOutro := <-module.Pp2plink.Ind: // vindo de outro processo
				if strings.HasPrefix(msgOutro.Message, "marker,") {
					// Mensagem de marcador do algoritmo Chandy-Lamport
					module.handleUponDeliverMarker(msgOutro.Message)
				} else {
					// Mensagem normal do protocolo DIMEX (reqEntry ou respOK).
					// PASSO CHANDY-LAMPORT: Se algum snapshot ativo estiver gravando o canal
					// de onde veio esta mensagem, registra ela no canal!
					senderId := module.extractSenderId(msgOutro.Message)
					if senderId >= 0 {
						for _, snap := range module.activeSnapshots {
							if snap.RecordingFrom[senderId] {
								snap.LocalState.Channels[senderId] = append(snap.LocalState.Channels[senderId], msgOutro.Message)
							}
						}
					}

					if strings.Contains(msgOutro.Message, "respOK") {
						module.outDbg(" ---->>> ME RESPONDERAM OK <<<---- " + msgOutro.Message)
						module.handleUponDeliverRespOk(msgOutro) // outro processo deu permissão

					} else if strings.Contains(msgOutro.Message, "reqEntry") {
						module.outDbg(" ---->>> ME PEDIRAM PARA USAR A SC <<<---- " + msgOutro.Message)
						module.handleUponDeliverReqEntry(msgOutro) // outro processo quer a SC
					}
				}
			}
		}
	}()
}

// ------------------------------------------------------------------------------------
// ------- tratamento de pedidos vindos da aplicação
// ------- UPON ENTRY
// ------- UPON EXIT
// ------------------------------------------------------------------------------------

// handleUponReqEntry trata o pedido da aplicação para ENTRAR na SC.
// Passos:
//  1. Atualiza o relógio lógico (timestamp atual)
//  2. Zera o contador de respostas
//  3. Envia "reqEntry,id,timestamp" para TODOS os outros processos
//  4. Muda o estado para wantMX (querendo a SC)
func (module *DIMEX_Module) handleUponReqEntry() {
	module.outDbg(" ---->>> ENTREI NA handleUponReqEntry <<<----")
	module.outDbg(" ---->>> VOU PEDIR ACESSO <<<----")
	module.lcl = int(time.Now().UnixNano()) // atualiza o relógio lógico com o tempo atual
	module.reqTs = module.lcl               // guarda o timestamp deste pedido
	module.nbrResps = 0                     // zera o contador de respostas OK

	// Envia requisição de entrada para TODOS os outros processos
	for i, address := range module.processes {
		if i != module.id { // não envia para si mesmo
			msg := fmt.Sprintf("reqEntry,%d,%d", module.id, module.reqTs)
			module.sendToLink(address, msg, "")
		}
	}

	// ATUALIZA O ESTADO: agora este processo QUER entrar na SC
	module.st = wantMX
	module.outDbg(" ---->>> MANDEI MSG DE PEDIDO PARA TODO MUNDO <<<----")
}

// handleUponReqExit trata o pedido da aplicação para SAIR da SC.
// Passos:
//  1. Para cada processo que estava esperando (waiting[i] == true), envia "respOK,id"
//  2. Limpa a fila de espera (todos waiting = false)
//  3. Muda o estado para noMX (não quer mais a SC)
func (module *DIMEX_Module) handleUponReqExit() {
	module.outDbg("ENTREI NO handleUponReqExit")
	module.outDbg(" ---->>> VOU SAIR DA SC <<<----")

	// Percorre todos os processos e envia OK para quem estava esperando
	for i, isWaiting := range module.waiting {
		if isWaiting { // vê quem ta esperando e avisa a todos que saiu
			module.sendToLink(module.processes[i], fmt.Sprintf("respOK,%d", module.id), " ")
		}
	}

	// Limpa a fila de espera e volta ao estado inicial
	module.waiting = make([]bool, len(module.processes))
	module.st = noMX // atualiza o estado para "não está na região crítica"
	module.outDbg(" ---->>> LIBEREI A SC <<<----")
}

// ------------------------------------------------------------------------------------
// ------- tratamento de mensagens de outros processos
// ------- UPON respOk
// ------- UPON reqEntry
// ------------------------------------------------------------------------------------

// handleUponDeliverRespOk trata o recebimento de uma resposta "respOK" de outro processo.
// Isso significa que o outro processo deu permissão para este entrar na SC.
// Quando o número de respostas atinge N-1 (todos responderam), o processo entra na SC.
func (module *DIMEX_Module) handleUponDeliverRespOk(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	module.outDbg(" ---->>> ENTREI NA handleUponDeliverRespOk <<<----")

	module.nbrResps++ // conta mais uma resposta OK recebida
	module.outDbg(fmt.Sprintf(" ---->>> RESPOSTA OK RECEBIDA. NUM DE MGS: %d <<<---- ", module.nbrResps))

	// Se recebeu OK de TODOS os outros (N-1), pode entrar na SC
	if module.nbrResps == len(module.processes)-1 {
		module.Ind <- dmxResp{} // sinaliza para a aplicação "pode acessar!"
		module.st = inMX        // muda estado para "na SC"
		module.outDbg(" ---->>> ENTRANDO NA SC <<<----")
	}
}

// handleUponDeliverReqEntry trata o recebimento de um pedido "reqEntry" de outro processo.
// Decide se dá permissão imediata ou coloca na fila de espera:
//
//   CASO 1 (responde OK): se este processo NÃO quer a SC, OU se ambos querem mas
//           o outro tem prioridade (timestamp menor, ou desempate por id menor)
//
//   CASO 2 (coloca na fila): se este processo ESTÁ na SC, OU se ambos querem mas
//           este processo tem prioridade
//
// Ao final, atualiza o relógio lógico se o timestamp recebido for maior que o local.
func (module *DIMEX_Module) handleUponDeliverReqEntry(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	var id, ts int
	module.outDbg(" ---->>> ENTRANDO NA handleUponDeliverReqEntry <<<----")

	// Extrai o id e timestamp do processo que está pedindo
	fmt.Sscanf(msgOutro.Message, "reqEntry,%d,%d", &id, &ts)

	// CASO 1: dá permissão imediata
	// → eu não quero a SC (noMX)
	// → eu quero, mas o outro pediu antes (tem prioridade)
	if module.st == noMX || (module.st == wantMX && before(id, ts, module.id, module.reqTs)) {
		module.outDbg(" ---->>> VOU DEIXAR PASSAR <<<---- ")
		module.sendToLink(module.processes[id], fmt.Sprintf("respOK,%d", module.id), " ")

	// CASO 2: nega permissão (coloca na fila de espera)
	// → eu estou na SC (inMX)
	// → eu quero e EU pedi antes (eu tenho prioridade)
	} else if module.st == inMX || (module.st == wantMX && before(module.id, module.reqTs, id, ts)) {
		module.outDbg(" ---->>> VAI TER QUE ESPERAR <<<---- ")
		module.waiting[id] = true // mando esperar na fila
	}

	// Atualiza relógio lógico se o timestamp recebido for mais recente
	if ts > module.lcl {
		module.outDbg("Atualizando relógio lógico local")
		module.lcl = ts
	}
}

// ------------------------------------------------------------------------------------
// ------- algoritmo de Chandy-Lamport (Distributed Snapshot)
// ------------------------------------------------------------------------------------

// StartSnapshot solicita que este processo inicie um snapshot distribuído com o id informado
func (module *DIMEX_Module) StartSnapshot(snId int) {
	module.SnapReq <- snId
}

// handleInitiateSnapshot é executado quando a aplicação deste processo inicia um novo snapshot
func (module *DIMEX_Module) handleInitiateSnapshot(snId int) {
	module.outDbg(fmt.Sprintf(" ---->>> INICIANDO SNAPSHOT %d <<<----", snId))

	snap := &SnapshotState{
		SnId:            snId,
		RecordedLocal:   true,
		LocalState:      module.captureLocalState(snId),
		RecordingFrom:   make(map[int]bool),
		MarkersReceived: make(map[int]bool),
	}

	// Como este processo é o iniciador:
	// 1. Grava seu estado local σ (já capturado acima)
	// 2. Envia MARKER(snId, id) para todos os canais de saída
	// 3. Inicia gravação de mensagens recebidas de cada um de seus canais de entrada
	for i := range module.processes {
		if i != module.id {
			snap.RecordingFrom[i] = true
			markerMsg := fmt.Sprintf("marker,%d,%d", snId, module.id)
			module.sendToLink(module.processes[i], markerMsg, " ")
		}
	}

	module.activeSnapshots[snId] = snap
}

// handleUponDeliverMarker trata a recepção de um marcador Chandy-Lamport
func (module *DIMEX_Module) handleUponDeliverMarker(msg string) {
	var snId, fromId int
	n, err := fmt.Sscanf(msg, "marker,%d,%d", &snId, &fromId)
	if err != nil || n != 2 {
		module.outDbg("Erro ao parsear marker: " + msg)
		return
	}

	snap, exists := module.activeSnapshots[snId]
	if !exists {
		// REGRA 1 DO CHANDY-LAMPORT:
		// Primeira vez que recebe o marcador para este snId!
		module.outDbg(fmt.Sprintf(" ---->>> 1º MARKER DO SNAPSHOT %d RECEBIDO DE %d <<<----", snId, fromId))

		snap = &SnapshotState{
			SnId:            snId,
			RecordedLocal:   true,
			LocalState:      module.captureLocalState(snId),
			RecordingFrom:   make(map[int]bool),
			MarkersReceived: make(map[int]bool),
		}

		// O estado do canal do remetente (fromId) é declarado VAZIO (não grava dele)
		snap.RecordingFrom[fromId] = false
		snap.MarkersReceived[fromId] = true

		// Envia marcador para todos os canais de saída OUT
		for i := range module.processes {
			if i != module.id {
				markerMsg := fmt.Sprintf("marker,%d,%d", snId, module.id)
				module.sendToLink(module.processes[i], markerMsg, " ")
			}
		}

		// Inicia gravação de mensagens de todos os OUTROS canais de entrada
		for i := range module.processes {
			if i != module.id && i != fromId {
				snap.RecordingFrom[i] = true
			}
		}

		module.activeSnapshots[snId] = snap
	} else {
		// REGRA 2 DO CHANDY-LAMPORT:
		// Marcador recebido DEPOIS da primeira vez (de outro canal ps):
		// Pára de gravar mensagens de fromId e fecha o canal.
		module.outDbg(fmt.Sprintf(" ---->>> MARKER SUBSEQUENTE DO SNAPSHOT %d RECEBIDO DE %d <<<----", snId, fromId))
		snap.RecordingFrom[fromId] = false
		snap.MarkersReceived[fromId] = true
	}

	// Se recebeu o marcador de todos os canais de entrada (N-1 processos):
	// A contribuição deste processo para o snapshot snId está concluída!
	if len(snap.MarkersReceived) == len(module.processes)-1 {
		module.outDbg(fmt.Sprintf(" ---->>> SNAPSHOT %d COMPLETO NO PROCESSO %d! <<<----", snId, module.id))
		module.saveSnapshot(snap.LocalState)
		delete(module.activeSnapshots, snId)
	}
}

// captureLocalState captura atomicamente o estado local deste processo no momento do snapshot
func (module *DIMEX_Module) captureLocalState(snId int) ProcessSnapshot {
	stStr := "noMX"
	if module.st == wantMX {
		stStr = "wantMX"
	} else if module.st == inMX {
		stStr = "inMX"
	}

	// Cópia do vetor waiting
	waitCopy := make([]bool, len(module.waiting))
	copy(waitCopy, module.waiting)

	// Inicializa o mapa de canais com slices vazios
	channels := make(map[int][]string)
	for i := range module.processes {
		if i != module.id {
			channels[i] = make([]string, 0)
		}
	}

	return ProcessSnapshot{
		SnId:     snId,
		Id:       module.id,
		State:    stStr,
		Lcl:      module.lcl,
		ReqTs:    module.reqTs,
		NbrResps: module.nbrResps,
		Waiting:  waitCopy,
		Channels: channels,
	}
}

// saveSnapshot persiste o snapshot completo deste processo em formato JSONL
func (module *DIMEX_Module) saveSnapshot(snap ProcessSnapshot) {
	if module.snapFile != nil {
		data, err := json.Marshal(snap)
		if err == nil {
			module.snapFile.WriteString(string(data) + "\n")
			module.snapFile.Sync()
		}
	}
}

// extractSenderId extrai o ID do processo remetente de uma mensagem DIMEX
func (module *DIMEX_Module) extractSenderId(msg string) int {
	if strings.HasPrefix(msg, "reqEntry,") {
		var id, ts int
		if n, _ := fmt.Sscanf(msg, "reqEntry,%d,%d", &id, &ts); n >= 1 {
			return id
		}
	} else if strings.HasPrefix(msg, "respOK,") {
		var id int
		if n, _ := fmt.Sscanf(msg, "respOK,%d", &id); n == 1 {
			return id
		}
	}
	return -1
}

// ------------------------------------------------------------------------------------
// ------- funções auxiliares
// ------------------------------------------------------------------------------------

// sendToLink envia uma mensagem para outro processo via PP2PLink.
// É um wrapper que facilita o envio e adiciona log de debug.
func (module *DIMEX_Module) sendToLink(address string, content string, space string) {
	module.outDbg(space + " ---->>>>   to: " + address + "     msg: " + content)
	module.Pp2plink.Req <- PP2PLink.PP2PLink_Req_Message{
		To:      address,
		Message: content}
}

// before compara dois pedidos para decidir quem tem PRIORIDADE.
// Prioridade é dada ao pedido com timestamp MENOR.
// Em caso de empate no timestamp, desempata pelo id MENOR.
// Retorna true se (oneId, oneTs) tem prioridade sobre (othId, othTs).
func before(oneId, oneTs, othId, othTs int) bool {
	if oneTs < othTs {
		return true // timestamp menor = pediu antes = tem prioridade
	} else if oneTs > othTs {
		return false // timestamp maior = pediu depois = não tem prioridade
	} else {
		return oneId < othId // timestamps iguais: desempata pelo id menor
	}
}

// outDbg imprime uma mensagem de debug formatada, apenas se o modo debug estiver ativo
func (module *DIMEX_Module) outDbg(s string) {
	if module.dbg {
		fmt.Println(". . . . . . . . . . . . [ DIMEX : " + s + " ]")
	}
}