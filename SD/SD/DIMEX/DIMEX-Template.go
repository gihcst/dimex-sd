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
	"fmt"
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

	dmx := &DIMEX_Module{
		Req: make(chan dmxReq, 1),
		Ind: make(chan dmxResp, 1),

		processes: _addresses,
		id:        _id,
		st:        noMX,                          // começa sem querer a SC
		waiting:   make([]bool, len(_addresses)),  // ninguém esperando inicialmente
		lcl:       0,
		reqTs:     0,
		dbg:       _dbg,

		Pp2plink: p2p}

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
// Essa goroutine fica em loop infinito aguardando eventos de duas fontes:
//   - canal Req: pedidos da aplicação (ENTER ou EXIT)
//   - canal Pp2plink.Ind: mensagens de outros processos (reqEntry ou respOK)
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

			case msgOutro := <-module.Pp2plink.Ind: // vindo de outro processo
				//fmt.Printf("dimex recebe da rede: ", msgOutro)
				if strings.Contains(msgOutro.Message, "respOK") {
					module.outDbg(" ---->>> ME RESPONDERAM OK <<<---- " + msgOutro.Message)
					module.handleUponDeliverRespOk(msgOutro) // outro processo deu permissão

				} else if strings.Contains(msgOutro.Message, "reqEntry") {
					module.outDbg(" ---->>> ME PEDIRAM PARA USAR A SC <<<---- " + msgOutro.Message)
					module.handleUponDeliverReqEntry(msgOutro) // outro processo quer a SC

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
//  1. Para cada processo que estava esperando (waiting[i] == true), envia "respOK"
//  2. Limpa a fila de espera (todos waiting = false)
//  3. Muda o estado para noMX (não quer mais a SC)
func (module *DIMEX_Module) handleUponReqExit() {
	module.outDbg("ENTREI NO handleUponReqExit")
	module.outDbg(" ---->>> VOU SAIR DA SC <<<----")

	// Percorre todos os processos e envia OK para quem estava esperando
    for i, isWaiting := range module.waiting { 
        if isWaiting {							// vê quem ta esperando e avisa a todos que saiu
            module.sendToLink(module.processes[i], "respOK", " ")
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
	module.outDbg(fmt.Sprintf(" ---->>> RESPOSTA OK RECEBIDA DE %s. NUM DE MGS: %d <<<---- ", msgOutro.From, module.nbrResps))

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
        module.sendToLink(module.processes[id], "respOK", " ")

    // CASO 2: nega permissão (coloca na fila de espera)
    // → eu estou na SC (inMX)
    // → eu quero e EU pedi antes (eu tenho prioridade)
    } else if module.st == inMX || (module.st == wantMX && before(module.id, module.reqTs, id, ts)) {
		module.outDbg(" ---->>> VAI TER QUE ESPERAR <<<---- ")
		module.waiting[id] = true // mando esperar na fila
    }

    // Atualiza relógio lógico se o timestamp recebido for mais recente
	if ts > module.lcl{
		module.outDbg("Atualizando relógio lógico local")
        module.lcl = ts
    }
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
		return true           // timestamp menor = pediu antes = tem prioridade
	} else if oneTs > othTs {
		return false          // timestamp maior = pediu depois = não tem prioridade
	} else {
		return oneId < othId  // timestamps iguais: desempata pelo id menor
	}
}

// outDbg imprime uma mensagem de debug formatada, apenas se o modo debug estiver ativo
func (module *DIMEX_Module) outDbg(s string) {
	if module.dbg {
		fmt.Println(". . . . . . . . . . . . [ DIMEX : " + s + " ]")
	}
}