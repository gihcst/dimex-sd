/*  Construido como parte da disciplina: FPPD - PUCRS - Escola Politecnica
    Professor: Fernando Dotti  (https://fldotti.github.io/)
    Modulo representando Algoritmo de Exclusão Mútua Distribuída:
    Semestre 2023/1

	VERSÃO PROPOSITALMENTE COM FALHAS PARA ESTUDO DE INVARIANTES
	- Falha 1: concede a SC mesmo quando a regra de prioridade não permite
	- Falha 2: bloqueia indevidamente requisições e impede acesso legítimo
*/

package main

import (
	PP2PLink "SD/PP2PLink"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

type State int

const (
	noMX State = iota
	wantMX
	inMX
)

type dmxReq int

const (
	ENTER dmxReq = iota
	EXIT
)

type dmxResp struct{}

type FaultMode int

const (
	FaultViolateSC = iota
	FaultBlockSC
	FaultRandom
)

type ProcessSnapshot struct {
	SnId     int              `json:"snId"`
	Id       int              `json:"id"`
	State    string           `json:"st"`
	Lcl      int              `json:"lcl"`
	ReqTs    int              `json:"reqTs"`
	NbrResps int              `json:"nbrResps"`
	Waiting  []bool           `json:"waiting"`
	Channels map[int][]string `json:"channels"`
}

type SnapshotState struct {
	SnId            int
	RecordedLocal   bool
	LocalState      ProcessSnapshot
	RecordingFrom   map[int]bool
	MarkersReceived map[int]bool
}

type DIMEX_Module struct {
	Req       chan dmxReq
	Ind       chan dmxResp
	processes []string
	id        int
	st        State
	waiting   []bool
	lcl       int
	reqTs     int
	nbrResps  int
	faultMode FaultMode
	dbg       bool

	Pp2plink *PP2PLink.PP2PLink

	SnapReq         chan int
	activeSnapshots map[int]*SnapshotState
	snapFile        *os.File
}

func DIMAD(_addresses []string, _id int, _dbg bool) *DIMEX_Module {

	p2p := PP2PLink.NewPP2PLink(_addresses[_id], _dbg)

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
		st:              noMX,
		waiting:         make([]bool, len(_addresses)),
		lcl:             0,
		reqTs:           0,
		faultMode:       FaultRandom,
		dbg:             _dbg,
		Pp2plink:        p2p,
		SnapReq:         make(chan int, 100),
		activeSnapshots: make(map[int]*SnapshotState),
		snapFile:        sFile,
	}

	for i := 0; i < len(dmx.waiting); i++ {
		dmx.waiting[i] = false
	}
	dmx.Start()
	dmx.outDbg(" ---->>> Init DIMEX! <<<---- ")
	return dmx
}

func (module *DIMEX_Module) Start() {

	go func() {
		for {
			select {
			case dmxR := <-module.Req:
				if dmxR == ENTER {
					module.outDbg(" ---->>> PEDI PARA USAR A SC <<<---- ")
					module.handleUponReqEntry()

				} else if dmxR == EXIT {
					module.outDbg(" ---->>> SAI DA SC <<<---- ")
					module.handleUponReqExit()
				}

			case snId := <-module.SnapReq:
				module.handleInitiateSnapshot(snId)

			case msgOutro := <-module.Pp2plink.Ind:
				if strings.HasPrefix(msgOutro.Message, "marker,") {
					module.handleUponDeliverMarker(msgOutro.Message)
				} else {
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
						module.handleUponDeliverRespOk(msgOutro)

					} else if strings.Contains(msgOutro.Message, "reqEntry") {
						module.outDbg(" ---->>> ME PEDIRAM PARA USAR A SC <<<---- " + msgOutro.Message)
						module.handleUponDeliverReqEntry(msgOutro)
					}
				}
			}
		}
	}()
}

func (module *DIMEX_Module) handleUponReqEntry() {
	module.outDbg(" ---->>> ENTREI NA handleUponReqEntry <<<----")
	module.outDbg(" ---->>> VOU PEDIR ACESSO <<<----")
	module.lcl = int(time.Now().UnixNano())
	module.reqTs = module.lcl
	module.nbrResps = 0

	for i, address := range module.processes {
		if i != module.id {
			msg := fmt.Sprintf("reqEntry,%d,%d", module.id, module.reqTs)
			module.sendToLink(address, msg, "")
		}
	}

	module.st = wantMX
	module.outDbg(" ---->>> MANDEI MSG DE PEDIDO PARA TODO MUNDO <<<----")
}

func (module *DIMEX_Module) handleUponReqExit() {
	module.outDbg("ENTREI NO handleUponReqExit")
	module.outDbg(" ---->>> VOU SAIR DA SC <<<----")

	for i, isWaiting := range module.waiting {
		if isWaiting {
			module.sendToLink(module.processes[i], fmt.Sprintf("respOK,%d", module.id), " ")
		}
	}

	module.waiting = make([]bool, len(module.processes))
	module.st = noMX
	module.outDbg(" ---->>> LIBEREI A SC <<<----")
}

func (module *DIMEX_Module) handleUponDeliverRespOk(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	module.outDbg(" ---->>> ENTREI NA handleUponDeliverRespOk <<<----")

	module.nbrResps++
	module.outDbg(fmt.Sprintf(" ---->>> RESPOSTA OK RECEBIDA. NUM DE MGS: %d <<<---- ", module.nbrResps))

	if module.nbrResps == len(module.processes)-1 {
		module.Ind <- dmxResp{}
		module.st = inMX
		module.outDbg(" ---->>> ENTRANDO NA SC <<<----")
	}
}

func (module *DIMEX_Module) SetFaultMode(mode string) {
	switch strings.ToLower(mode) {
	case "violate":
		module.faultMode = FaultViolateSC
	case "block":
		module.faultMode = FaultBlockSC
	case "random":
		module.faultMode = FaultRandom
	default:
		module.faultMode = FaultRandom
	}
}

func (module *DIMEX_Module) handleUponDeliverReqEntry(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	var id, ts int
	fmt.Sscanf(msgOutro.Message, "reqEntry,%d,%d", &id, &ts)

	if module.faultMode == FaultRandom {
		if rand.Intn(2) == 0 {
			module.outDbg(" ---->>> [DIMAD] falha aleatoria: concedendo acesso indevido <<<----")
			module.sendToLink(module.processes[id], fmt.Sprintf("respOK,%d", module.id), " ")
		} else {
			module.outDbg(" ---->>> [DIMAD] falha aleatoria: bloqueando indevidamente <<<----")
			module.waiting[id] = true
		}
		if ts > module.lcl {
			module.lcl = ts
		}
		return
	}

	if module.faultMode == FaultViolateSC {
		module.outDbg(" ---->>> [DIMAD] falha: concede SC indevidamente <<<----")
		module.sendToLink(module.processes[id], fmt.Sprintf("respOK,%d", module.id), " ")
		if ts > module.lcl {
			module.lcl = ts
		}
		return
	}

	if module.faultMode == FaultBlockSC {
		module.outDbg(" ---->>> [DIMAD] falha: bloqueia indevidamente <<<----")
		module.waiting[id] = true
		if ts > module.lcl {
			module.lcl = ts
		}
		return
	}

	// comportamento correto da cópia original
	if module.st == noMX || (module.st == wantMX && before(id, ts, module.id, module.reqTs)) {
		module.sendToLink(module.processes[id], fmt.Sprintf("respOK,%d", module.id), " ")
	} else if module.st == inMX || (module.st == wantMX && before(module.id, module.reqTs, id, ts)) {
		module.waiting[id] = true
	}

	if ts > module.lcl {
		module.lcl = ts
	}
}

func (module *DIMEX_Module) StartSnapshot(snId int) {
	module.SnapReq <- snId
}

func (module *DIMEX_Module) handleInitiateSnapshot(snId int) {
	module.outDbg(fmt.Sprintf(" ---->>> INICIANDO SNAPSHOT %d <<<----", snId))

	snap := &SnapshotState{
		SnId:            snId,
		RecordedLocal:   true,
		LocalState:      module.captureLocalState(snId),
		RecordingFrom:   make(map[int]bool),
		MarkersReceived: make(map[int]bool),
	}

	for i := range module.processes {
		if i != module.id {
			snap.RecordingFrom[i] = true
			markerMsg := fmt.Sprintf("marker,%d,%d", snId, module.id)
			module.sendToLink(module.processes[i], markerMsg, " ")
		}
	}

	module.activeSnapshots[snId] = snap
}

func (module *DIMEX_Module) handleUponDeliverMarker(msg string) {
	var snId, fromId int
	n, err := fmt.Sscanf(msg, "marker,%d,%d", &snId, &fromId)
	if err != nil || n != 2 {
		module.outDbg("Erro ao parsear marker: " + msg)
		return
	}

	snap, exists := module.activeSnapshots[snId]
	if !exists {
		module.outDbg(fmt.Sprintf(" ---->>> 1º MARKER DO SNAPSHOT %d RECEBIDO DE %d <<<----", snId, fromId))

		snap = &SnapshotState{
			SnId:            snId,
			RecordedLocal:   true,
			LocalState:      module.captureLocalState(snId),
			RecordingFrom:   make(map[int]bool),
			MarkersReceived: make(map[int]bool),
		}

		snap.RecordingFrom[fromId] = false
		snap.MarkersReceived[fromId] = true

		for i := range module.processes {
			if i != module.id {
				markerMsg := fmt.Sprintf("marker,%d,%d", snId, module.id)
				module.sendToLink(module.processes[i], markerMsg, " ")
			}
		}

		for i := range module.processes {
			if i != module.id && i != fromId {
				snap.RecordingFrom[i] = true
			}
		}

		module.activeSnapshots[snId] = snap
	} else {
		module.outDbg(fmt.Sprintf(" ---->>> MARKER SUBSEQUENTE DO SNAPSHOT %d RECEBIDO DE %d <<<----", snId, fromId))
		snap.RecordingFrom[fromId] = false
		snap.MarkersReceived[fromId] = true
	}

	if len(snap.MarkersReceived) == len(module.processes)-1 {
		module.outDbg(fmt.Sprintf(" ---->>> SNAPSHOT %d COMPLETO NO PROCESSO %d! <<<----", snId, module.id))
		module.saveSnapshot(snap.LocalState)
		delete(module.activeSnapshots, snId)
	}
}

func (module *DIMEX_Module) captureLocalState(snId int) ProcessSnapshot {
	stStr := "noMX"
	if module.st == wantMX {
		stStr = "wantMX"
	} else if module.st == inMX {
		stStr = "inMX"
	}

	waitCopy := make([]bool, len(module.waiting))
	copy(waitCopy, module.waiting)

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

func (module *DIMEX_Module) saveSnapshot(snap ProcessSnapshot) {
	if module.snapFile != nil {
		data, err := json.Marshal(snap)
		if err == nil {
			module.snapFile.WriteString(string(data) + "\n")
			module.snapFile.Sync()
		}
	}
}

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

func (module *DIMEX_Module) sendToLink(address string, content string, space string) {
	module.outDbg(space + " ---->>>>   to: " + address + "     msg: " + content)
	module.Pp2plink.Req <- PP2PLink.PP2PLink_Req_Message{
		To:      address,
		Message: content}
}

func before(oneId, oneTs, othId, othTs int) bool {
	if oneTs < othTs {
		return true
	} else if oneTs > othTs {
		return false
	} else {
		return oneId < othId
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Uso: go run ./DIMAD <id> <endereco...>")
		fmt.Println("Exemplo: go run ./DIMAD 0 127.0.0.1:5000 127.0.0.1:6001 127.0.0.1:7002")
		return
	}

	id, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("id inválido:", os.Args[1])
		return
	}

	addresses := os.Args[2:]
	if len(addresses) == 0 {
		fmt.Println("forneça pelo menos um endereço de destino")
		return
	}

	dmx := DIMAD(addresses, id, true)
	fmt.Printf("DIMAD pronto: processo %d | enderecos=%v\n", id, addresses)

	file, err := os.OpenFile("./mxOUT.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("erro abriendo arquivo mxOUT.txt:", err)
		return
	}
	defer file.Close()

	time.Sleep(2 * time.Second)

	if id == 0 {
		go func() {
			totalSnapshots := 100
			for snId := 1; snId <= totalSnapshots; snId++ {
				time.Sleep(50 * time.Millisecond)
				dmx.StartSnapshot(snId)
			}
			fmt.Println("[DIMAD] snapshots disparados")
		}()
	}

	for {
		time.Sleep(300 * time.Millisecond)
		fmt.Println("[APP] pedido de acesso")
		dmx.Req <- ENTER
		fmt.Println("[APP] aguardando permissão")
		<-dmx.Ind

		_, _ = file.WriteString("|")
		fmt.Println("[APP] entrou na SC")
		time.Sleep(150 * time.Millisecond)
		_, _ = file.WriteString(".")
		fmt.Println("[APP] saiu da SC")
		dmx.Req <- EXIT
	}
}

func (module *DIMEX_Module) outDbg(s string) {
	if module.dbg {
		fmt.Println(". . . . . . . . . . . . [ DIMEX : " + s + " ]")
	}
}
