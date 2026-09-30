/*
  Construido como parte da disciplina: Sistemas Distribuidos - PUCRS - Escola Politecnica
  Professor: Fernando Dotti  (https://fldotti.github.io/)
  Modulo representando Perfect Point to Point Links tal como definido em:
    Introduction to Reliable and Secure Distributed Programming
    Christian Cachin, Rachid Gerraoui, Luis Rodrigues
  * Semestre 2018/2 - Primeira versao.  Estudantes:  Andre Antonitsch e Rafael Copstein
  * Semestre 2019/1 - Reaproveita conexões TCP já abertas - Estudantes: Vinicius Sesti e Gabriel Waengertner
  * Semestre 2020/1 - Separa mensagens de qualquer tamanho atee 4 digitos.
  Sender envia tamanho no formato 4 digitos (preenche com 0s a esquerda)
  Receiver recebe 4 digitos, calcula tamanho do buffer a receber,
  e recebe com io.ReadFull o tamanho informado - Dotti
  * Semestre 2022/1 - melhorias eliminando retorno de erro aos canais superiores.
  se conexao fecha nao retorna nada.   melhorias em comentarios.   adicionado modo debug. - Dotti
*/

package PP2PLink

import (
	"fmt"
	"io"
	"net"
	"strconv"
)

// PP2PLink_Req_Message é a mensagem que a camada de cima envia para o PP2PLink enviar pela rede
type PP2PLink_Req_Message struct {
	To      string // endereço de destino (ip:porta)
	Message string // conteúdo da mensagem
}

// PP2PLink_Ind_Message é a mensagem que o PP2PLink entrega para a camada de cima quando recebe algo da rede
type PP2PLink_Ind_Message struct {
	From    string // endereço de quem enviou
	Message string // conteúdo da mensagem recebida
}

// PP2PLink é a estrutura principal do módulo de comunicação
type PP2PLink struct {
	Ind   chan PP2PLink_Ind_Message // canal de INDICAÇÃO: PP2PLink → DIMEX (mensagens recebidas)
	Req   chan PP2PLink_Req_Message // canal de REQUISIÇÃO: DIMEX → PP2PLink (mensagens a enviar)
	Run   bool                      // flag indicando se o módulo está ativo
	dbg   bool                      // modo debug
	Cache map[string]net.Conn       // cache de conexões TCP abertas (evita reconectar toda hora)
}

// NewPP2PLink cria e inicializa um novo módulo PP2PLink.
// Recebe o endereço local (ip:porta) onde vai escutar conexões, e flag de debug.
func NewPP2PLink(_address string, _dbg bool) *PP2PLink {
	p2p := &PP2PLink{
		Req:   make(chan PP2PLink_Req_Message, 1),
		Ind:   make(chan PP2PLink_Ind_Message, 1),
		Run:   true,
		dbg:   _dbg,
		Cache: make(map[string]net.Conn)}
	p2p.outDbg(" Init PP2PLink!")
	p2p.Start(_address)
	return p2p
}

func (module *PP2PLink) outDbg(s string) {
	if module.dbg {
		fmt.Println(". . . . . . . . . . . . . . . . . [ PP2PLink msg : " + s + " ]")
	}
}

func (module *PP2PLink) Start(address string) {
	// Abre um socket TCP escutando no endereço local
	listen, err := net.Listen("tcp4", address)
	if err != nil {
		fmt.Printf(". . . . . . . . . . . . . . . . . [ PP2PLink msg : erro ao abrir listener em %s: %v ]\n", address, err)
		return
	}

	// GOROUTINE 1: LISTENER - recebe mensagens de outros processos
	go func() {
		for {
			// Aceita repetidamente tentativas novas de conexao
			conn, err := listen.Accept()
			if err != nil {
				module.outDbg("erro : " + err.Error() + " ao aceitar conexao.")
				return
			}
			module.outDbg("ok   : conexao aceita com outro processo.")

			// Para cada conexão aceita, lança uma goroutine de leitura
			go func(conn net.Conn) {
				for { // recebe mensagens enquanto a conexão estiver aberta
					// 1. Lê os 4 primeiros bytes (tamanho da mensagem em texto)
					bufTam := make([]byte, 4)
					_, err := io.ReadFull(conn, bufTam)
					if err != nil {
						module.outDbg("erro : " + err.Error() + " conexao fechada pelo outro processo.")
						_ = conn.Close()
						break
					}

					// 2. Converte o tamanho para int e lê exatamente essa quantidade de bytes
					tam, err := strconv.Atoi(string(bufTam))
					if err != nil {
						module.outDbg("erro : " + err.Error() + " ao converter tamanho da mensagem.")
						_ = conn.Close()
						break
					}
					bufMsg := make([]byte, tam)        // declara buffer do tamanho exato
					_, err = io.ReadFull(conn, bufMsg) // le do tamanho do buffer ou da erro
					if err != nil {
						fmt.Println("@", err)
						_ = conn.Close()
						break
					}

					// 3. Monta a mensagem e repassa para a camada de cima (DIMEX) via canal Ind
					msg := PP2PLink_Ind_Message{
						From:    conn.RemoteAddr().String(),
						Message: string(bufMsg)}
					module.Ind <- msg // repassa mensagem para modulo superior
				}
			}(conn)
		}
	}()

	// GOROUTINE 2: SENDER — envia mensagens para outros processos
	go func() {
		for {
			// Aguarda mensagem no canal Req (vinda do DIMEX) e envia
			message := <-module.Req
			module.Send(message)
		}
	}()
}

// Send envia uma mensagem para outro processo via TCP.
func (module *PP2PLink) Send(message PP2PLink_Req_Message) {
	var conn net.Conn
	var ok bool
	var err error

	// Verifica se já existe uma conexão aberta para o destino (cache)
	if conn, ok = module.Cache[message.To]; ok {
	} else { // se nao existe, abre e guarda na cache
		conn, err = net.Dial("tcp", message.To)
		module.outDbg("ok   : conexao iniciada com outro processo")
		if err != nil {
			fmt.Println(err)
			return
		}
		module.Cache[message.To] = conn
	}

	// Calcula o tamanho da mensagem e formata como string de 4 dígitos
	// Ex: mensagem de 6 chars → "0006"
	str := strconv.Itoa(len(message.Message))
	for len(str) < 4 {
		str = "0" + str // preenche com zeros à esquerda
	}
	if !(len(str) == 4) {
		module.outDbg("ERROR AT PPLINK MESSAGE SIZE CALCULATION - INVALID MESSAGES MAY BE IN TRANSIT")
	}
	_, err = fmt.Fprintf(conn, str)             // escreve 4 caracteres com tamanho
	_, err = fmt.Fprintf(conn, message.Message) // escreve a mensagem com o tamanho calculado
	if err != nil {
		module.outDbg("erro : " + err.Error() + ". Conexao fechada. 1 tentativa de reabrir:")
		conn, err = net.Dial("tcp", message.To)
		if err != nil {
			//fmt.Println(err)
			module.outDbg("       " + err.Error())
			return
		} else {
			module.outDbg("ok   : conexao iniciada com outro processo.")
		}
		module.Cache[message.To] = conn
		_, err = fmt.Fprintf(conn, str)             // escreve 4 caracteres com tamanho
		_, err = fmt.Fprintf(conn, message.Message) // escreve a mensagem com o tamanho calculado
	}
	return
}
