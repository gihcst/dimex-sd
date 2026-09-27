// Construido como parte da disciplina: Sistemas Distribuidos - PUCRS - Escola Politecnica
//  Professor: Fernando Dotti  (https://fldotti.github.io/)
// Uso p exemplo:
//   go run usaDIMEX-f.go 0 127.0.0.1:5000  127.0.0.1:6001  127.0.0.1:7002
//   go run usaDIMEX-f.go 1 127.0.0.1:5000  127.0.0.1:6001  127.0.0.1:7002
//   go run usaDIMEX-f.go 2 127.0.0.1:5000  127.0.0.1:6001  127.0.0.1:7002
// ----------
// LANCAR N PROCESSOS EM SHELL's DIFERENTES, UMA PARA CADA PROCESSO.
// para cada processo fornecer: seu id único (0, 1, 2 ...) e a mesma lista de processos.
// o endereco de cada processo é o dado na lista, na posicao do seu id.
// no exemplo acima o processo com id=1  usa a porta 6001 para receber e as portas
// 5000 e 7002 para mandar mensagens respectivamente para processos com id=0 e 2
// -----------
// Esta versão supõe que todos processos tem acesso a um mesmo arquivo chamado "mxOUT.txt"
// Todos processos escrevem neste arquivo, usando o protocolo dimex para exclusao mutua.
// Os processos escrevem "|." cada vez que acessam o arquivo.   Assim, o arquivo com conteúdo
// correto deverá ser uma sequencia de
// |.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.
// |.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.
// |.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.|.
// etc etc ...     ....  até o usuário interromper os processos (ctl c).
// Qualquer padrao diferente disso, revela um erro.
//      |.|.|.|.|.||..|.|.|.  etc etc  por exemplo.
// Se voce retirar o protocolo dimex vai ver que o arquivo poderá entrelacar
// "|."  dos processos de diversas diferentes formas.
// Ou seja, o padrão correto acima é garantido pelo dimex.
// Ainda assim, isto é apenas um teste.  E testes são frágeis em sistemas distribuídos.

package main

import (
	"SD/DIMEX"
	"fmt"
	"os"
	"strconv"
	"time"
)

func main() {

	// Validação de argumentos: precisa do id + lista de endereços
	if len(os.Args) < 2 {
		fmt.Println("Please specify at least one address:port!")
		fmt.Println("go run useDIMEX-f.go 0 127.0.0.1:5000  127.0.0.1:6001  127.0.0.1:7002 ")
		fmt.Println("go run useDIMEX-f.go 1 127.0.0.1:5000  127.0.0.1:6001  127.0.0.1:7002 ")
		fmt.Println("go run useDIMEX-f.go 2 127.0.0.1:5000  127.0.0.1:6001  127.0.0.1:7002 ")
		return
	}

	// Lê o id do processo e a lista de endereços dos argumentos
	id, _ := strconv.Atoi(os.Args[1])
	addresses := os.Args[2:]
	fmt.Print("id: ", id, "   ")
	fmt.Println(addresses)

	// Cria o módulo DIMEX (que internamente cria o PP2PLink e começa a escutar)
	var dmx *DIMEX.DIMEX_Module = DIMEX.NewDIMEX(addresses, id, true)
	fmt.Println(dmx)

	// Abre arquivo que TODOS os processos devem poder usar
	file, err := os.OpenFile("./mxOUT.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// Aguarda 3 segundos para facilitar inicializacao de todos processos (a mão)
	time.Sleep(3 * time.Second)

	// Loop principal: acessa a SC repetidamente
	for {
		// PASSO 1: Pede acesso à seção crítica
		fmt.Println("[ APP id: ", id, " PEDE   MX ]")
		dmx.Req <- DIMEX.ENTER // envia pedido de ENTRADA para o módulo DIMEX

		// PASSO 2: Aguarda permissão (bloqueia até receber)
		fmt.Println("[ APP id: ", id, " ESPERA MX ]")
		<-dmx.Ind // bloqueia aqui até o DIMEX sinalizar que pode entrar

		// SEÇÃO CRÍTICA COMEÇA AQUI
		_, err = file.WriteString("|") // escreve "|" marcando ENTRADA na SC
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return
		}

		fmt.Println("[ APP id: ", id, " *EM*   MX ]")

		_, err = file.WriteString(".") // escreve "." marcando SAÍDA da SC
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return
		}
		// SEÇÃO CRÍTICA TERMINA AQUI

		// PASSO 3: Libera a seção crítica
		dmx.Req <- DIMEX.EXIT
		fmt.Println("[ APP id: ", id, " FORA   MX ]")
	}
}
