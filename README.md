# 1. Fundamentos de Sockets TCP

## 1.1. Conceito e Funcionamento do Socket TCP
Um socket atua como uma interface de software (API) entre o processo de aplicação e a camada de transporte dentro de um hospedeiro, funcionando de maneira análoga a uma porta por onde as mensagens entram e saem da rede. O protocolo TCP é orientado a conexão, o que exige que o cliente e o servidor realizem um processo de apresentação inicial em três etapas (three-way handshake) para estabelecer os parâmetros antes do envio dos dados da aplicação.
<div align="center">
  <img src="imagens/three_way_hand_shake.png" width="80%" />
</div>
A conexão resultante é full-duplex, dados podem fluir em ambas as direções simultaneamente, e ponto a ponto.
Enquanto um socket UDP é identificado por uma tupla de dois elementos (IP de destino e porta de destino), um socket de conexão TCP é identificado de forma única por uma tupla de quatro elementos: endereço IP de origem, porta de origem, endereço IP de destino, porta de destino.
<div align="center">
  <img src="imagens/comp_socket_udp_tcp.png" width="80%" />
</div>

## 1.2. Funcionamento do Buffer TCP e Controle de Fluxo
Ao estabelecer uma conexão TCP, cliente e servidor alocam recursos contendo variáveis de estado e buffers de envio e de recepção. Quando o processo de aplicação escreve dados no socket, os bytes são depositados no buffer de envio do TCP. O TCP retira pedaços de dados desse buffer conforme sua própria conveniência e os empacota em segmentos limitados pelo Tamanho Máximo do Segmento (Maximum Segment Size - MSS).
