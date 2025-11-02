# Distributed Database Replication - Multicast communication 

Um sistema de comunicação peer to peer, no qual todos os 4 nós atuam como cliente e servidor, e estão trocando requisições entre si, e tentando sincronizar suas requisições ( o pior caso possível ).

As mensagens foram traduzidas em insert, query, update, e delete, simulando as requisições de um banco de dados.

As máquinas foram colocadas em diferentes localidades dos EUA, a fim de forçar a desordenação dos dados, pelo problema de localidade geográfica.
