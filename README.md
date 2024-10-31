# MyGeoEyeV2

Distributed system for 

# Main

# Datanodes

# utils/

Devem ser executados de dentro de MyGeoEyeV2 (usa caminhos relativos)

* service: controle de inicialização dos daemons (depende de {start|stop}_host.exp  {start|stop}_main.sh {start|stop}_datanode.sh) 
* setup_all: preparo inicial do ambiente nos nós (depende de setup_host.exp)
* update_all: atualiza arquivos fonte nos nós (depende de update_host.exp)