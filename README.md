```markdown
# Smart Port TOS - Minimum Viable Product (MVP)

Este repositório contém o código-fonte do Minimum Viable Product (MVP) para um Terminal Operating System (TOS) focado em monitoramento portuário inteligente. A arquitetura foi concebida para operar sob o paradigma Event-Driven (Orientada a Eventos), especializando-se na ingestão assíncrona de telemetria de hardware (sensores de calado) e no gerenciamento de estado de embarcações durante operações de carga e descarga.

## 1. Arquitetura do Sistema

O sistema implementa um padrão de Gateway Híbrido, isolando a camada de rede por meio do modelo de concorrência nativo da linguagem Go (Goroutines). Esta topologia garante alta disponibilidade e mitiga o bloqueio de I/O em operações simultâneas:

* **Ingestão de Borda (Edge Ingestion - MQTT):** Responsável por recepcionar payloads de dispositivos IoT físicos alocados nas embarcações, operando sobre o protocolo TCP (Porta 1883) com baixo overhead estrutural.
* **Mensageria e Buffer (Apache Kafka):** Atua como middleware de desacoplamento. Garante a retenção e o ordenamento dos dados temporais, provendo resiliência contra picos de requisições (backpressure) e isolando as falhas da camada de persistência.
* **Persistência Time-Series (PostgreSQL):** Modelagem de dados orientada a inserções contínuas (Append-Only) com tipagem de ponto flutuante de dupla precisão (`float64`), vital para a auditoria de variações milimétricas do calado.
* **Exposição de Dados (HTTP/REST):** Interface síncrona para consumo por aplicações cliente (Dashboards, ERPs), provendo o estado consolidado da embarcação em formato JSON.

## 2. Stack Tecnológica

* **Linguagem de Programação:** Go (Golang) 1.21+
* **Banco de Dados Relacional:** PostgreSQL 15
* **Object-Relational Mapping (ORM):** GORM
* **Broker de Mensageria:** Apache Kafka 7.4.0 (Confluent Image)
* **Broker IoT:** Eclipse Mosquitto (MQTT 5.0)
* **Roteamento HTTP:** Gorilla Mux
* **Orquestração de Infraestrutura:** Docker e Docker Compose

## 3. Topologia de Diretórios

```text
internal/
├── core/
│   ├── domain/       # Modelagem de entidades e interfaces (Ship, TelemetryEvent, ShipTelemetry)
│   └── services/     # Casos de uso e regras de negócio de domínio (PortService)
├── handlers/
│   ├── repository/   # Implementação concreta da camada de acesso a dados (Postgres)
│   └── http/         # Controladores REST e Middlewares
├── kafka/            # Implementação de Produtores e Consumidores (Segmentio kafka-go)
└── mqtt/             # Cliente assinante para ingestão IoT (Eclipse Paho)
cmd/
└── server/
    └── main.go       # Ponto de entrada, injeção de dependências e orquestração de Goroutines

```

## 4. Procedimentos de Inicialização

### 4.1. Provisionamento de Infraestrutura

Execute o orquestrador de containers no diretório raiz para inicializar os serviços de mensageria e banco de dados:

```bash
docker-compose up -d

```

### 4.2. Resolução de Dependências

Valide e instale os pacotes Go declarados no `go.mod`:

```bash
go mod tidy

```

### 4.3. Execução do Binário

Inicie o serviço do Gateway IoT e da API REST:

```bash
go run cmd/server/main.go

```

## 5. Especificações de Interface (Contratos)

### 5.1. Ingestão de Dados (MQTT)

O broker Mosquitto aceita publicações no tópico estipulado para a atualização do estado da embarcação.

* **Tópico:** `v1/telemetry/draft`
* **Esquema de Payload (JSON):**

```json
{
  "ship_id": "SHIP-123",
  "draft": 10.50,
  "timestamp": "2026-03-11T10:00:00Z"
}

```

### 5.2. Consulta de Estado (HTTP REST)

Endpoint síncrono para a recuperação do modelo de dados da embarcação.

* **Rota:** `GET /ships/{id}`
* **Host Padrão:** `http://localhost:8080`

## 6. Ambiente de Simulação (Mock)

A atual versão do arquivo `main.go` instacia uma Goroutine geradora de carga (Mock). Este processo injeta eventos sintéticos de incremento de calado (+0.1m) a cada 5 segundos no produtor Kafka, visando validar a integridade dos fluxos de escrita e leitura (Write/Read Paths) na ausência temporária de hardware físico.

## 7. Roadmap e Próximas Implementações

As seguintes demandas arquiteturais e regras de negócio estão mapeadas para o próximo ciclo de desenvolvimento:

1. **Segurança e Autenticação (Read Path):**
* Implementação de um Middleware HTTP.
* Validação de tokens JWT (JSON Web Tokens) para restringir o acesso aos endpoints da API REST, garantindo que apenas sistemas autorizados consultem a telemetria portuária.


2. **Motor de Regras de Negócio Dinâmico (Core Domain):**
* **Cálculo de Conclusão de Operação:** Desenvolver a lógica no `PortService` para avaliar as métricas de profundidade.
* **Parâmetros Específicos por Navio:** Estruturar a entidade `Ship` para conter propriedades limitantes (thresholds) de calado máximo e mínimo. A regra deverá considerar que a marcação de fim de descarga ou carregamento varia de acordo com as especificações físicas e a capacidade de lastro de cada embarcação de forma individual.


3. **Transição para Ambiente de Produção (Google Cloud Platform - GCP):**
* **Desativação do Ambiente Local:** Migração da execução em `localhost` e substituição do `docker-compose` por infraestrutura em nuvem gerenciada.
* **Containerização e Orquestração:** Implantação da aplicação Go nativa no Google Kubernetes Engine (GKE) ou no Google Cloud Run para escalabilidade automática.
* **Banco de Dados Gerenciado:** Transição do PostgreSQL local para o Cloud SQL for PostgreSQL, assegurando alta disponibilidade e backups automatizados.
* **Roteamento e Rede:** Configuração de um Cloud Load Balancer para gerenciar o tráfego de entrada na API REST (HTTP) e a ingestão de dados dos sensores IoT (MQTT TCP).



```

```
