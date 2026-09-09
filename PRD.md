# Product Requirement Document (PRD)

## 1. Visão Geral do Produto

### 1.1 Nome do Projeto (Codinome)

HelixRack Core (Servidor HTTP/1.1 Nativo em Rust para Aplicações Rack/Grape)

### 1.2 Declaração do Problema

Servidores web tradicionais do ecossistema Ruby apresentam limitações estruturais em cenários de
tráfego intenso e restrição de recursos (ex: 1 vCPU em pods Kubernetes):

* Puma (Threads do SO): Alto custo de context switching no kernel do Linux e disputa pela única
  trava do GVL (Global VM Lock), gerando footprint de memória elevado e degradação em P99.
* Falcon (Fibers/I-O Cooperativo): Suscetível a Head-of-Line Blocking e serialização de
  requisições quando o código Ruby/Grape executa I/O síncrono clássico (gems C sem suporte ao
  async scheduler) ou operações CPU-bound de curta duração.

### 1.3 Solução Proposta

Um servidor web minimalista e ultra-performático construído em Rust, embarcando diretamente o
runtime do CRuby via C-API (rb-sys/magnus). O motor utiliza um Event Loop de thread única (Tokio
current_thread) com suporte a io_uring, assumindo todo o ciclo de vida de rede, parsing de
protocolo e gerenciamento de tempo da VM do Ruby, oferecendo transparência total para aplicações
Grape/Rack existentes.

## 2. Objetivos e Métricas de Sucesso

### 2.1 Objetivos de Negócio e Engenharia

* Eficiência em Single-Core: Garantir o menor tempo de espera na fila (Queue Time) e máximo
  throughput (RPS) sob o limite estrito de 1 vCPU.
* Compatibilidade Transparente: Executar qualquer aplicação Grape/Rack sem necessidade de alterar
  código Ruby, gems de banco de dados ou adotar drivers assíncronos.
* Zero Footprint Desnecessário: Eliminar alocações repetitivas de memória no Garbage Collector do
  Ruby durante a fase de parsing HTTP.

### 2.2 Métricas Chave de Desempenho (KPIs)

* P99 Latency: Redução de no mínimo 40% no P99 em relação ao Puma (1 worker, N threads) em
  cenários de alta concorrência em 1 vCPU.
* Uso de Memória RAM: Consumo estático e dinâmico de memória (RSS) até 50% menor em comparação ao
  Puma rodando a mesma carga.
* Throughput (RPS): Manutenção do throughput estável mesmo sob saturação de conexões em
  Keep-Alive, sem estourar a fila do backlog do socket Linux.

## 3. Escopo do Produto

### 3.1 O que ESTÁ NO ESCOPO (In-Scope)

* Protocolo: Suporte exclusivo a HTTP/1.1 com HTTP Keep-Alive nativo e persistente.
* Interface do Servidor: Implementação estrita do protocolo Rack (Rack SPEC).
* Motor de I/O: Tokio assíncrono em modo current_thread com backend io_uring para Linux moderno
  (fallback automático para epoll).
* Interface C-API / GVL:
  * Captura de pontos de liberação do GVL (rb_thread_call_without_gvl) durante I/O de gems
    externas.
  * Mecanismo de preempção cooperativa controlada utilizando hooks de instrução da VM do Ruby
    (rb_postponed_job).
* Construção de env Zero-Copy: Montagem direta das estruturas C da Hash do Rack na memória interna
  da VM antes da execução do `.call`.
* Empacotamento: Compilação em binário estático e distribuição em formato de RubyGem nativa.

### 3.2 O que NÃO ESTÁ NO ESCOPO (Out-of-Scope)

* Terminação TLS / HTTPS: O servidor rodará exclusivamente em HTTP limpo (Porta 80/8080). A
  criptografia TLS deve ser resolvida na camada de Ingress / Reverse Proxy (ex: Envoy, NGINX ou
  AWS ALB).
* WebSockets / Server-Sent Events (SSE): Sem suporte a conexões bidirecionais duradouras ou
  upgrade de protocolo HTTP.
* HTTP/2 e HTTP/3 (QUIC): Sem suporte a multiplexação L7 nativa no binário (delegado ao Ingress
  L7 se necessário).
* Servimento de Arquivos Estáticos: O servidor não implementará suporte a static file serving ou
  chamadas de sendfile.

## 4. Requisitos Funcionais

### 4.1 Ciclo de Vida da Requisição e Protocolo Rack

* RF01 - Aceite e Parsing HTTP/1.1: O motor em Rust deve realizar o parsing de verbos HTTP,
  headers e query strings utilizando alocação zero (zero-copy parsing via httparse) diretamente
  dos buffers lidos via io_uring.
* RF02 - Mapeamento da Hash env: O servidor deve traduzir a requisição HTTP em uma Hash válida
  segundo o protocolo Rack contendo obrigatoriamente: `REQUEST_METHOD`, `PATH_INFO`,
  `QUERY_STRING`, `SERVER_NAME`, `SERVER_PORT`, `rack.version`, `rack.input`, `rack.errors`,
  `rack.url_scheme` (http).
* RF03 - Invocação do App Grape: O servidor deve invocar o método `.call(env)` do objeto da
  aplicação Ruby previamente carregado.
* RF04 - Tradução e Stream de Resposta: O servidor deve receber o retorno de 3 elementos
  `[status, headers, body]` do Rack e canalizar os bytes da resposta de volta ao socket TCP sem
  acumular todo o payload na RAM.
* RF05 - Gerenciamento de Keep-Alive: Manter sockets TCP abertos para reuso de requisições do
  mesmo cliente, aplicando timeouts configuráveis de inatividade e limite máximo de requisições
  por conexão.

### 4.2 Integração com o Runtime do CRuby

* RF06 - Gerenciamento do GVL: O motor em Rust deve requisitar a posse do GVL estritamente para a
  chamada do `.call(env)` e liberar o GVL durante o parsing de novas requisições de rede ou
  encerramento do socket.
* RF07 - Time-Slicing e Interrupção: O motor em Rust deve monitorar a duração de execução da
  chamada Ruby. Se uma requisição ultrapassar um limiar configurável (ex: N ms) sem liberar o
  GVL, o Rust deve disparar um job pausado (rb_postponed_job) para permitir que o Event Loop
  processe eventos de I/O pendentes no socket.

## 5. Requisitos Não-Funcionais e Arquitetura do Sistema

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ CONTAINER / POD KUBERNETES (1 vCPU)                                          │
│                                                                                │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │ WORKER RUST (Tokio current_thread)                                     │  │
│  │                                                                          │  │
│  │  1. Socket TCP (Keep-Alive) ──> io_uring / epoll                        │  │
│  │  2. Parser HTTP (httparse) ──> Zero-Copy Buffer                         │  │
│  │  3. Preenche estrutura C da Hash Rack `env`                             │  │
│  └───────────────────────────────────┬────────────────────────────────────┘  │
│                                       │                                       │
│                                  (Captura GVL)                                │
│                                       │                                       │
│  ┌───────────────────────────────────▼────────────────────────────────────┐  │
│  │ CRUBY RUNTIME (VM)                                                      │  │
│  │                                                                          │  │
│  │  4. GrapeApp.call(env) ──> [ Status, Headers, Body ]                    │  │
│  │  5. Executa lógica de negócio / Serialização JSON                       │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 5.1 Performance e Escala

* RNF01 - Modelo de Concorrência: Funcionamento estrito em thread única de CPU (rt do Tokio) no
  lado do Rust, sem criação de thread pools adicionais no SO, eliminando contenção por mutexes
  em single-core.
* RNF02 - Mecanismo de I/O: Utilização de ring buffers compartilhados (io_uring) para submissão e
  conclusão de I/O de rede com zero syscalls no modo polling (SQPOLL), quando suportado pelo
  kernel Linux.
* RNF03 - Alocador de Memória: O binário nativo compilado deve utilizar jemalloc ou mimalloc para
  gerenciar a heap do servidor, evitando fragmentação de memória.

### 5.2 Confiabilidade e Operação

* RNF04 - Tratamento de Panics / Crashes: Exceções do Ruby dentro do Grape devem ser capturadas e
  convertidas para uma resposta HTTP 500 Internal Server Error sem causar panic no motor em Rust
  ou Segfault no processo.
* RNF05 - Life Cycle & Shutdown Gracioso: O servidor deve responder aos sinais SIGTERM e SIGINT
  do SO/Kubernetes:
  * Para de aceitar novas conexões TCP.
  * Aguarda o término das requisições HTTP ativas dentro de um limite (grace period).
  * Encerra o processo de forma limpa (PID 1).

## 6. Configuração e Interfaces de Uso

O servidor será empacotado como uma gem chamada `helix_rack`.

### 6.1 Inicialização via CLI

```sh
$ bundle exec helix_rack -a config.ru -p 8080 -o 0.0.0.0 --max-keepalive 10000
```

### 6.2 Parâmetros de Configuração

| Parâmetro | Tipo | Padrão | Descrição |
|---|---|---|---|
| `-a, --app` | String | `config.ru` | Caminho para o arquivo Rackup / Grape App. |
| `-p, --port` | Integer | `8080` | Porta TCP de escuta. |
| `-b, --bind` | String | `0.0.0.0` | IP de interface de rede. |
| `--keep-alive-timeout` | Integer | `15` | Timeout em segundos para fechar conexões ociosas. |
| `--max-keepalive` | Integer | `10000` | Número máximo de requisições por conexão TCP antes do encerramento forçado (`Connection: close`). |
| `--cpu-time-slice` | Integer | `5` | Limite de tempo (ms) contínuo no Ruby antes da sinalização de pausa. |

## 7. Planos de Testes e Validação

### 7.1 Validação de Compatibilidade Rack

* Rodar a suíte oficial de testes da Rack Spec para garantir compliance total com o contrato do
  `[status, headers, body]`.
* Testes de integração montando uma aplicação Grape completa contendo validação de parâmetros,
  rotas aninhadas, middlewares de erro e serialização JSON.

### 7.2 Benchmarking de Performance (1 vCPU Limit)

* Ambiente: Pod Kubernetes limitado a `cpus: "1.0"` e `memory: "512Mi"`.
* Ferramenta de Carga: k6 ou wrk injetando tráfego variado (100 a 5.000 conexões concorrentes
  HTTP Keep-Alive).
* Cenários Comparativos (HelixRack vs Puma vs Falcon):
  * Payload Leve (Hello World / Ping): Avaliação da capacidade máxima do parser de rede.
  * I/O Misto (Grape + Query PostgreSQL): Validação da captura da liberação do GVL.
  * CPU-Bound Leve (Grape + Serialization JSON): Avaliação da eficácia do mecanismo de
    preempção contra a serialização da fila.
