# 🚀 CampusLab Backend — Arquitectura de Microservicios
Este repositorio contiene la arquitectura backend del sistema CampusLab, diseñada mediante una arquitectura basada en microservicios, mensajería orientada a eventos y un patrón Backend for Frontend (BFF). El sistema facilita la gestión, reserva y reporte de uso de laboratorios y equipamiento institucional.


## 🏗️ Arquitectura del Sistema
El backend está dividido en 4 servicios independientes construidos con Spring Boot (Java 17) y desacoplados mediante contenedores Docker, apoyados por **RabbitMQ** (mensajería transaccional) y **Apache Kafka** (procesamiento de flujos en tiempo real y analítica):

```text
campuslab-backend/
├── ms-campuslab-bff/          # (Puerto 8080) Gateway / BFF, monitoreo Kafka y métricas en tiempo real
├── ms-campuslab-bookings/     # (Puerto 8081) Microservicio de Gestión de Reservas y Productor Kafka
├── ms-campuslab-catalog/      # (Puerto 8082) Microservicio de Catálogo y Consumidor de Disponibilidad
├── ms-campuslab-reports/      # (Puerto 8083) Microservicio de Reportes, Analítica y Procesamiento de Streams
└── infra/                     # Orquestación de infraestructura
    └── compose.yml            # Docker Compose para levantar microservicios, Kafka, Zookeeper, Kafka-UI y RabbitMQ
```

## 📦 Descripción de Componentes
1. ```ms-campuslab-bff``` (Puerto 8080): Actúa como puerta de entrada única para el cliente web (Angular). Se encarga del enrutamiento de peticiones mediante ```RestClient```, validación de tokens OAuth2/JWT (Azure AD / Entra ID), administración del clúster Kafka (```AdminClient```) y exposición de métricas analíticas en vivo.
2. ```ms-campuslab-bookings``` (Puerto 8081): Encargado de la creación, consulta y actualización de estados de reservas de laboratorios y equipos. Publica eventos en ```campuslab.bookings.stream``` y define la topología de tópicos mediante ```KafkaTopicConfig```.
3. ```ms-campuslab-catalog``` (Puerto 8082): Administra el catálogo de recursos disponibles (laboratorios, herramientas, equipos de medición) y sincroniza la disponibilidad en tiempo real consumiendo eventos desde Kafka (```catalog-availability-group```).
4. ```ms-campuslab-reports``` (Puerto 8083): Procesa eventos de reserva (```reports-analytics-group```) con tolerancia a fallos (```DefaultErrorHandler```), calcula métricas agregadas de uso y las publica en ```campuslab.analytics.metrics```.

## 🌊 Arquitectura de Streaming y Mensajería (Apache Kafka + RabbitMQ)

CampusLab implementa una arquitectura híbrida que combina RabbitMQ para colas transaccionales con Apache Kafka para distribución desacoplada de eventos y analítica en tiempo real.

### Topología de Tópicos Kafka
| Tópico | Particiones | Productor(es) | Consumidores (Consumer Groups) | Propósito |
| :----: | :---------: | :-----------: | :----------------: | :-------: |
| ```campuslab.bookings.stream``` | 3 | ```ms-campuslab-bookings``` | ```reports-analytics-group```(```ms-campuslab-reports```), ```catalog-availability-group``` (```ms-campuslab-catalog```) | Distribución en paralelo de eventos de reservas (```BOOKING_CREATED```, ```BOOKING_UPDATED```) para analítica y sincronización de disponibilidad. |
| ```campuslab.analytics.metrics``` | 3 | ```ms-campuslab-bookings``` | ```bff-analytics-group```(```ms-campuslab-bff```) | Publicación de métricas agregadas calculadas en tiempo real para exposición al frontend. |

### Resiliencia y Gobernanza

* Reintentos Controlados: ```ms-campuslab-reports``` incorpora un ```DefaultErrorHandler``` con ```FixedBackOff``` (3 reintentos cada 2 segundos) y validación defensiva de payloads para evitar bloqueos de partición ante mensajes inválidos.
* Gobernanza Explícita: El broker deshabilita la autocreación (```KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"```), delegando la creación controlada de tópicos y particiones a ```KafkaTopicConfig```.

## 🛠️ Requisitos Previos
Para ejecutar el backend de forma local o en un servidor (como AWS EC2), asegúrate de contar con:   
* Docker Desktop (o Docker Engine en Linux) v20.10+

* Docker Compose v2.0+

* Java 17 OpenJDK y Maven 3.8+ (Opcional, solo si deseas ejecutar los proyectos individualmente sin Docker).

## 🚦 Guía de Inicio Rápido (Con Docker Compose)
El método recomendado para levantar toda la solución es mediante Docker Compose:

### 1. Clonar el repositorio y entrar a la carpeta de infraestructura

```bash
git clone <URL_DEL_REPOSITORIO>
cd campuslab-backend/infra
```

### 2. Construir y levantar los contenedores
Ejecuta el siguiente comando para compilar las imágenes en paralelo y levantar los servicios en segundo plano:

```bash
docker compose up --build -d
```

### 3. Verificar que los servicios estén activos

```bash
docker compose ps
```

Deberías ver los 4 contenedores en estado ```Up / Running```:

* ```ms-campuslab-bff``` en ```http://localhost:8080```
* ```ms-campuslab-bookings``` en ```http://localhost:8081```
* ```ms-campuslab-catalog``` en ```http://localhost:8082```
* ```ms-campuslab-reports``` en ```http://localhost:8083```
* ```campuslab-kafka``` en ```localhost:9092``` (Listener interno Docker: ```kafka:29092```)
* ```campuslab-kafka-ui``` (Panel visual de Kafka) en ```http://localhost:8090```
* ```campuslab-rabbitmq``` (Panel Management) en ```httplocalhost:15672``` (```guest``` / ```guest```)

### 4. Monitorear Logs
Si deseas ver la consola de auditoría de algún servicio en particular (ej. el BFF):

```bash
docker compose logs -f ms-campuslab-bff
docker compose logs -f ms-campuslab-reports
```

## 🔗 Endpoints Principales (Consumo desde el Frontend)
Todas las peticiones desde el Frontend (Angular) deben ser dirigidas exclusivamente al BFF (http://```localhost:8080```):

| Servicio de Destino | Método | Endpoint en el BFF | Descripción |
| :----: | :---: | :-------: | :-------: |
| Catálogo | ```GET``` | ```/api/catalog/resources``` | Obtiene la lista completa de recursos |
| Catálogo | ```GET``` | ```/api/catalog/resources/{id}``` | Obtiene el detalle de un recurso específico |
| Reservas | ```GET``` | ```/api/bookings``` | Lista todas las reservas realizadas |
| Reservas | ```POST``` | ```/api/bookings``` | Crea una nueva solicitud de reserva |
| Reservas | ```PUT``` | ```/api/bookings/{id}/status``` | Actualiza el estado de una reserva |
| Reportes | ```GET``` | ```/api/report/summary``` | Obtiene métricas y porcentaje de ocupación |
| Kafka Admin (BFF) | ```GET``` | ```/api/bff/kafka/cluster``` | Consulta tópicos, particiones, réplicas y Consumer Groups activos (```Admin```, ```Tecnico```) |
| Kafka Metrics (BFF) | ```GET``` | ```/api/bff/kafka/metrics``` | Devuelve las métricas analíticas en tiempo real consumidas desde Kafka (```Admin```, ```Tecnico```) |
| Kafka Test (BFF) | ```POST``` | ```/api/bff/kafka/publish/{topic}``` | Publica un mensaje JSON de prueba hacia el tópico indicado (```Admin```, ```Tecnico```) |


## 🛑 Detener el Entorno
Para apagar y eliminar los contenedores e instancias de red creadas por Docker:

```bash
cd campuslab-backend/infra
docker compose down
```