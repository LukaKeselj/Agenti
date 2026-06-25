# Pametna kuća – Federated Learning + Actor Model u Go

Upravljanje IoT uređajima u pametnoj kući pomoću **federativnog učenja (FedAvg)** i **aktorskog modela** (Go gorutine + kanali + gRPC).

## Arhitektura

```
                    +-----------+
                    |  Logger   |
                    +-----+-----+
                          |
        +-----------------+------------------+
        |         CoordinatorActor           |
        | (FedAvg agregacija, runde, gRPC)   |
        +--+-----------------------------+---+
           |                             |
     StartTraining               AdjustEnvironment
           |                             |
     +-----+-----+               +-------v-------+
     | Sensor A  |  ...          | DeviceCtrl    |
     | Sensor B  |  ModelUpdate  | (klima,       |
     | Sensor C  |  ----->       |  grejanje,    |
     +-----------+               |  rolete,      |
                                 |  svetlo)      |
                                 +---------------+
     +-------------------+
     | SupervisorActor   | <- heartbeat -> svi aktori
     +-------------------+
```

## Struktura

```
actor-framework/       # Generički aktorski radni okvir
  actor.go             # Actor interface, ActorRef
  mailbox.go           # Baferisani kanal (FIFO)
  context.go           # ActorContext, Become/Unbecome
  system.go            # ActorSystem (lifecycle)
  remote/              # gRPC transport + .proto
  supervision/         # SupervisorActor, strategije restarta

smart-home/            # FL aplikacija
  actors/              # Sensor, Coordinator, DeviceCtrl, Logger
  model/               # MLP (5→8→3), FedAvg agregacija
  data/                # IoT dataset (IID + non-IID)
  persistence/         # JSON serijalizacija stanja
  evaluation/          # MSE, RMSE, MAE, R² metrike

cmd/demo/              # Demo aplikacija
docker-compose.yml     # Docker Compose
Dockerfile             # Multi-stage Alpine build
```

## Pokretanje

### Lokalno

```bash
go run ./cmd/demo/
```

### Docker

```bash
docker compose up --build
```

## Rezultati

10 rundi FedAvg, 5 senzora, 800 uzoraka/senzoru, non-IID distribucija:

| Round | MSE       | RMSE      | R²        |
|-------|-----------|-----------|-----------|
| 1     | 0.060968  | 0.246918  | 0.227692  |
| 3     | 0.007007  | 0.083708  | 0.911239  |
| 5     | 0.005677  | 0.075345  | 0.928090  |
| 10    | 0.005652  | 0.075183  | 0.928398  |

## Tehnologije

- **Go** 1.26 – gorutine + kanali za aktorski model
- **gRPC** + **Protocol Buffers** – udaljena komunikacija
- **JSON** – perzistencija stanja aktora
- **Docker** + **Compose** – kontejnerizacija

## Specifikacija

Detaljna specifikacija: `Agenti_Keselj_tekst.txt`

**Student:** Luka Kešelj, RA 102/2022  
**Predmet:** Softverski agenti, 2026.
