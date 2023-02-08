# Portfolio Balance

## Description

This service is for serving the computed portfolio balance of customers.
Most of the computing work is to be done by the data team which is eventually stored to
the database that this service connects to.

## Services
There are 2 kafka consumers:
1. Data point consumer to process 14M data point in 1 hour using bulk insert for 100K data per insert. This service has kafka writer to re-process duplicated data in different topic
2. Pending data point consumer to re-process duplicated data with slow insert

## How to Run
1. Data Point Consumer
- go to `cd cmd/data-point-consumer`
- run `go run main.go`
2. Pending data point consumer
- go to `cd cmd/pending-dp-consumer`
- run `go run main.go`