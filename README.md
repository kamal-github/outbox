![Github](https://github.com/kamal-github/outbox/workflows/Go/badge.svg)
[![Go Reference](https://pkg.go.dev/badge/github.com/kamal-github/outbox.svg)](https://pkg.go.dev/github.com/kamal-github/outbox)

Outbox
===

An implementation of [Transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html) pattern for reliable publishing the messages.

Infrastructure support
===

Currently, outbox worker can fetch outbox rows from below mentioned DB.
* Postgres
* MySQL

and can publish messages to below mentioned PubSub systems
* Amazon SQS
* RabbitMQ

Installation
===

```go
go get github.com/kamal-github/outbox
```

Usage
===
Please have a look at the well commented [examples](https://github.com/kamal-github/outbox/blob/main/worker_test.go)

Contribution
===

To run tests, run blow command, it will fetch all the prerequisites and run the tests.
```makefile
make test
```

Pull requests are welcome. Please fork it and send a pull request against main branch. Make sure to add tests ;)


# License
This project is licensed under the [MIT license](LICENSE).
