package gallery

//go:generate mockgen -source=./api/interfaces.go -destination=./mocks/api_mock.go -package=mocks
//go:generate mockgen -source=./command/repository.go -destination=./mocks/command_mock.go -package=mocks
//go:generate mockgen -source=./query/repository.go -destination=./mocks/query_mock.go -package=mocks
