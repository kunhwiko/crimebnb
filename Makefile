
help:
	@echo "make help"
	@echo "make build -- install dependencies for local development."


build:
	cd ./app/client/ && npm install
	cd ./app/server/ && npm install

client:
	docker-compose -f ./app/client/docker-compose.yml down --remove-orphans
	docker-compose -f ./app/client/docker-compose.yml up

server:
	docker-compose -f ./app/server/docker-compose.yml down --remove-orphans
	docker-compose -f ./app/server/docker-compose.yml up
