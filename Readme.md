## Teste de interação com api do ZooKeeper

Iniciar ZooKeeper:

    docker run --name zookeeper-dev -p 2181:2181 -p 8080:8080 --restart always -d zookeeper:3.8.1

Executar aplicação:

    ./gradlew run

    