package hellozoo;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Servidor implements Runnable, Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(Servidor.class);

    enum Mensagem {
        PESQUISAR_SERVIDORES, PARAR
    }

    private String nome;
    private boolean executando;
    private List<String> servidoresConhecidos = List.of();

    private ZooKeeper zookeeper;
    private BlockingQueue<Mensagem> filaDeProcessamento = new ArrayBlockingQueue<>(10);

    public Servidor(String nome, String stringDeConexao) throws IOException {
        this.nome = nome;
        zookeeper = new ZooKeeper(stringDeConexao, 2000, this);
    }

    @Override
    public void run() {
        try {
            registrar();
            pesquisarServidores();
            while (executando) {
                processarMensagem();
            }
        } catch (Exception e) {
            LOG.error("", e);
        } finally {
            encerrarConexao();
        }
    }

    private void registrar() throws KeeperException, InterruptedException {
        LOG.info("===== {} iniciando =====", nome);
        var zNode = App.ZNODE_SERVIDORES + "/" + nome;
        zookeeper.create(zNode, "informações do servidor".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        this.executando = true;
    }

    private void pesquisarServidores() throws KeeperException, InterruptedException {
        this.servidoresConhecidos = zookeeper.getChildren(App.ZNODE_SERVIDORES, this)
                .stream()
                .filter(nomeDeServidor -> !nomeDeServidor.equals(nome))
                .toList();
        LOG.info("{} - Servidores conhecidos: {}", nome, this.servidoresConhecidos);
    }

    private void processarMensagem() throws KeeperException, InterruptedException {
        switch (filaDeProcessamento.take()) {
            case PESQUISAR_SERVIDORES -> pesquisarServidores();
            case PARAR -> executando = false;
        }
    }

    private void encerrarConexao() {
        LOG.info("===== {} parando =====", nome);
        try {
            zookeeper.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void parar() {
        filaDeProcessamento.offer(Mensagem.PARAR);
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case NodeChildrenChanged -> filaDeProcessamento.offer(Mensagem.PESQUISAR_SERVIDORES);
            case ChildWatchRemoved -> {}
            case DataWatchRemoved -> {}
            case NodeCreated -> {}
            case NodeDataChanged -> {}
            case NodeDeleted -> {}
            case None -> {}
            case PersistentWatchRemoved -> {}
        }
    }
}
