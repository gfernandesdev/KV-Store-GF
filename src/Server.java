import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Server {
    private String serverIP;
    private int serverPort;
    private boolean isLeader = false;
    private Server[] servers; // Array para armazenar todas as instâncias dos servidores

    //private Map<String, String> keyValueStore = new HashMap<>();
    private static HashMap<String, ValueTime> keyValueStore = new HashMap<>();
    private long timestamp_sys = System.currentTimeMillis();

    public static void main(String[] args) {
        ValueTime teste = new ValueTime("teste", 999999999);
        keyValueStore.put("teste-chave", teste);
        Scanner scanner = new Scanner(System.in);
        System.out.print("Digite o IP do Server: ");
        String serverIP = scanner.nextLine();
        System.out.print("Digite a porta do Server: ");
        int serverPort = scanner.nextInt();
        scanner.nextLine(); // Limpa o buffer

        Server server = new Server(serverIP, serverPort, null);
        server.start();

        scanner.close();
    }


    public Server(String serverIP, int serverPort, Server[] servers) {
        this.serverIP = serverIP;
        this.serverPort = serverPort;
        this.servers = servers;

        // Verifica se é o líder
        if (serverPort == 10097) {
            isLeader = true;
        }
    }

    private static class ValueTime {
        public String value;
        public long timestamp;

        public ValueTime(String value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }
    }
    private void start() {
        try {
            ServerSocket serverSocket = new ServerSocket(serverPort);

            // Criar um ThreadPoolExecutor com um número adequado de threads
            int numThreads = 5; // Defina o número de threads que deseja (ajuste conforme necessário)
            ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

            while (true) {
                Socket clientSocket = serverSocket.accept();

                ClientHandler clientHandler = new ClientHandler(clientSocket, this);
                executorService.execute(clientHandler);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private synchronized ValueTime put(String key, String value) {
        ValueTime value_time = new ValueTime(key, timestamp_sys);
        keyValueStore.put(key, value_time);
        return value_time;
    }

    private synchronized ValueTime get(String key) {
        ValueTime value = keyValueStore.get(key);
        if (value == null){
            return value;
        }
        else{
            if (value.timestamp >= timestamp_sys){
                return value;
            }
            else{
                ValueTime response = new ValueTime("TRY_OTHER_SERVER_OR_LATER", timestamp_sys);
                return response;
            }
        }

    }

    private class ClientHandler implements Runnable {
        private Socket clientSocket;
        private ObjectInputStream in;
        private ObjectOutputStream out;
        private Server server; // Referência para a instância do servidor

        public ClientHandler(Socket socket, Server server) {
            this.clientSocket = socket;
            this.server = server;
        }

        @Override
        public void run() {
            try {
                 in = new ObjectInputStream(clientSocket.getInputStream());
                 out = new ObjectOutputStream(clientSocket.getOutputStream());

                Mensagem requestMessage;

                while ((requestMessage = readMessage()) != null) {
                    if (requestMessage.comando == null) {
                        System.out.println("Mensagem vazia (NULL).");
                        continue; // Ignore this message and continue listening for the next one.
                    }

                    String response;

                    if (requestMessage.comando.equals("PUT")) {
                        String key = requestMessage.key;
                        String value = requestMessage.value;

                        if (server.isLeader) {
                            System.out.println("Cliente " + clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort() + " PUT key:" + key + " value:" + value);
                            ValueTime value_time = put(key, value);
                            replicateData(key, value, value_time);
                        } else {
                            System.out.println("Encaminhando PUT key:" + key + " value:" + value);
                            String leaderResponse = forwardPutToLeader(key, value);
                        }
                        response = "PUT_OK key: " + key + " value:" + value + " realizada no servidor " + server.serverIP + ":" + "10097";

                    } else if (requestMessage.comando.equals("GET")) {
                        String key = requestMessage.key;
                        ValueTime value = get(key);
                        response = (value != null) ? "GET key: " + key + " value: " + value.value + " obtido do servidor " + server.serverIP + ":" + server.serverPort +" meu timestamp"+ timestamp_sys +" e do servidor "+ value.timestamp : "NULL";
                        if(value != null ){
                            System.out.println("Cliente " + clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort() + " GET key:"+key+" ts:"+ timestamp_sys +". Meu ts é "+ value.timestamp +", portanto devolvendo "+ value.value);
                        }
                    } else if (requestMessage.comando.equals("REPLICATE")) {
                        String key = requestMessage.key;
                        String value = requestMessage.value;
                        put(key, value);
                        response = "REPLICATION_OK";
                        System.out.println("REPLICATION_OK");
                    } else {
                        response = "Invalid command";
                    }

                    // Create a Message object for the response
                    Mensagem responseMessage = new Mensagem(response);

                    //if (response.equals("PUT_OK key: " + requestMessage.key + " value:" + requestMessage.value + " encaminhado para o líder. Resposta do líder: REPLICATION_OK")) {
                        // Send an empty response to the client after forwarding the PUT request
                    //    responseMessage = new Mensagem("");
                    //}

                    out.writeObject(responseMessage);
                    out.flush(); // Certifica-se de que os dados são enviados imediatamente
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            } finally {
                try {
                    // Fechando o socket e os streams de I/O do cliente
                    out.close();
                    in.close();
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private Mensagem readMessage() throws IOException, ClassNotFoundException {
            try {
                return (Mensagem) in.readObject();
            } catch (EOFException e) {
                // O cliente fechou a conexão
                return null;
            }
        }

        private void replicateData(String key, String value, ValueTime valuet) {
            // Define os IPs e portas dos servidores para replicação
            String[] serverIPs = {"127.0.0.1", "127.0.0.1", "127.0.0.1"};
            int[] serverPorts = {10097, 10098, 10099};

            // Envia a requisição PUT para os outros servidores para replicação
            for (int i = 0; i < serverIPs.length; i++) {
                if (!serverIPs[i].equals(serverIP) || serverPorts[i] != serverPort) {
                    try {
                        Socket replicateSocket = new Socket(serverIPs[i], serverPorts[i]);
                        ObjectOutputStream replicateOut = new ObjectOutputStream(replicateSocket.getOutputStream());

                        Mensagem replicateMessage = new Mensagem("REPLICATE", key, value);
                        replicateOut.writeObject(replicateMessage);
                        replicateOut.flush(); // Certifica-se de que os dados são enviados imediatamente

                        ObjectInputStream replicateIn = new ObjectInputStream(replicateSocket.getInputStream());
                        Mensagem replicateResponse = (Mensagem) replicateIn.readObject();

                        replicateOut.close();
                        replicateIn.close();
                        replicateSocket.close();
                    } catch (IOException | ClassNotFoundException e) {
                        System.err.println("Error replicating data to server " + serverIPs[i] + ":" + serverPorts[i]);
                        e.printStackTrace();
                    }
                } else {

                }
            }
            System.out.println("Enviando PUT_OK ao Cliente " + clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort() + " da key:"+ key +" ts:" + valuet.timestamp);
        }

        private String forwardPutToLeader(String key, String value) {
            try {
                // Envia a requisição PUT para o líder

                Socket leaderSocket = new Socket("127.0.0.1", 10097);
                ObjectOutputStream leaderOut = new ObjectOutputStream(leaderSocket.getOutputStream());


                Mensagem requestMessage = new Mensagem("PUT", key, value);
                leaderOut.writeObject(requestMessage);
                leaderOut.flush(); // Certifica-se de que os dados são enviados imediatamente

                ObjectInputStream leaderIn = new ObjectInputStream(leaderSocket.getInputStream());
                Mensagem responseMessage = (Mensagem) leaderIn.readObject();

                leaderOut.close();
                leaderIn.close();
                leaderSocket.close();

                return responseMessage.response;
            } catch (IOException | ClassNotFoundException e) {
                System.err.println("Error forwarding PUT request to leader.");
                e.printStackTrace();
                return "Error forwarding PUT request to leader.";
            }
        }
    }
}
