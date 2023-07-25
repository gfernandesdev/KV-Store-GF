import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Server {
    private String serverIP;
    private int serverPort;
    private boolean isLeader = false;
    private Server[] servers; // Array para armazenar todas as instâncias dos servidores

    private Map<String, String> keyValueStore = new HashMap<>();

    public Server(String serverIP, int serverPort, Server[] servers) {
        this.serverIP = serverIP;
        this.serverPort = serverPort;
        this.servers = servers;
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter Server 1 IP: ");
        String server1IP = scanner.nextLine();
        System.out.print("Enter Server 1 Port: ");
        int server1Port = scanner.nextInt();

        System.out.print("Enter Server 2 IP: ");
        String server2IP = scanner.next();
        System.out.print("Enter Server 2 Port: ");
        int server2Port = scanner.nextInt();

        System.out.print("Enter Server 3 IP: ");
        String server3IP = scanner.next();
        System.out.print("Enter Server 3 Port: ");
        int server3Port = scanner.nextInt();

        Server[] servers = {
                new Server(server1IP, server1Port, null),
                new Server(server2IP, server2Port, null),
                new Server(server3IP, server3Port, null)
        };

        // Configurar o array de servidores para cada instância
        servers[0].servers = servers;
        servers[1].servers = servers;
        servers[2].servers = servers;

        System.out.print("Select the leader server (1-3): ");
        int leaderIndex = scanner.nextInt();
        leaderIndex--; // Adjusting to zero-based indexing

        // Criar um array de ServerSocket para os três servidores
        ServerSocket[] serverSockets = new ServerSocket[3];

        try {
            for (int i = 0; i < servers.length; i++) {
                if (i == leaderIndex) {
                    servers[i].setLeader();
                }
                serverSockets[i] = new ServerSocket(servers[i].serverPort);
                System.out.println("Server " + (i + 1) + " started on " + servers[i].serverIP + ":" + servers[i].serverPort);
            }

            // Criar um array de ExecutorService para os três servidores
            ExecutorService[] executorServices = new ExecutorService[3];

            int numThreads = 5; // Defina o número de threads que deseja (ajuste conforme necessário)
            for (int i = 0; i < servers.length; i++) {
                executorServices[i] = Executors.newFixedThreadPool(numThreads);
                int finalI = i;
                executorServices[i].execute(() -> servers[finalI].start(serverSockets[finalI]));
            }

            // Esperar que os servidores terminem suas tarefas
            for (ExecutorService executorService : executorServices) {
                executorService.shutdown();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setLeader() {
        isLeader = true;
        System.out.println("This server is the leader");
    }

    private void start(ServerSocket serverSocket) {
        try {
            System.out.println("Server started on " + serverIP + ":" + serverPort);

            // Criar um ThreadPoolExecutor com um número adequado de threads
            int numThreads = 5; // Defina o número de threads que deseja (ajuste conforme necessário)
            ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println(numThreads);
                System.out.println("Client connected: " + clientSocket.getInetAddress().getHostAddress());

                ClientHandler clientHandler = new ClientHandler(clientSocket, this);
                executorService.execute(clientHandler);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private synchronized void put(String key, String value) {
        keyValueStore.put(key, value);
    }

    private synchronized String get(String key) {
        return keyValueStore.get(key);
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
                    String[] command = requestMessage.getComando().split(":");
                    String response;

                    if (command[0].equals("PUT")) {
                        String key = requestMessage.getKey();
                        String value = requestMessage.getValue();

                        if (server.isLeader) {
                            System.out.println("Cliente " + clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort() + " PUT key:" + key + " value:" + value);
                            put(key, value);
                            response = "PUT_OK key: " + key + " value:" + value + " realizada no servidor " + server.serverIP + ":" + server.serverPort;
                            replicateData(key, value);
                        } else {
                            System.out.println("Encaminhando PUT key:" + key + " value:" + value);
                            String leaderResponse = forwardPutToLeader(key, value);
                            response = "Forwarded PUT to leader. Response: " + leaderResponse;
                        }
                    } else if (command[0].equals("GET")) {
                        String key = requestMessage.getKey();
                        String value = get(key);
                        response = (value != null) ? "GET key: " + key + " value: " + value + " obtido do servidor " + server.serverIP + ":" + server.serverPort : "Key not found";
                    } else if (command[0].equals("REPLICATE")) {
                        String key = requestMessage.getKey();
                        String value = requestMessage.getValue();
                        put(key, value);
                        response = "REPLICATION_OK";
                    } else {
                        response = "Invalid command";
                    }

                    // Create a Message object for the response
                    Mensagem responseMessage = new Mensagem(response);

                    if (response.equals("Forwarded PUT to leader. Response: REPLICATION_OK")) {
                        // Send an empty response to the client after forwarding the PUT request
                        responseMessage = new Mensagem("");
                    }

                    out.writeObject(responseMessage);
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            } finally {
                try {
                    /// Fechando o socket e os streams de I/O do cliente
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

        private void replicateData(String key, String value) {
            // Send PUT request to other servers for replication
            for (int i = 0; i < servers.length; i++) {
                if (servers[i] == server) continue; // Skip the current server (leader)
                try {
                    Socket replicationSocket = new Socket(servers[i].serverIP, servers[i].serverPort);
                    ObjectOutputStream replicationOut = new ObjectOutputStream(replicationSocket.getOutputStream());
                    ObjectInputStream replicationIn = new ObjectInputStream(replicationSocket.getInputStream());

                    // Create a Message object for the replication request
                    Mensagem requestMessage = new Mensagem("REPLICATE", key, value);
                    replicationOut.writeObject(requestMessage);

                    // Wait for the response from the server
                    Mensagem responseMessage = (Mensagem) replicationIn.readObject();
                    System.out.println("Replication response from " + servers[i].serverIP + ":" + servers[i].serverPort + ": " + responseMessage.getResponse());

                    replicationOut.close();
                    replicationIn.close();
                    replicationSocket.close();
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }

        private String forwardPutToLeader(String key, String value) {
            try {
                Socket leaderSocket = new Socket(servers[0].serverIP, servers[0].serverPort);
                ObjectOutputStream leaderOut = new ObjectOutputStream(leaderSocket.getOutputStream());
                ObjectInputStream leaderIn = new ObjectInputStream(leaderSocket.getInputStream());

                // Create a Message object for the PUT request
                Mensagem requestMessage = new Mensagem("PUT", key, value);
                leaderOut.writeObject(requestMessage);

                // Wait for the response from the leader
                Mensagem responseMessage = (Mensagem) leaderIn.readObject();

                leaderOut.close();
                leaderIn.close();
                leaderSocket.close();

                return responseMessage.getResponse();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }

            return "Leader request failed";
        }
    }
}


