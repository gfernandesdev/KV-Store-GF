import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

public class Client {
    private static Map<Integer, String> servers = new HashMap<>();
    private static Random random = new Random();

    public static void main(String[] args) {
        Client client = new Client();
        client.start();
    }

    private void start() {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("Options:\n1. PUT\n2. GET\n3. INIT\n4. EXIT");
            System.out.print("Enter option: ");
            int option = scanner.nextInt();
            scanner.nextLine(); // Limpa o buffer

            switch (option) {
                case 1: // PUT
                    handlePutRequest(scanner);
                    break;
                case 2: // GET
                    handleGetRequest(scanner);
                    break;
                case 3: // INIT
                    handleInitRequest(scanner);
                    break;
                case 4: // EXIT
                    scanner.close();
                    return;
                default:
                    System.out.println("Invalid option");
                    break;
            }
        }
    }

    private void handlePutRequest(Scanner scanner) {
        if (servers.isEmpty()) {
            System.out.println("Please initialize the servers first using INIT option.");
            return;
        }

        try {
            int serverIndex = random.nextInt(servers.size()) + 1;
            String server = servers.get(serverIndex);
            String[] serverInfo = server.split(":");
            String serverIP = serverInfo[0];
            int serverPort = Integer.parseInt(serverInfo[1]);

            try (Socket socket = new Socket(serverIP, serverPort);



                 ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                 ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

                System.out.print("Enter key: ");
                String key = scanner.nextLine();
                System.out.print("Enter value: ");
                String value = scanner.nextLine();

                System.out.println("key: " + key + ", value: " + value);

                Mensagem requestMessage = new Mensagem("PUT", key, value);
                System.out.println(requestMessage);
                out.writeObject(requestMessage);

                Mensagem responseMessage = (Mensagem) in.readObject();
                System.out.println(responseMessage.response);

            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void handleGetRequest(Scanner scanner) {
        if (servers.isEmpty()) {
            System.out.println("Please initialize the servers first using INIT option.");
            return;
        }

        try {
            int serverIndex = random.nextInt(servers.size()) + 1;
            String server = servers.get(serverIndex);
            String[] serverInfo = server.split(":");
            String serverIP = serverInfo[0];
            int serverPort = Integer.parseInt(serverInfo[1]);

            Socket socket = new Socket(serverIP, serverPort);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

            System.out.print("Enter key: ");
            String key = scanner.nextLine();

            Mensagem requestMessage = new Mensagem("GET", key);
            out.writeObject(requestMessage);

            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            Mensagem responseMessage = (Mensagem) in.readObject();
            System.out.println(responseMessage.response);

            out.close();
            in.close();
            socket.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void handleInitRequest(Scanner scanner) {
        try {
            for (int i = 1; i <= 3; i++) {
                System.out.print("Enter Server " + i + " IP: ");
                String serverIP = scanner.nextLine();
                System.out.print("Enter Server " + i + " Port: ");
                int serverPort = scanner.nextInt();
                scanner.nextLine(); // Limpa o buffer

                servers.put(i, serverIP + ":" + serverPort);
            }

            System.out.println("Servers initialized successfully.");
        } catch (Exception e) {
            System.out.println("Invalid input format. Please try again.");
        }
    }
}
