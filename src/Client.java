import java.io.*;
import java.net.Socket;
import java.util.Random;
import java.util.Scanner;

public class Client {
    private Socket socket;
    private String[] serverIPs;
    private int[] serverPorts;

    public Client() {
        this.serverIPs = new String[3];
        this.serverPorts = new int[3];
    }

    public static void main(String[] args) {
        Client client = new Client();
        client.start();
    }

    private void start() {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            displayMenu();

            int option = scanner.nextInt();

            if (option == 1) {
                if (!isInitialized()) {
                    System.out.println("Please initialize the client first (choose INIT option).");
                    continue;
                }

                System.out.print("Enter key: ");
                String key = scanner.next();
                System.out.print("Enter value: ");
                String value = scanner.next();

                performPutRequest(key, value);
            } else if (option == 2) {
                if (!isInitialized()) {
                    System.out.println("Please initialize the client first (choose INIT option).");
                    continue;
                }

                System.out.print("Enter key: ");
                String key = scanner.next();

                performGetRequest(key);
            } else if (option == 3) {
                // Initialize the client
                init();
            } else {
                System.out.println("Invalid option");
            }

            try {
                Thread.sleep(1000); // Wait for 1 second before displaying the menu again
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean isInitialized() {
        for (int i = 0; i < 3; i++) {
            if (serverIPs[i] == null || serverPorts[i] == 0) {
                return false;
            }
        }
        return true;
    }

    private void displayMenu() {
        System.out.println("\nOptions:");
        System.out.println("1. PUT");
        System.out.println("2. GET");
        System.out.println("3. INIT");
        System.out.print("Enter option: ");
    }

    private void init() {
        Scanner scanner = new Scanner(System.in);
        for (int i = 0; i < 3; i++) {
            System.out.print("Enter Server " + (i + 1) + " IP: ");
            serverIPs[i] = scanner.next();
            System.out.print("Enter Server " + (i + 1) + " Port: ");
            serverPorts[i] = scanner.nextInt();
        }
        System.out.println("Client initialized.");
    }

    private void performPutRequest(String key, String value) {
        // Choose a random server to connect to
        Random random = new Random();
        int randomServer = random.nextInt(3);
        String serverIP = serverIPs[randomServer];
        int serverPort = serverPorts[randomServer];

        handlePutRequest(serverIP, serverPort, key, value);
    }

    private void performGetRequest(String key) {
        // Choose a random server to connect to
        Random random = new Random();
        int randomServer = random.nextInt(3);
        String serverIP = serverIPs[randomServer];
        int serverPort = serverPorts[randomServer];

        handleGetRequest(serverIP, serverPort, key);
    }

    private void handlePutRequest(String serverIP, int serverPort, String key, String value) {
        try {
            // Connect to the server
            socket = new Socket(serverIP, serverPort);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            // Create a Message object for the PUT request
            Mensagem requestMessage = new Mensagem("PUT", key, value);
            out.writeObject(requestMessage);

            // Wait for the response from the server
            Mensagem responseMessage = (Mensagem) in.readObject();

            // If the response is empty, it means the PUT request was forwarded successfully
            if (responseMessage.getResponse().isEmpty()) {
                System.out.println("PUT request forwarded to leader. Waiting for response...");
                responseMessage = (Mensagem) in.readObject();
            }

            System.out.println("Response: " + responseMessage.getResponse());

            // Close the connections
            out.close();
            in.close();
            socket.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void handleGetRequest(String serverIP, int serverPort, String key) {
        try {
            // Connect to the server
            socket = new Socket(serverIP, serverPort);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            Mensagem requestMessage = new Mensagem("GET", key, null);
            out.writeObject(requestMessage);

            // Wait for the response from the server
            Mensagem responseMessage = (Mensagem) in.readObject();
            System.out.println("Response: " + responseMessage.getResponse());

            // Close the connections
            out.close();
            in.close();
            socket.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}