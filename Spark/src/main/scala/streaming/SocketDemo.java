package streaming;

import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

public class SocketDemo {
    public static void main(String[] args) throws Exception{
        Integer port = 1234;
        ServerSocket serverSocket = new ServerSocket(port);

        Socket clientSocket = serverSocket.accept();
        OutputStream outputStream = clientSocket.getOutputStream();

        Scanner scanner = new Scanner(System.in);
        while (true) {
            String next = scanner.next() + "\r\n";
            outputStream.write(next.getBytes() );
        }
    }
}
