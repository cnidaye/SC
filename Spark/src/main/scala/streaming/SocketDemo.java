package streaming;

import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketDemo {
    public static void main(String[] args) throws Exception{
        Integer port = 1234;
        ServerSocket serverSocket = new ServerSocket(port);

        Socket clientSocket = serverSocket.accept();
        OutputStream outputStream = clientSocket.getOutputStream();
        while (true) {
            String message = "Hello\r\n";
            outputStream.write(message.getBytes());
            Thread.sleep(1000);
        }
    }
}
