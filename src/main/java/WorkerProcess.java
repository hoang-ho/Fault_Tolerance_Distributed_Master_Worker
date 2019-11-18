import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static java.util.stream.Collectors.toMap;


public class WorkerProcess {
    private static String output = "/Users/hoangho/project-2-group-29/output_files";
//    private static String output = "/Users/chaitanya/Desktop/project-2-group-29/output_files";

    private static String readFile(String fileName) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        try {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                sb.append("\n");
                line = br.readLine();
            }
            return sb.toString();
        } finally {
            br.close();
        }
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);
        int heartBeat = Integer.parseInt(args[1]);
        System.out.println("New Worker Created with port " + port + " and heartbeat " + heartBeat);
        final CyclicBarrier gate = new CyclicBarrier(3);

        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
                try {
                    Socket socket = new Socket("127.0.0.1", port);
                    System.out.println("Starting worker using " + port);
                    DataInputStream inStream = new DataInputStream(socket.getInputStream());
                    DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());
                    String serverMessage = "";

                    while (!serverMessage.equals("Exit")) {
                        outStream.writeUTF("Ready");
                        outStream.flush();
                        serverMessage = inStream.readUTF();

                        if (serverMessage.equals("")) {
                            outStream.writeUTF("Finished ");
                            serverMessage = inStream.readUTF();
                            continue;
                        }

                        /* Word Count */
                        String outputFile = output + "/" + serverMessage.split("/")[serverMessage.split("/").length - 1];
                        String content = readFile(serverMessage);

                        Map<String, Integer> map = new HashMap<>();
                        String[] words = content.split("\\s+");
                        for (String word : words) {
                            if (!word.equals("")) {
                                Integer n = map.get(word);
                                n = (n == null) ? 1 : n + 1;
                                map.put(word, n);
                            }
                        }

                        Writer writer = new BufferedWriter(new OutputStreamWriter(
                                new FileOutputStream(new File(outputFile)), StandardCharsets.UTF_8));
                        map.forEach((key, value) -> {
                            try {
                                writer.write(key + " " + Integer.toString(value) + System.lineSeparator());
                                writer.flush();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
                        writer.close();
                        System.out.println("Worker finishes writing to file " + outputFile);
                        outStream.writeUTF("Finished " + outputFile);
                        outStream.flush();
                        serverMessage = inStream.readUTF();
                        System.out.println(serverMessage);
                    }
                    outStream.close();
                    inStream.close();
                    socket.close();
                    System.out.println("Exiting worker " + port);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
                Socket socket = null;
                try {
                    System.out.println("Starting worker heartbeat using " + heartBeat);
                    socket = new Socket("127.0.0.1", heartBeat);
                    DataInputStream inStream = new DataInputStream(socket.getInputStream());
                    DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());
                    String serverMessage = "";

                    outStream.writeUTF("Ready");
                    while (!serverMessage.equals("Exit")) {
                        outStream.writeUTF("Alive");
                        serverMessage = inStream.readUTF();
                    }

                    inStream.close();
                    outStream.close();
                    socket.close();
                    System.out.println("Exiting worker heartbeat " + heartBeat);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        });

        t1.start();
        t2.start();
        try {
            gate.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }

    }
}
