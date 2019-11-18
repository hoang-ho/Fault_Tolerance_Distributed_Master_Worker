import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static java.util.stream.Collectors.toMap;


public class WordCount implements Master {
    private int numWorker;
    private int port = 5000;
    private Stack<String> Files = new Stack<>();
    private int[] workerSockets;
    private int counter = 0;
    private HashMap<Integer, Process> aliveProcess = new HashMap<>();
    private Set<Process> aliveProcesses;
    private List<String> outputFiles;
    private PrintStream outputStream = new PrintStream(new ByteArrayOutputStream());
    private HashMap<Integer, List<String>> workerFiles = new HashMap<>();
    private List<Thread> allThreads = new ArrayList<>();


    public WordCount(int workerNum, String[] filenames) {
        this.numWorker = workerNum;
        this.workerSockets = new int[numWorker];
        for (String filename : filenames) {
            this.Files.push(filename);
        }
        this.aliveProcesses = new HashSet<Process>(numWorker);
        this.outputFiles = new ArrayList<>();
    }

    public class DistributeWorkThread extends Thread {
        private int id;
        private CyclicBarrier gate;

        DistributeWorkThread(int id, CyclicBarrier gate) {
            this.id = id;
            this.gate = gate;
        }

        public void run() {
            try {
                gate.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }

            ServerSocket server = null;
            try {
                synchronized (workerSockets) {
                    server = new ServerSocket(workerSockets[id]);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            Socket serverClient = null;
            try {
                serverClient = server.accept();
                DataInputStream inStream = new DataInputStream(serverClient.getInputStream());
                DataOutputStream outStream = new DataOutputStream(serverClient.getOutputStream());
                System.out.println("Start distributing work");

                String clientMessage = "", serverMessage = "";

                synchronized (Files) {
                    if (!Files.empty()) {
                        serverMessage = Files.pop();
                        if (workerFiles.containsKey(id)) {
                            workerFiles.get(id).add(serverMessage);
                        } else {
                            List<String> temp = new ArrayList<>();
                            temp.add(serverMessage);
                            workerFiles.put(id, temp);
                        }
                    }
                }
                while (true) {
                    clientMessage = inStream.readUTF();
                    if (!clientMessage.startsWith("Finished")) {
                        System.out.println("Sending message: " + serverMessage);
                        outStream.writeUTF(serverMessage);
                        outStream.flush();
                        Thread.sleep(7000);
                    } else if (clientMessage.startsWith("Finished")) {
                        if (clientMessage.split("\\s+").length > 1) {
                            outputFiles.add(clientMessage.split("\\s+")[1]);
                        }

//                        if (id <= (numWorker / 2)) {
//                            Thread.sleep(3000);
//                        }
                        synchronized (Files) {
                            if (!Files.empty()) {
                                serverMessage = Files.pop();
                                outStream.writeUTF(serverMessage);
                                System.out.println("Sending message: " + serverMessage);
                                workerFiles.get(id).add(serverMessage);
                                clientMessage = "";
                            } else {
                                outStream.writeUTF("Exit");
                                inStream.close();
                                outStream.close();
                                serverClient.close();
                                server.close();
                                System.out.println("Exit thread " + id);
                                break;
                            }
                        }
                    }
                }
            } catch (IOException e) {
                try {
                    serverClient.close();
                    server.close();
                } catch (IOException ex) {
                    // nope
                }
                e.printStackTrace();
            } catch (InterruptedException e) {
                try {
                    serverClient.close();
                    server.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
                e.printStackTrace();
            }
        }
    }

    public class HeartBeat extends Thread {
        private int id;
        private CyclicBarrier gate;

        HeartBeat(int id, CyclicBarrier gate) {
            this.id = id;
            this.gate = gate;
        }

        public void run() {
            try {
                gate.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
            ServerSocket server = null;
            Socket serverClient = null;
            try {
                synchronized (workerSockets) {
                    server = new ServerSocket(workerSockets[id] + 1);
                }
                serverClient = server.accept();
                DataInputStream inStream = new DataInputStream(serverClient.getInputStream());
                DataOutputStream outStream = new DataOutputStream(serverClient.getOutputStream());
                System.out.println("Starting heartbeat ");
                String message = inStream.readUTF();
                while (!message.equals("Ready")) {
                    message = inStream.readUTF();
                }

                while (true) {
                    outStream.writeUTF("Is Alive?");
                    message = inStream.readUTF();
                    if (message.equals("Alive")) {
                        System.out.println("At heartbeat " + id + " received Alive");
                        if (aliveProcess.get(workerSockets[id]) != null) {
                            aliveProcesses.add(aliveProcess.get(workerSockets[id]));
                        }
                    } else {
                        aliveProcesses.remove(aliveProcess.get(workerSockets[id]));
                        synchronized (Files) {
                            for (String work : workerFiles.get(id)) {
                                System.out.println("Pushing back work" + work);
                                Files.push(work);
                            }
                        }
                        break;
                    }

                    if (Files.isEmpty()) {
//                        System.out.println("Current files " + Files);
                        outStream.writeUTF("Exit");
                        break;
                    }

                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException ie) {
                        outStream.writeUTF("Exit");
                        inStream.close();
                        outStream.close();
                        serverClient.close();
                        server.close();
                        System.out.println("Exiting heartbeat " + id);
                    }
                }
                inStream.close();
                outStream.close();
                serverClient.close();
                server.close();
                System.out.println("Exiting heartbeat " + id);
            } catch (IOException e) {
                try {
                    server.close();
                    serverClient.close();
                    System.out.println("Catch Fault");

                    for (String work : workerFiles.get(id)) {
                        System.out.println("Pushing back work" + work);
                        Files.push(work);
                    }

                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }

                    final CyclicBarrier gate = new CyclicBarrier(3);
                    DistributeWorkThread workThread = new DistributeWorkThread(id, gate);
                    HeartBeat heartBeat = new HeartBeat(id, gate);
                    counter = id;
                    workThread.start();
                    heartBeat.start();
                    try {
                        gate.await();
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    } catch (BrokenBarrierException ex) {
                        ex.printStackTrace();
                    }
                    System.out.println("Creating fault tolerance worker");
                    createWorker();

                } catch (IOException ex) {
                    // nope
                }
//                e.printStackTrace();
            }
        }
    }


    public void run() {
        final CyclicBarrier gate = new CyclicBarrier(3); // gate

        while (counter < numWorker) {
            synchronized (workerSockets) {
                workerSockets[counter] = port;
            }
            int finalI = counter;

            DistributeWorkThread t1 = new DistributeWorkThread(finalI, gate);
            HeartBeat t2 = new HeartBeat(finalI, gate);
            allThreads.add(t1);
            allThreads.add(t2);
            try {
                t1.start();
                t2.start();
                gate.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }

            try {
                createWorker();
            } catch (IOException e) {
                e.printStackTrace();
            }

            port += 2;
            counter += 1;
        }


        Thread accumulate = new Thread(new Runnable() {
            @Override
            public void run() {
                Map<String, Integer> map = new HashMap<>();

                for (Thread t : allThreads) {
                    try {
                        t.join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                while (true) {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    synchronized (Files) {
                        if (Files.empty()) {
                            System.out.println("Current stack of work: " + Files.toString());
                            break;
                        }
                    }
                }
                System.out.println("Everything finished");

                for (String str : outputFiles) {
                    BufferedReader br;
                    try {
//                        System.out.println("File is = "+str);
                        File file = new File(str);
//                        System.out.println("File = "+file);
                        br = new BufferedReader(new FileReader(file));

                        String st;
                        while ((st = br.readLine()) != null) {
                            String[] parts = st.split("\\s+");
//                            System.out.println(parts[0] + " " + parts[1]);
                            String word = parts[0];
                            int count = Integer.parseInt(parts[1].trim());


                            if (!word.equals("")) {
                                Integer n = map.get(word);
                                n = (n == null) ? count : n + count;
                                map.put(word, n);
                            }
                        }
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }

                Map<String, Integer> sorted = map.entrySet().stream().sorted(Map.Entry.comparingByKey()).sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));

                sorted.forEach((key, value) -> {
                    outputStream.println(value + " : " + key);
                    outputStream.flush();
                });
                outputStream.close();
            }
        });
        accumulate.start();
        try {
            accumulate.join();
            System.out.println("finished outstream");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Collection<Process> getActiveProcess() {
        return aliveProcesses;
    }

    public void setOutputStream(PrintStream out) {
        this.outputStream = out;
    }


    public void createWorker() throws IOException {
        final int index = counter;
        createProcess(index);
    }

    public void createProcess(int index) throws IOException {
        Class klass = WorkerProcess.class;
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome +
                File.separator + "bin" +
                File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = klass.getCanonicalName();
        ProcessBuilder processBuilder = new ProcessBuilder(javaBin, "-cp", classpath, className, String.valueOf(workerSockets[index]), String.valueOf(workerSockets[index] + 1));
        Process newProcess = processBuilder.start();
        System.out.println("Creating a new worker");

        aliveProcess.put(workerSockets[index], newProcess);

        BufferedReader br = new BufferedReader(new InputStreamReader(newProcess.getInputStream()));

        System.out.println(br.readLine());
        System.out.println(br.readLine());
        System.out.println(br.readLine());
        System.out.println(br.readLine());
        System.out.println(br.readLine());
    }


    public static void main(String[] args) throws Exception {
        String[] filenames = new String[6];
        filenames[0] = "/Users/hoangho/project-2-group-29/example-output/king-james-version-bible-copy.txt";
        filenames[1] = "/Users/hoangho/project-2-group-29/example-output/king-james-version-bible.txt";
        filenames[2] = "/Users/hoangho/project-2-group-29/example-output/random_copy.txt";
        filenames[3] = "/Users/hoangho/project-2-group-29/example-output/random.txt";
        filenames[4] = "/Users/hoangho/project-2-group-29/example-output/war-and-peace-copy.txt";
        filenames[5] = "/Users/hoangho/project-2-group-29/example-output/war-and-peace.txt";
//        filenames[0] = "/Users/hoangho/project-2-group-29/src/test/resources/simple.txt";

//        filenames[0] = "/Users/chaitanya/Desktop/project-2-group-29/example-output/king-james-version-bible.txt";
//        filenames[1] = "/Users/chaitanya/Desktop/project-2-group-29/example-output/war-and-peace.txt";
//        filenames[2] = "/Users/chaitanya/Desktop/project-2-group-29/example-output/king-james-version-bible-copy.txt";
//        filenames[3] = "/Users/chaitanya/Desktop/project-2-group-29/example-output/war-and-peace-copy.txt";
//        filenames[4] = "/Users/chaitanya/Desktop/project-2-group-29/example-output/random_copy.txt";
//        filenames[5] = "/Users/chaitanya/Desktop/project-2-group-29/example-output/random.txt";

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        WordCount wc = new WordCount(3, filenames);
        wc.setOutputStream(new PrintStream(out));
        wc.run();
    }

}

