import java.io.BufferedReader;
import java.io.BufferedOutputStream;  
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;  
import java.io.OutputStream;
import java.net.Socket;
import java.net.ServerSocket;  
import java.util.concurrent.*;
import java.net.InetAddress;  
import java.io.*;
import java.util.Random;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.ArrayList;
import java.net.MulticastSocket;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;



public class Lamport {

    static String ipArray[] = new String[3];
    static int portArray[] = new int [3];
    static Map<String, Integer> pArray = new HashMap<String, Integer>();
    static int id;
    static int nOp;
    static int clockRate;
    static AtomicLong timestamp = new AtomicLong(0);
    static int x = 100;
    static int y = 100;
    static DateFormat df = new SimpleDateFormat("MM/dd HH:mm:ss");
    public static volatile boolean finish = false;

    public static void main(String[] args) throws IOException {

        if(args.length!=3) {
            System.out.println("Error");
            System.exit(1);
        }
        
        id = Integer.valueOf(args[0]);
        nOp = Integer.valueOf(args[1]);
        clockRate = Integer.valueOf(args[2]);

        try {
            BufferedReader br = new BufferedReader(new FileReader("info.txt"));
            String strLine = null;
            //Read File Line By Line
            int i=0;
            while ((strLine = br.readLine()) != null)   {
                ipArray[i] = strLine.split("\\s+",2)[0];
                portArray[i] = Integer.valueOf(strLine.split("\\s+",2)[1]);
                pArray.put(ipArray[i], i);
                
                // Write into a file
                try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("log"+Lamport.id+".txt", true)))) {
                    out.println("[P"+i+"]"+strLine);
                }catch (IOException e) {
                    System.out.println("Error"+e);
                }
                i++;
            }
        } catch(IOException e) {
            System.out.println("Please check info.txt format");
            System.err.println("Error:"+e);
        }

        try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("log"+Lamport.id+".txt", true)))) {
            out.println("P"+id+"("+ipArray[id]+") is listening on port "+portArray[id]+" ...");
        }catch (IOException e) {
            System.out.println("Error"+e);
        }

        new Timer();
        Server server = new Server(id, portArray[id]);  
        setUpConnection(server);
        try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("log"+Lamport.id+".txt", true)))) {
            out.println("All connected.");
        }catch (IOException e) {
            System.out.println("Error"+e);
        } 
        System.out.println("Ready");
        new Client();

        while(true) {
            try {
                Thread.sleep(1000);
                if(Lamport.finish) {
                    System.exit(1);
                }
            } catch(InterruptedException e) {
                System.err.println("Error:"+e);
            }
        }
    }

    static void setUpConnection(Server server) {
        for(int i=0; i<id; i++) {
            boolean ready = false;
            while(!ready) {
                try {
                    new Client(ipArray[i], portArray[i]);
                    ready = true;
                } catch(IOException e) {
                    ready = false;
                }
            }
        }
        while(!server.getReady()) {
            try {
                Thread.sleep(100);
            } catch(InterruptedException e) {
                System.err.println("Error:"+e);
            } 
        }
    }
}

class Server extends Thread {
    private ServerSocket server;
    private boolean ready;
    private int id;
    static Map<String, String> q = new HashMap<String, String>();
    static Lock lockQ = new ReentrantLock();
    static Map<String, Integer> ackCounter = new HashMap<String, Integer>();
    static Lock lockACK = new ReentrantLock();
    private int P0 = 0;
    public Server(int id, int port) throws IOException {
        this.id = id;
        server = new ServerSocket(port);
        ready = id==2? true : false;
        start();
    }
    public void run() {
        try {
            int i=0;
            while(!Lamport.finish) {
                Socket socket = server.accept();
                if(!ready) {
                    Date date = new Date();

                    //System.out.println("New connection accepted "  
                    //    + socket.getInetAddress() + ":" + socket.getPort()); 

                    // Write into a file
                    String ip = socket.getInetAddress().getHostAddress();
                    int j = Lamport.pArray.get(ip);
                    try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("log"+Lamport.id+".txt", true)))) {
                        out.print("["+Lamport.df.format(date)+"] P"+Lamport.id+" is connected from P"+j+"("+ip+":"+Lamport.portArray[j]+").");
                    }catch (IOException e) {
                        System.out.println("Error"+e);
                    }
                    if(Lamport.id == 1 && Client.P1 == 0) {
                        try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("log"+Lamport.id+".txt", true)))) {
                            out.println("\nWaiting for all to be connected...");
                        }catch (IOException e) {
                            System.out.println("Error"+e);
                        }
                        Client.P1++;
                    }

                    if(Lamport.id == 0 && P0<1) {
                        try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("log"+Lamport.id+".txt", true)))) {
                            out.println("\nWaiting for all to be connected...");
                        }catch (IOException e) {
                            System.out.println("Error"+e);
                        }
                        P0++;
                    }

                    i++;
                    if(i == 2-id) {
                        ready = true;
                    }
                } else {
                    new Dispatcher(socket);
                }
            }
        } catch(IOException e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        }
    }

    public boolean getReady() {
        return ready;
    }
}

class Dispatcher extends Thread {
    private BufferedReader br = null; 
    private PrintWriter pw = null;
    private Socket client = null;
    static int opNumber = 1;
    static int fCounter = 0;
    static Lock lockF = new ReentrantLock();
    public Dispatcher(Socket s) {
        this.client = s;
        start();
    }
     
    public void run() {
        try {
            br = new BufferedReader(new InputStreamReader(client.getInputStream()));  

            String str = br.readLine();
            // Write result into a file
            //try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("log.txt", true)))) {
            //    out.println(Lamport.df.format(date) +" = "+str);
            //}catch (IOException e) {
            //    System.out.println("Error"+e);
            //}

            //System.out.println(str);
            String recData[] = str.split(",");
            String flag = recData[0];
            if(flag.equals("UPDATE")) {
                //Insert q
                updateQueue("ADD", recData[1]+","+recData[2]+","+recData[3]);
                Long currentTime = Lamport.timestamp.get();
                long senderTimestamp = Long.valueOf(recData[3].split("\\.", 2)[0]);
                
                Lamport.timestamp.getAndSet(Math.max(senderTimestamp, currentTime)+1);
                //Multicast ACK
                for(int i=0; i<3; i++) {
                    if(i==Lamport.id)
                        continue;
                        //Date dateobj = new Date();
                    new Worker(i, "ACK,"+recData[1]);

                }

            } else if(flag.equals("ACK")) {
                if(countACK(str.split(",")[1])) {
                    updateQueue("RM", str.split(",")[1]);
                }
            } else if(flag.equals("FINISH")) {

                if(Integer.valueOf(recData[1]) == Lamport.id) {
                    while(Dispatcher.opNumber <= 3*Lamport.nOp) {
                        try {
                            Thread.sleep(100);
                        }catch(InterruptedException e) {
                            System.err.println("Error:"+e);
                        }
                    }

                    Date date = new Date();
                    try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("log"+Lamport.id+".txt", true)))) {
                        out.println("["+Lamport.df.format(date)+"] P"+Lamport.id+" finished.");
                    }catch (IOException e) {
                        System.out.println("Error"+e);
                    }

                    for(int i=0; i<3; i++) {
                        if(Lamport.id == i)
                            continue;
                        new Worker(i, "FINISH"+","+Lamport.id);
                    }


                    while(fCounter !=2 ) {
                        try {
                            Thread.sleep(100);
                        } catch(InterruptedException e) {
                            System.err.println("Error:"+e);
                        }
                    }

                    date = new Date();
                    try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("log"+Lamport.id+".txt", true)))) {
                        out.println("["+Lamport.df.format(date)+"] All finished.");
                        out.println("P"+Lamport.id+" is terminating...");
                    }catch (IOException e) {
                        System.out.println("Error"+e);
                    }

                    Lamport.finish = true;

                } else {
                    Date date = new Date();
                    try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("log"+Lamport.id+".txt", true)))) {
                        out.println("["+Lamport.df.format(date)+"] P"+recData[1]+" finished.");
                    }catch (IOException e) {
                        System.out.println("Error"+e);
                    }

                    Dispatcher.lockF.lock();
                    try {
                        fCounter++;
                    } catch (Exception e) {

                    } finally {
                        Dispatcher.lockF.unlock();
                    }
                }
            }
        } catch(IOException e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        } finally {
            try {
                br.close();
                client.close();
            } catch (IOException e) {  
                e.printStackTrace();  
            }
        }
    }

    private void updateQueue(String op, String str1) {
        Server.lockQ.lock();
        String str = str1;

        try {
            boolean flag1 = Server.q.containsKey(str.split(",")[0]);
            if(flag1 && !op.equals("RM")) {
                Server.q.put(str.split(",")[0], str.split(",")[1]+","+str.split(",")[2]+",yes");
                op = "RM";
                str = str.split(",")[0];
                Server.q = SortByTimeStamp(Server.q);
            }

            if(op.equals("ADD")) {
                boolean flag = Server.q.containsKey(str.split(",")[0]);
                Server.q.put(str.split(",")[0], str.split(",")[1]+","+str.split(",")[2]);

                /*
                if(flag)
                    Server.q.put(str.split(",")[0], str.split(",")[1]+","+str.split(",")[2]+",yes");
                else
                    Server.q.put(str.split(",")[0], str.split(",")[1]+","+str.split(",")[2]);
                */
                //Server.q.add(str);

                Server.q = SortByTimeStamp(Server.q);
/*
for(Object key : Server.q.keySet()) {
                    System.out.println("after "+Server.q.get(key));
                }
                System.out.println(); */

            } else if(op.equals("RM")) {
                //update Currency
                boolean flag = Server.q.containsKey(str);
                if(flag) {

                    Server.q.put(str, Server.q.get(str)+",yes");

                    Iterator it = Server.q.keySet().iterator();
                    while(it.hasNext()) {
                        String key = (String)it.next();
                        String mess[] = Server.q.get(key).split(",");
                        if(mess.length >= 3) {
                            String data[] = mess[0].split("/");
                            Lamport.x +=Integer.valueOf(data[0]); 
                            Lamport.y +=Integer.valueOf(data[1]);    
                            it.remove();
                            Server.q.remove(key);

                            // Write result into a file
                            Date date = new Date();

                            try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("log"+Lamport.id+".txt", true)))) {
                                out.println("["+Lamport.df.format(date)+"] [OP"+(Dispatcher.opNumber++)+" : C"+Lamport.timestamp+
                                "] Currency value is set to ("+Lamport.x+","+Lamport.y+") by ("+data[0]+
                                ","+data[1]+").");
                            }catch (IOException e) {
                                System.out.println("Error"+e);
                            }
                        } else {
                            break;
                        }
                    }
                } else 
                    Server.q.put(str, "");

/*
                 for(Object key : Server.q.keySet()) {
                    System.out.println("levt "+Server.q.get(key));
                }
                System.out.println(); */
                
            }
        } catch (Exception e) {

        } finally {
            Server.lockQ.unlock();
        }
    }

    private  boolean countACK(String str) {
        Server.lockACK.lock();
        try {
            String message = str;
            boolean flag = Server.ackCounter.containsKey(message);
            if(flag) {
                int count = Server.ackCounter.get(message);
            
                if(count == 1) {
                    Server.ackCounter.remove(message);
                    return true;
                } else {
                    Server.ackCounter.put(message, count+1);
                }
                
            } else {
                Server.ackCounter.put(message, 1);
            }
        } catch (Exception e) {

        } finally {
            Server.lockACK.unlock();
        }

        return false;
    }

/*
    private Map SortByTimeStamp(Map<String, String> map) {
        ValueComparator vc = new ValueComparator(map);
        Map sortedMap = new HashMap<String, String>(vc);
        sortedMap.putAll(map);
        return sortedMap;
    }*/
    private Map<String, String> SortByTimeStamp(Map<String, String> map) {
        List<Map.Entry<String, String>> list =
            new LinkedList<Map.Entry<String, String>>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, String>>() {
            public int compare(Map.Entry<String, String> s1,
                               Map.Entry<String, String> s2) {
                if(s1.getValue().equals(""))
                    return -1;
                else if(s2.getValue().equals(""))
                    return -1;
                else {
                    if(Integer.valueOf(s1.getValue().split(",")[1].split("\\.")[0]) < Integer.valueOf(s2.getValue().split(",")[1].split("\\.")[0]))
                            return -1;
                    else if(Integer.valueOf(s1.getValue().split(",")[1].split("\\.")[0]) == Integer.valueOf(s2.getValue().split(",")[1].split("\\.")[0])) {
                        if(Integer.valueOf(s1.getValue().split(",")[1].split("\\.")[1]) < Integer.valueOf(s2.getValue().split(",")[1].split("\\.")[1]))
                            return -1;
                        else 
                            return 1;
                    }
                        else
                            return 1;
                }
            }
        });

        Map<String, String> sortedMap = new LinkedHashMap<String, String>();
        for (Iterator<Map.Entry<String, String>> it = list.iterator(); it.hasNext();) {
            Map.Entry<String, String> entry = it.next();
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        return sortedMap;
    }


}

class Client {
    static int P2 = 0;
    static int P1 = 0;
    private int deltaX, deltaY;
    public Client(String ip, int port) throws IOException {
        Socket client = new Socket(ip, port);
        Date date = new Date();
        try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("log"+Lamport.id+".txt", true)))) {
            out.print("["+Lamport.df.format(date)+"] P"+Lamport.id+" is connected to P"+Lamport.pArray.get(ip)+" ("+ip+":"+port+").");
            if(Lamport.id == 2 && P2<1) {
                P2++;
                out.println("\nWaiting for all to be connected...");
            } 

            if(Lamport.id == 1 && P1 == 0) {
                out.println("\nWaiting for all to be connected...");
                P1++;
            }
        }catch (IOException e) {
            System.out.println("Error"+e);
        }

    }

    public Client() throws IOException {
        int count = 0;
        while(count<Lamport.nOp) {
            generateDelta();
            Lamport.timestamp.getAndIncrement();
            long currentTime = Lamport.timestamp.get();
            for(int i=0; i<3; i++) {
                new Worker(i, "UPDATE,"+Lamport.id+"/"+count+","+deltaX+"/"+deltaY+","+currentTime+"."+Lamport.id);
            }
            count++;
        }
        //terminate
        new Worker("FINISH"+","+Lamport.id);
    }

    private void generateDelta() {
        Random rn = new Random();
        deltaX = rn.nextInt(160) - 80;
        deltaY = rn.nextInt(160) - 80;
        int interval = rn.nextInt(1000) + 1;
        try {
            Thread.sleep(interval);
        } catch(InterruptedException e) {
            System.err.println("Error:"+e);
        }
    }
}

class Worker extends Thread {
    private Socket client = null;
    private BufferedReader br = null;  
    private PrintWriter pw = null;
    private String data;
    private int id;

    public Worker(String data) {
        this.id = Lamport.id;
        this.data = data;
        start();
    }
    public Worker(int id, String data) {
        this.id = id;
        this.data = data;
        start();
    }
    public void run() {
        try {
            //Date date= new Date();
            //System.out.println(Lamport.df.format(date)+" "+data);
            client = new Socket(Lamport.ipArray[id], Lamport.portArray[id]);
            pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(  
                    client.getOutputStream())));  

            pw.println(data);
            pw.flush();
        } catch (Exception e) {  
            e.printStackTrace(); 
        } finally {
            try {
                pw.close();
                client.close();
            } catch (IOException e) {   
                e.printStackTrace();  
            }
        }
    }
}

class Timer extends Thread{
    public Timer() {
        start();
    }
    public void run() {
        while(!Lamport.finish) {
            try {
                Thread.sleep(1000);
                Lamport.timestamp.addAndGet(Lamport.clockRate);
            } catch(InterruptedException e) {
                System.err.println("Error:"+e);
            }
        }
    }
}