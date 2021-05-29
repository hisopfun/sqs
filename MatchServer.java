package aws.example.sqs;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import java.util.Date;
import java.util.List;

import java.util.*;
import java.io.*;
import java.util.ArrayList;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.lang.String;
//import java.net.InetAddress;
import java.net.SocketAddress;
import java.util.List;
import java.text.DateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicInteger;

public class MatchServer {
    //BS=,price=,qty=
    public class order{
        String sysTime;
        String time;
        String stockNo;
        String BS;
        float bid;
        float ask;
        float price;
        int qty;
        String ipAdr;
        int sequence;

        public order(String nSysTime, String nTime, String nStockNo, String nBS, float nPrice, int nQty, String nIPAdr, int nSequence){
            sysTime = nSysTime;
            time = nTime;
            stockNo = nStockNo;
            BS = nBS;
            price = nPrice;
            qty = nQty;
            ipAdr = nIPAdr;
            sequence = nSequence;
        }

    }
    ArrayList<order> match = new ArrayList<order>();
    ArrayList<order> bid = new ArrayList<order>();
    ArrayList<order> ask = new ArrayList<order>();
    ArrayList<order> allOrder = new ArrayList<order>();
    AtomicInteger ordTime = new AtomicInteger( 0 );

    //Client List
    private ArrayList<AsynchronousSocketChannel> list = new ArrayList<>();

    //IP List
    private List<SocketAddress>ips = new ArrayList<SocketAddress>();

    //Destination client
    AsynchronousSocketChannel clientChannel;

    //create a socket channel and bind to local bind address
    AsynchronousServerSocketChannel serverSock;// =  AsynchronousServerSocketChannel.open().bind(sockAddr);
    AsynchronousServerSocketChannel serverSockMain;

    public MatchServer( String bindAddr, int bindPort ) throws IOException {
        serverSock =  AsynchronousServerSocketChannel.open().bind(new InetSocketAddress(bindAddr, bindPort));
        serverSockMain =  AsynchronousServerSocketChannel.open().bind(new InetSocketAddress("0.0.0.0", 19030));   

       //start to accept the connection from client
        serverSock.accept(serverSock, new CompletionHandler<AsynchronousSocketChannel,AsynchronousServerSocketChannel >() {
            @Override
            public void completed(AsynchronousSocketChannel sockChannel, AsynchronousServerSocketChannel serverSock ) {
                
                //a connection is accepted, start to accept next connection
                serverSock.accept( serverSock, this );

                try{
                    //Print IP Address
                    System.out.println( sockChannel.getLocalAddress().toString());

                    //Add To Client List
                    list.add(list.size(), sockChannel);

                }catch(IOException e) {
                    e.printStackTrace();
                }

                //start to read message from the client
                startRead( sockChannel );
                
            }

            @Override
            public void failed(Throwable exc, AsynchronousServerSocketChannel serverSock) {
                System.out.println( "fail to accept a connection");
            }
        } );

        //start to accept the connection from client
        serverSockMain.accept(serverSockMain, new CompletionHandler<AsynchronousSocketChannel,AsynchronousServerSocketChannel >() {

            @Override
            public void completed(AsynchronousSocketChannel sockChannel, AsynchronousServerSocketChannel serverSockMain ) {
                //a connection is accepted, start to accept next connection
                serverSockMain.accept( serverSockMain, this );

                //Print IP Address
                try{
                    System.out.println( sockChannel.getLocalAddress());
                    clientChannel = sockChannel;
                }catch(IOException e) {

                    e.printStackTrace();
                }

                //Add To Client List
                //list.add(list.size(), sockChannel);

                //start to read message from the client
                startRead( sockChannel );
                
            }

            @Override
            public void failed(Throwable exc, AsynchronousServerSocketChannel serverSockMain) {
                System.out.println( "fail to accept a connection");
            }
        } );
        
    }


    private static String getString(ByteBuffer buf){
        byte[] bytes = new byte[buf.remaining()]; // create a byte array the length of the number of bytes written to the buffer
        buf.get(bytes); // read the bytes that were written
        String packet = new String(bytes);
        return packet;
    }

    private order ParseOrder(String sysTime, String orderMsg, String ipAdr){
        orderMsg = orderMsg.replace("\n", "");
        String time = orderMsg.split(",")[0];
        String stockNo = orderMsg.split(",")[1];
        String bs = orderMsg.split(",")[2];
        float price = Float.parseFloat(orderMsg.split(",")[3]);
        int qty = Integer.parseInt(orderMsg.split(",")[4]);

        return new order(sysTime, time, stockNo, bs, price, qty, ipAdr, ordTime.incrementAndGet());
    }
//2021/05/23 15:55:14.763,TXF,B,10000.0,1,127.0.0.1
//2021/05/23 15:55:14.763,TXF,S,10000.0,1,127.0.0.1
    private void limit(order ord, ArrayList<order> lim){
        //B
        if (ord.BS.equals("B")){
            System.out.println("ord:" + ord.BS + " " + ord.qty + " " + ord.price + " " +  ord.sequence);
            for(int i = 0; i < lim.size(); i++){
                if (ord.price > lim.get(i).price){
                    lim.add(i, ord);
                    reportClient(i, lim, "Ord report:" + ord.sysTime + "," + ord.stockNo+","+
                        ord.BS+","+ord.qty+","+ord.price);
                    break;
                }

                if (lim.size()-1 == i){
                    lim.add(ord);
                    reportClient(i, lim, "Ord report:" + ord.sysTime + "," + ord.stockNo+","+
                        ord.BS+","+ord.qty+","+ord.price);
                    break;
                }
            }
            if (lim.size() == 0)    {
                lim.add(ord);
                reportClient(0, lim, "Ord report:" + ord.sysTime + "," + ord.stockNo+","+
                        ord.BS+","+ord.qty+","+ord.price);
            }
        }

        //S
        if (ord.BS.equals("S")){
            System.out.println("ord:" + ord.BS + " " + ord.qty + " " + ord.price + " " + ord.sequence);
            for(int i = 0; i < lim.size(); i++){
                if (ord.price < lim.get(i).price){
                    lim.add(i, ord);
                    reportClient(i, lim, "Ord report:" + ord.sysTime + "," + ord.stockNo+","+
                        ord.BS+","+ord.qty+","+ord.price);
                    break;
                }

                if (lim.size()-1 == i){
                    lim.add(ord);
                    reportClient(i, lim, "Ord report:" + ord.sysTime + "," + ord.stockNo+","+
                        ord.BS+","+ord.qty+","+ord.price);
                    break;
                }
            }
            if (lim.size() == 0) {
              lim.add(ord);
                reportClient(0, lim, "Ord report:" + ord.sysTime + "," + ord.stockNo+","+
                        ord.BS+","+ord.qty+","+ord.price);
            }
        }

        allOrder.add(ord);
        //Print
        //System.out.println("limit:" + ord.BS + " " + ord.qty + " " + ord.price);
        for(int i = 0; i < lim.size(); i++) 
            System.out.println(lim.get(i).BS + lim.get(i).price);

    }

    private void removeLim(ArrayList<order> lim, int n){
        reportClient(n, lim, "Del report:" + getSysTime() + "," + lim.get(n).stockNo+","+
            lim.get(n).BS+","+lim.get(n).qty+","+lim.get(n).price);
        lim.remove(n);
    }

    private boolean trade( ArrayList<order> bid, ArrayList<order> ask, ArrayList<order> mat){
        if (bid.size() == 0 || ask.size() == 0) return false;
        System.out.println(">>>>>>>>>" + bid.get(0).price+ "," + bid.get(0).sequence + "," + ask.get(0).price+ "," + ask.get(0).sequence + ">>>>>>>>>>>>>>" );
        if (bid.get(0).price > ask.get(0).price){

            //Buy
            if (bid.get(0).sequence > ask.get(0).sequence){
                mat.add(new order(getSysTime(), "", ask.get(0).stockNo, ask.get(0).BS, ask.get(0).price
                    , 1, "", 0));
                reportClient(0, bid, "Mat report:" + mat.get(mat.size()-1).sysTime + "," + mat.get(mat.size()-1).stockNo+","+
                    bid.get(0).BS+","+mat.get(mat.size()-1).qty+","+mat.get(mat.size()-1).price);
                reportClient(0, ask, "Mat report:" + mat.get(mat.size()-1).sysTime + "," + mat.get(mat.size()-1).stockNo+","+
                    ask.get(0).BS+","+mat.get(mat.size()-1).qty+","+mat.get(mat.size()-1).price);
                removeLim(bid, 0);
                removeLim(ask, 0);
                return true;
            //Sell
            }else if (bid.get(0).sequence < ask.get(0).sequence){
                mat.add(new order(getSysTime(), "", bid.get(0).stockNo, bid.get(0).BS, bid.get(0).price
                    , 1, "", 0));
                reportClient(0, bid, "Mat report:" + mat.get(mat.size()-1).sysTime + "," + mat.get(mat.size()-1).stockNo+","+
                    bid.get(0).BS+","+mat.get(mat.size()-1).qty+","+mat.get(mat.size()-1).price);
                reportClient(0, ask, "Mat report:" + mat.get(mat.size()-1).sysTime + "," + mat.get(mat.size()-1).stockNo+","+
                    ask.get(0).BS+","+mat.get(mat.size()-1).qty+","+mat.get(mat.size()-1).price);
                removeLim(bid, 0);
                removeLim(ask, 0);
                return true;
            }
        }



        if (bid.get(0).price == ask.get(0).price){
            mat.add(new order(getSysTime(), "", ask.get(0).stockNo, ask.get(0).BS, ask.get(0).price
                , ask.get(0).qty, "", 0));
            reportClient(0, bid, "Mat report:" + mat.get(mat.size()-1).sysTime + "," + mat.get(mat.size()-1).stockNo+","+
                bid.get(0).BS+","+mat.get(mat.size()-1).qty+","+mat.get(mat.size()-1).price);
            reportClient(0, ask, "Mat report:" + mat.get(mat.size()-1).sysTime + "," + mat.get(mat.size()-1).stockNo+","+
                ask.get(0).BS+","+mat.get(mat.size()-1).qty+","+mat.get(mat.size()-1).price);
            removeLim(bid, 0);
            removeLim(ask, 0);
            return true;
        }

        return false;
    }

    private String getSysTime(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        sdf.setTimeZone(TimeZone.getTimeZone("Asia/Taipei"));
        return sdf.format(new Date());
    } 

    private void reportClient(int n, ArrayList<order> lim, String msg){
        try{
            //Report to client
            for(int i = 0; i < list.size(); i++){
                if (list.get(i).getRemoteAddress().toString().contains(lim.get(n).ipAdr)){
                    System.out.println("Send To client:" + msg + lim.get(n).ipAdr);
                    try{
                        startWrite(list.get(i), msg + "\n");
                    }catch(Exception ex){
                        //lim.remove(n);
                    }
                }
            }
        }catch(Exception ex){

        }
    }

    public String getAllOrder(ArrayList<order> bid, ArrayList<order> ask){

        String all = "";
        if (bid.size() > 0){
            float price = bid.get(0).price;
            int qty = bid.get(0).qty;
            for(int i = 0; i < bid.size(); i++){
                if (bid.get(i).price != price){
                    all += bid.get(i).stockNo + ","+ bid.get(i).BS + "," + price + "," + qty + "|";
                    price = bid.get(i).price;
                    qty =  bid.get(i).qty;
                }else if (i != 0){
                    qty += bid.get(i).qty;
                }

                if (bid.size() - 1 == i){
                    all += bid.get(i).stockNo + ","+ bid.get(i).BS + "," + price + "," + qty + "|";
                }
            }
        }


        if (ask.size() > 0){
            float price = ask.get(0).price;
            int qty = ask.get(0).qty;
            for(int i = 0; i < ask.size(); i++){
                if (ask.get(i).price != price){
                    all += ask.get(i).stockNo + ","+ ask.get(i).BS + "," + price + "," + qty + "|";
                    price = ask.get(i).price;
                    qty =  ask.get(i).qty;
                }else if (i != 0){
                    qty += ask.get(i).qty;
                }

                if (ask.size() - 1 == i){
                    all += ask.get(i).stockNo + ","+ ask.get(i).BS + "," + price + "," + qty + "|";
                }
            }
        }
        return all;
    }

    private void startRead( AsynchronousSocketChannel sockChannel ) {
        final ByteBuffer buf = ByteBuffer.allocate(2048);
        
        //read message from client
        sockChannel.read( buf, sockChannel, new CompletionHandler<Integer, AsynchronousSocketChannel >() {

            /**
             * some message is read from client, this callback will be called
             */
            @Override
            public void completed(Integer result, AsynchronousSocketChannel channel  ) {

                //ipaddress
                String ipAdr = "";
                try{

                    //Print IPAdress
                    ipAdr = channel.getRemoteAddress().toString();
                    System.out.println(ipAdr);
                }catch(IOException e) {
                    e.printStackTrace();
                }

                //if client is close ,return
                buf.flip();
                if (buf.limit() == 0) return;

                //Print Message
                String msg = getString(buf);

                //time
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
                sdf.setTimeZone(TimeZone.getTimeZone("Asia/Taipei"));
                System.out.println(sdf.format(new Date()) + " " + buf.limit() + " client: "+ ipAdr + " " + msg);

                //
                order ord = null;
                try{
                    ord = ParseOrder(sdf.format(new Date()), msg, ipAdr);
                }catch(Exception ex){
                    startRead( channel );
                    return;
                }
                //2021/05/23 15:55:14.763,TXF,B,10000.0,1,127.0.0.1
                //2021/05/23 15:55:14.763,TXF,S,10000.0,1,127.0.0.1

                if (ord.BS.equals("B")) {
                    limit(ord, bid);
                }
                if (ord.BS.equals("S")) {
                    limit(ord, ask);
                }
                if (trade(bid, ask, match) == true){
                    String content = "Mat:" + match.get(match.size()-1).sysTime + "," + match.get(match.size()-1).stockNo+","+
                    match.get(match.size()-1).BS+","+match.get(match.size()-1).qty+","+match.get(match.size()-1).price+"\n";
                    sendQueue(content);
                    //startWrite(clientChannel, word);
                }

                //send to all 
                //startWrite(clientChannel, "Ord:" + getAllOrder(bid, ask)+"\n");
                sendQueue("Ord:" + getAllOrder(bid, ask)+"\n");
                
                //bid size and ask size
                System.out.println("Blist:" + bid.size() + " Alist:" + ask.size());

                // echo the message
//------------------>                //startWrite( channel, buf );
                
                //start to read next message again
                startRead( channel );
            }

            @Override
            public void failed(Throwable exc, AsynchronousSocketChannel channel ) {
                System.out.println( "fail to read message from client");
            }
        });
    }
        
    private void startWrite( final AsynchronousSocketChannel sockChannel, final String message) {
        
        System.out.println(message);
        ByteBuffer buf = ByteBuffer.allocate(2048);
        buf.put(message.getBytes());
        buf.flip();
        sockChannel.write(buf, sockChannel, new CompletionHandler<Integer, AsynchronousSocketChannel >() {
            @Override
            public void completed(Integer result, AsynchronousSocketChannel channel ) {
                //after message written
                //NOTHING TO DO
            }

            @Override
            public void failed(Throwable exc, AsynchronousSocketChannel channel) {
                System.out.println( "Fail to write the message to server");
            }
        });

    }

    private static void sendQueue(String msg){
        //SQS
        AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

        //Send multiple messages to the queue
        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl("https://sqs.us-east-1.amazonaws.com/912791518131/bidq")
                .withMessageBody(msg);
        sqs.sendMessage(send_msg_request);

    }

    public static void reciveQueue(){

        //SQS
        AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

        // receive messages from the queue
         String queueUrl = "https://sqs.us-east-1.amazonaws.com/912791518131/bidq";
        List<Message> messages = sqs.receiveMessage(queueUrl).getMessages();


        // delete messages from the queue
        for (Message m : messages) {
            System.out.println(m.getBody());
            sqs.deleteMessage(queueUrl, m.getReceiptHandle());
        }
    }


    public static void main( String[] args ) {
        try {
            new MatchServer( "0.0.0.0", 3576 );

            for(;;){
                Thread.sleep(10*1000);
            }
        } catch (Exception ex) {
            Logger.getLogger(MatchServer.class.getName()).log(Level.SEVERE, null, ex);
        }
    } 
}
