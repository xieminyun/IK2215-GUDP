import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class GUDPSocket implements GUDPSocketAPI {
    private final DatagramSocket datagramSocket;

    volatile LinkedList<GUDPBuffer> sendQueues = new LinkedList<>();
    volatile LinkedList<GUDPBuffer> receiveQueues = new LinkedList<>();
    public BlockingQueue<GUDPPacket> receivingQueue = new LinkedBlockingQueue<>();
    public int sendQueueIndex = -1;
    public int receiveQueueIndex = -1;
    public int expectSeqNo = 0;
    public int bsnSeqNo = 0;
    public int count = 0;


    public boolean sendingExit = false;
    public boolean isRunning = true;
    SendThread sendThread = new SendThread();
    ReceiveThread receiveThread = new ReceiveThread();
    ReceiveAckThread ackThread = new ReceiveAckThread();


    public boolean isReceive = false;

    public GUDPSocket(DatagramSocket socket) throws SocketException {
        datagramSocket = socket;
    }

    public void setSendWindowIndex(int sendWindowIndex) {
        this.sendQueueIndex = sendWindowIndex;
    }

    public void setReceiveQueueIndex(int receiveQueueIndex) {
        this.receiveQueueIndex = receiveQueueIndex;
    }


    public void send(DatagramPacket packet) throws IOException {
        InetAddress packetAddress = packet.getAddress();
        int packetPort = packet.getPort();
        if (sendQueueIndex < 0) {
            //create a new GUDPbuffer for this endpoint
            InetSocketAddress newEndPoint = new InetSocketAddress(packetAddress, packetPort);
            GUDPBuffer sendBuffer = new GUDPBuffer();
            ByteBuffer bsnBuf = ByteBuffer.allocate(GUDPPacket.HEADER_SIZE);
            bsnBuf.order(ByteOrder.BIG_ENDIAN);
            GUDPPacket bsnPacket = new GUDPPacket(bsnBuf);
            //set BSNPacket attributes
            bsnPacket.setSocketAddress(newEndPoint);
            bsnPacket.setVersion(GUDPPacket.GUDP_VERSION);
            bsnPacket.setType(GUDPPacket.TYPE_BSN);
            bsnPacket.setPayloadLength(0);
            Random rand = new Random();
            int randNum = rand.nextInt(Short.MAX_VALUE);
            bsnSeqNo = randNum;
            bsnPacket.setSeqno(randNum);
            sendBuffer.addToSendQueue(bsnPacket);
            sendBuffer.setSequence(randNum + 1);
            sendBuffer.setSent(randNum);
            sendQueues.add(sendBuffer);
            setSendWindowIndex(sendQueues.indexOf(sendBuffer));
        }
        GUDPBuffer currentSendBuffer = sendQueues.get(sendQueueIndex);
        GUDPPacket gudppacket = GUDPPacket.encapsulate(packet);
        int seqNo = currentSendBuffer.getSequence();
        gudppacket.setSeqno(seqNo);
        currentSendBuffer.setSequence(seqNo + 1);
        currentSendBuffer.addToSendQueue(gudppacket);
    }

    public void receive(DatagramPacket packet) throws IOException {
        if (!isReceive) {
            receiveThread.start();
            isReceive = true;
        }
        GUDPPacket gudpPacket = null;
        try {
            gudpPacket = receivingQueue.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        gudpPacket.decapsulate(packet);
    }

    public void finish() throws IOException {
        if (!sendThread.isAlive()) {
            sendThread.start();
        }
    }

    public void close() throws IOException {
    }

    public class SendThread extends Thread {
        @Override
        public void run() {
            System.out.println("start receiving ACK");
            ackThread.start();
            GUDPBuffer gBuf = sendQueues.get(sendQueueIndex);
            LinkedList<wrapperGUDPPacket> sendingQueue = gBuf.getSendQueue();
            LinkedList<GUDPPacket> ackBuffer = gBuf.getAckBuf();
            while (!sendingExit) {
                for (int i = 0; i < ackBuffer.size(); i++) {
                    int ackIndex = ackBuffer.get(i).getSeqno() - 1 - bsnSeqNo;
                    wrapperGUDPPacket ackPacket = sendingQueue.get(ackIndex);
                    if (!ackPacket.ackReceived) {
                        ackPacket.ackReceived = true;
                        ackBuffer.remove(i);
                        count++;
                    }
                }
                int first = gBuf.getFirst();
                int last = gBuf.getLast();
                 for(int index = first; index < last; index ++) {
                   wrapperGUDPPacket packetInSendingQueue = sendingQueue.get(index);
                   if (packetInSendingQueue.ackReceived) {
                       first++;
                       continue;
                   }
                   break;
               }
                gBuf.setFirst(first);
                if (count != sendingQueue.size()) {
                    int newlast = Math.min(sendingQueue.size(), first + GUDPPacket.MAX_WINDOW_SIZE);
                    for (int index = first; index < newlast; index++) {
                        wrapperGUDPPacket packetToBeSent = sendingQueue.get(index);
                        if (!packetToBeSent.sent) {
                            packetToBeSent.markSent();
                            try {
                                DatagramPacket udpPacket = packetToBeSent.gudpPacket.pack();
                                datagramSocket.send(udpPacket);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                    gBuf.setLast(newlast);
                    last = newlast;
                }
                for (int index = first; index < last; index++) {
                    wrapperGUDPPacket packetToBeSent = sendingQueue.get(index);
                    if (packetToBeSent.isTimeout() & packetToBeSent.retryTimes <= 10) {
                        packetToBeSent.markSent();
                        try {
                            DatagramPacket udpPacket = packetToBeSent.gudpPacket.pack();
                            datagramSocket.send(udpPacket);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        packetToBeSent.retryTimes++;
                        System.out.println("Retransmit packet SEQ " + (packetToBeSent.getGudpPacket().getSeqno()) + " for " + packetToBeSent.retryTimes + "time(s)");
                    }
                    if (packetToBeSent.retryTimes == 10) {
                        System.out.println("packet SEQ" + (packetToBeSent.getGudpPacket().getSeqno()) + " fails to send, too many retry times");
                        sendingExit = true;
                        System.out.println("Sender Close");
                        System.exit(1);
                    }
                }

                if (count == sendingQueue.size()) {
                    sendingExit = true;
                    System.out.println("Finish");
                    System.exit(1);
                }
            }
        }
    }

    public class ReceiveThread extends Thread {
        @Override
        public void run() {
            while (isRunning) {
                byte[] buf = new byte[GUDPPacket.MAX_DATAGRAM_LEN];
                DatagramPacket udpPacket = new DatagramPacket(buf, buf.length);
                try {
                    datagramSocket.receive(udpPacket);
                    GUDPPacket gudppacket = GUDPPacket.unpack(udpPacket);
                    if (receiveQueueIndex < 0) {
                        //create a new recevingQueue
                        GUDPBuffer gBuffer = new GUDPBuffer();
                        receiveQueues.add(gBuffer);
                        setReceiveQueueIndex(receiveQueues.indexOf(gBuffer));
                    }
                    GUDPBuffer currentRecQueue = receiveQueues.get(receiveQueueIndex);
                    if (gudppacket.getType() == GUDPPacket.TYPE_BSN) {
                        expectSeqNo = gudppacket.getSeqno() + 1;
                        currentRecQueue.getReceiveBuffer().add(gudppacket);
                        sendAck(gudppacket);
                    }
                    if (gudppacket.getType() == GUDPPacket.TYPE_DATA) {
                        if (gudppacket.getSeqno() == expectSeqNo) {
                            currentRecQueue.getReceiveBuffer().add(gudppacket);
                            sendAck(gudppacket);
                        }
                    }
                    while (true) {
                        GUDPPacket gudpPacket = currentRecQueue.getReceiveBuffer().peek();
                        if (gudpPacket == null) {
                            break;
                        }
                        int seqNo = gudpPacket.getSeqno();
                        if (seqNo < expectSeqNo) {
                            currentRecQueue.getReceiveBuffer().remove();
                            continue;
                        }
                        receivingQueue.add(gudpPacket);
                        currentRecQueue.getReceiveBuffer().remove();
                        expectSeqNo++;
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            }

        }
    }

    private void sendAck(GUDPPacket gudppacket) throws IOException {
        //Send Ack
        ByteBuffer AckBuf = ByteBuffer.allocate(GUDPPacket.HEADER_SIZE);
        AckBuf.order(ByteOrder.BIG_ENDIAN);
        GUDPPacket AckPacket = new GUDPPacket(AckBuf);
        //set AckPacket attributes
        AckPacket.setVersion(GUDPPacket.GUDP_VERSION);
        AckPacket.setType(GUDPPacket.TYPE_ACK);
        AckPacket.setSeqno(gudppacket.getSeqno() + 1);
        AckPacket.setPayloadLength(0);
        InetSocketAddress sock = gudppacket.getSocketAddress();
        InetSocketAddress AckSocketAddr = new InetSocketAddress(sock.getAddress(), sock.getPort());
        AckPacket.setSocketAddress(AckSocketAddr);
        DatagramPacket udpAck = AckPacket.pack();
        datagramSocket.send(udpAck);
        System.out.println("send ACK");
    }

    public class ReceiveAckThread extends Thread {
        @Override
        public void run() {
            while (isRunning) {
                byte[] buf = new byte[VSFtp.MAX_LEN];
                DatagramPacket udpAck = new DatagramPacket(buf, buf.length);
                GUDPBuffer gBuf = sendQueues.get(sendQueueIndex);
                try {
                    datagramSocket.receive(udpAck);
                    GUDPPacket gudpAck = GUDPPacket.unpack(udpAck);
                    gBuf.addToAckBuffer(gudpAck);
                    System.out.println("ACK received");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    class wrapperGUDPPacket {

        final GUDPPacket gudpPacket;
        Instant retransmitTime;
        boolean sent = false;
        boolean ackReceived;
        int retryTimes = 0;

        final Duration TIME_OUT = Duration.ofMillis(1000);
        final int MAX_RETRY_TIMES = 5;

        public wrapperGUDPPacket(GUDPPacket gudpPacket) {
            this.gudpPacket = gudpPacket;
        }

        public GUDPPacket getGudpPacket() {
            return gudpPacket;
        }

        public void markSent() {
            retransmitTime = Instant.now().plus(TIME_OUT).minusNanos(1);
            sent = true;
        }

        public boolean isTimeout() {
            return sent && !ackReceived && Instant.now().isAfter(retransmitTime);
        }
    }

    class GUDPBuffer {
        public LinkedList<wrapperGUDPPacket> sendQueue = new LinkedList<>();
        public LinkedList<GUDPPacket> ackBuf = new LinkedList<>();
        public int first = 0;
        public int last = 1;
        private final Queue<GUDPPacket> receiveBuffer = new PriorityQueue<>(Comparator.comparingInt(GUDPPacket::getSeqno));
        private InetSocketAddress endPoint;
        public int sequence;
        public int sent;

        public GUDPBuffer() {
        }


        public void addToSendQueue(GUDPPacket gPacket) {
            sendQueue.add(new wrapperGUDPPacket(gPacket));
        }


        public void addToAckBuffer(GUDPPacket ackPacket) {
            ackBuf.add(ackPacket);
        }

        public Queue<GUDPPacket> getReceiveBuffer() {
            return receiveBuffer;
        }

        public int getFirst() {
            return first;
        }

        public void setFirst(int first) {
            this.first = first;
        }

        public int getLast() {
            return last;
        }

        public void setLast(int last) {
            this.last = last;
        }

        public void setSequence(int sequence) {
            this.sequence = sequence;
        }

        public void setSent(int sent) {
            this.sent = sent;
        }

        public LinkedList<wrapperGUDPPacket> getSendQueue() {
            return sendQueue;
        }

        public LinkedList<GUDPPacket> getAckBuf() {
            return ackBuf;
        }

        public int getSequence() {
            return sequence;
        }

        public InetSocketAddress getEndPoint() {
            return endPoint;
        }

    }
}


