//package GUDP;

import java.lang.reflect.Type;
import java.net.DatagramPacket;
import java.io.IOException;

public interface GUDPSocketAPI {

    public void send(DatagramPacket packet) throws IOException, InterruptedException;

    public void receive(DatagramPacket packet) throws IOException;

    public void finish() throws IOException;

    public void close() throws IOException;
}

