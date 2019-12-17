package com.virtual.pairprogrammers;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.*;

public class Socket
{
    public static void main(String[] args) throws IOException, InterruptedException
    {
        ServerSocket echoSocket = new ServerSocket(8989);

        while(true)
        {

            java.net.Socket socket = echoSocket.accept();

            for(int ticks = 0; ticks < 1000; ticks++)
            {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

                double sample = Math.random();

                String level = "DEBUG";

                if(sample < 0.0001)
                {
                    level = "FATAL";
                }

                out.write(level);
                out.flush();

                System.out.println(level);
            }
        }
    }
}
