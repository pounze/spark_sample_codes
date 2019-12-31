package com.virtual.pairprogrammers.flink;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.Random;

public class SocketExample
{
    public static void main(String ...args) throws IOException
    {
        ServerSocket listener = new ServerSocket(9090);

        try
        {
            Socket socket = listener.accept();

            try
            {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                Random rand = new Random();
                Date d = new Date();
                while(true)
                {
                    int i = rand.nextInt(1000);

                    String s = "" + System.currentTimeMillis()+","+i;
                    out.println(s);
                    Thread.sleep(50);
                }
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            finally {
                socket.close();
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        finally {
            listener.close();
        }
    }
}
