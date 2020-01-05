package com.virtual.pairprogrammers.flink;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.Random;

public class DataSever
{
    public static void main(String ...args) throws IOException
    {
        ServerSocket listener = new ServerSocket(9090);

        try
        {
            Socket socket = listener.accept();
            System.out.println("Got new connection: "+socket.toString());

            try
            {
                PrintWriter out = new PrintWriter(socket.getOutputStream(),true);
                Random rand = new Random();

                int count = 0;
                int tenSum = 0;
                Date d = new Date();

                for(int x=1;x<5000000;x++)
                {
                    int key = (x%2) + 1;
                    String s = key + "," + x;
                    System.out.println(s);
                    out.println(s);
                    Thread.sleep(50);
                }
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            finally
            {
                socket.close();
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            listener.close();
        }
    }
}
