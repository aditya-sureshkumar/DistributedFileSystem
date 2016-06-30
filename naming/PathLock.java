package naming;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by India on 5/21/2016.
 */


public class PathLock {

    public Queue<ClientId> clientQueue;
    public int readers;
   // public int writers;
    public boolean ExcLockReq;
    //private static final int NoLock=0;
    //private static final int SharedLock=1;
    //private static final int ExcLock=2;


    PathLock(){
        clientQueue = new LinkedList<ClientId>();
        readers=0;
        ExcLockReq=false;
        //writers=0;
        //System.out.println("object created");
    }

    public boolean checkShared(ClientId id)
    {
        if(this.ExcLockReq || this.clientQueue.peek()!=id) //.peek()!=id)
        return true;
        return false;
    }

    public boolean checkExc(ClientId id)
    {
        //System.out.println("readers " + this.readers);
        //System.out.println("exc " + this.ExcLockReq);
        //System.out.println("readers " + (this.clientQueue.peek() == id));
        if( this.readers > 0 || this.ExcLockReq || this.clientQueue.peek()!=id)//|| clientQueue.peek()!=id)
            return true;
        return false;
    }

    public void setLock(boolean exc)
    {
//        System.out.println("setlock exclusive" + exc);
        ClientId id = new ClientId(exc);
        clientQueue.add(id);

       // int a = 0;


        if(!exc) {
            //boolean check =
            while (checkShared(id)) {
//                System.out.println("shared lock");
                try {
                    //ClientId id = new ClientId(exc);
                    //clientQueue.add(id);
                    //a = 1;
                    synchronized (id) {
                        id.wait();
                    }

                } catch (Exception e) {
                }
            }
           // if(a == 0)
            clientQueue.remove(id);
            readers++;

            //TODO - added this
            if(!clientQueue.isEmpty())
                synchronized (clientQueue.peek()){
                    clientQueue.peek().notifyAll();}

//            System.out.println("Got read lock");
        }
        else
        {
            //ExcLockReq++;

//            boolean check = this.writers>0 || this.readers>0 || clientQueue.peek()!=id;
            while (checkExc(id)) {
                //System.out.println("non shared lock");
                try {
                    synchronized (id) {
                        id.wait();
                    }
                }
                catch (Exception e){}
            }

                clientQueue.remove(id);
                ExcLockReq=true;
//            System.out.println("Got write lock");
        }
    }


    public void unsetLock(boolean exc) {
        if(!exc) {
            readers--;

            if(!clientQueue.isEmpty())
                synchronized (clientQueue.peek()) {
              clientQueue.peek().notifyAll();}
        }
        else
        {
           // writers--;
            try {
                ExcLockReq=false;
                if(!clientQueue.isEmpty())
                    synchronized (clientQueue.peek()){
                 clientQueue.peek().notifyAll();}
            }
            catch (Exception e){}
        }
    }
}
