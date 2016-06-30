package storage;

import java.io.*;
import java.net.*;
import java.lang.Exception.*;
import java.lang.Object;
import common.*;
import rmi.*;
import naming.*;

/** Storage server.

    <p>
    Storage servers respond to client file access requests. The files accessible
    through a storage server are those accessible under a given directory of the
    local filesystem.
 */
public class StorageServer implements Storage, Command
{
    private File root_dir;
    private static int clientport=5000;
    private static int commandport=6000;
    Skeleton <Storage>  storageSkeleton;
    Skeleton <Command>  commandSkeleton;
    private boolean make_dir;
    /** Creates a storage server, given a directory on the local filesystem, and
        ports to use for the client and command interfaces.

        <p>
        The ports may have to be specified if the storage server is running
        behind a firewall, and specific ports are open.

        @param root Directory on the local filesystem. The contents of this
                    directory will be accessible through the storage server.
        @param client_port Port to use for the client interface, or zero if the
                           system should decide the port.
        @param command_port Port to use for the command interface, or zero if
                            the system should decide the port.
        @throws NullPointerException If <code>root</code> is <code>null</code>.
    */
    public StorageServer(File root, int client_port, int command_port)
    {
        if(root==null)
            throw new NullPointerException("Root is null");
        this.root_dir=root;
        if(client_port!=0)
            this.clientport=client_port;
        if(command_port!=0)
            this.commandport=command_port;
    }

    /** Creats a storage server, given a directory on the local filesystem.

        <p>
        This constructor is equivalent to
        <code>StorageServer(root, 0, 0)</code>. The system picks the ports on
        which the interfaces are made available.

        @param root Directory on the local filesystem. The contents of this
                    directory will be accessible through the storage server.
        @throws NullPointerException If <code>root</code> is <code>null</code>.
     */
    public StorageServer(File root)
    {
        if(root==null)
            throw new NullPointerException("Root is null");
        this.root_dir=root;
        /*ServerSocket clientSocket = null;
        ServerSocket commandSocket = null;
        try
        {
            clientSocket = new ServerSocket(0);
            commandSocket = new ServerSocket(0);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        this.clientport=clientSocket.getLocalPort();
        this.commandport=commandSocket.getLocalPort();
        */
    }

    /** Starts the storage server and registers it with the given naming
        server.

        @param hostname The externally-routable hostname of the local host on
                        which the storage server is running. This is used to
                        ensure that the stub which is provided to the naming
                        server by the <code>start</code> method carries the
                        externally visible hostname or address of this storage
                        server.
        @param naming_server Remote interface for the naming server with which
                             the storage server is to register.
        @throws UnknownHostException If a stub cannot be created for the storage
                                     server because a valid address has not been
                                     assigned.
        @throws FileNotFoundException If the directory with which the server was
                                      created does not exist or is in fact a
                                      file.
        @throws RMIException If the storage server cannot be started, or if it
                             cannot be registered.
     */
    public synchronized void start(String hostname, Registration naming_server) throws RMIException, UnknownHostException, FileNotFoundException
    {


        if(!this.root_dir.exists())
            throw new  FileNotFoundException("Root not found");
        if(hostname == null || naming_server == null )
            throw new NullPointerException("host name or naming serbver is null ");


        InetSocketAddress storageAddress=null;
        InetSocketAddress commandAddress=null;
        storageAddress = new InetSocketAddress(hostname, clientport++);
        commandAddress = new InetSocketAddress(hostname, commandport++);

        this.storageSkeleton = new Skeleton<Storage>(Storage.class, this,storageAddress);
        this.commandSkeleton = new Skeleton<Command>(Command.class, this,commandAddress);

        Storage store = Stub.create(Storage.class, storageAddress);
        Command cmd = Stub.create(Command.class, commandAddress);

        this.storageSkeleton.start();
        this.commandSkeleton.start();

        Path[] nameserver_registered=null;
        try
        {
            nameserver_registered = naming_server.register(store , cmd , Path.list(root_dir));
        }
        catch(RMIException e)
        {
            e.printStackTrace();

        }
        for(Path p : nameserver_registered)
        {
            this.delete(p);
        }

        deleteEmpty(root_dir);//C
    }

    private void deleteEmpty(File root_dir2)
    {
        // TODO Auto-generated method stub
        if(root_dir2.isFile())
            return;
        if(root_dir2.list().length >0)
        {
            for (File f : root_dir2.listFiles())
                deleteEmpty(f);
        }
        if(root_dir2.list().length == 0)
            root_dir2.delete();
    }



    /** Stops the storage server.

        <p>
        The server should not be restarted.
     */
    public void stop()
    {
        try
        {
            this.commandSkeleton.stop();
            this.storageSkeleton.stop();
            stopped(null);
        }
        catch (Exception e)
        {
            //e.printStackTrace();
        }
    }

    /** Called when the storage server has shut down.

        @param cause The cause for the shutdown, if any, or <code>null</code> if
                     the server was shut down by the user's request.
     */
    protected void stopped(Throwable cause)
    {
    }

    // The following methods are documented in Storage.java.
    @Override
    public synchronized long size(Path file) throws FileNotFoundException
    {
        if(file==null)
            throw new NullPointerException("Path is null");

        File f=file.toFile(this.root_dir);

        if(!f.exists())
            throw new FileNotFoundException("File does not exist.");
        if(f.isDirectory())
            throw new FileNotFoundException("Path refers to a directory.");

        return f.length();
    }

    @Override
    public synchronized byte[] read(Path file, long offset, int length)
        throws FileNotFoundException, IOException
    {
        if(file==null)
            throw new NullPointerException("Path is null");
        File f = file.toFile(this.root_dir);

        if(!f.exists())
            throw new FileNotFoundException("File does not exist.");
        if(f.isDirectory())
            throw new FileNotFoundException("Path refers to a directory.");

        if(length+offset > this.size(file))
            throw new IndexOutOfBoundsException("length and offset is greater than file size");
        if (length < 0 || offset < 0)
            throw new IndexOutOfBoundsException();


        RandomAccessFile readfile = new RandomAccessFile(f, "r");
        readfile.seek(offset);

        byte[] b=new byte[length];

        int r;
        try
        {
            r = readfile.read(b);
        }
        catch(Exception e)
        {
            readfile.close();
            throw new IOException(e);
        }
        readfile.close();

        if(r < length)
            throw new IndexOutOfBoundsException("Length exceeded the file");

        return b;
    }

    @Override
    public synchronized void write(Path file, long offset, byte[] data)
        throws FileNotFoundException, IOException
    {
      //System.out.println("Write check 11");
        if(file==null||data==null)
            throw new NullPointerException("Path is null");
        //System.out.println("Write check 12");
        File f = file.toFile(this.root_dir);
        //System.out.println("Write check 13");

        if(f.isDirectory())
            throw new FileNotFoundException("Path refers to a directory.");
          //  System.out.println("Write check 14");

        if(!f.exists()){
            //create(file);
            throw new FileNotFoundException("File does not exist.");
          }


      //  System.out.println("Write check 15");
        if (offset < 0)
            throw new IndexOutOfBoundsException();
        if (data.length == 0) {
            return;
        }

        //System.out.println("Write check 1");
        RandomAccessFile writefile = new RandomAccessFile(f, "rw");
        writefile.seek(offset);
        try
        {
            writefile.write(data);
        }
        catch(Exception e)
        {
      //    System.out.println("Write Exception");
            writefile.close();
            throw new IOException(e);
        }
        writefile.close();

    }

    // The following methods are documented in Command.java.
    @Override
    public synchronized boolean create(Path file)
    {
    //  System.out.println("Attempting to create file "+ file.toString());
        if(file.isRoot())
            return false;

        if (file.toFile(root_dir).exists()) {
            return false;
        }

        if(file.parent().toFile(root_dir).isFile()){
        	delete(file.parent());
        }

        boolean tmake_dir = file.parent().toFile(root_dir).mkdirs();
    //    System.out.println("Success in creating parents "+ tmake_dir);
        try
        {
            return file.toFile(root_dir).createNewFile();
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public synchronized boolean delete(Path path)
    {
         if (path == null)
             throw new NullPointerException ("Null Path Cannot be Deleted");
         if(path.isRoot())
             return false;
         if(path.toFile(root_dir).exists()&& ! path.toFile(root_dir).isDirectory())
             return path.toFile(root_dir).delete();

         return recurssive_delete(path.toFile(root_dir));
     }
    private boolean recurssive_delete(File f)
    {
        if (!f.exists())
            return false;
        else if(f.isDirectory())
        {
            for(File file : f.listFiles())
            {
                if (recurssive_delete(file) == false) return false;
            }
        }
        return f.delete();
    }

    @Override
    public synchronized boolean copy(Path file, Storage server)
        throws RMIException, FileNotFoundException, IOException
    {
    //	System.out.println("COPPPPPP" + file + " "+ server);
        if (file == null || server == null)
            throw new NullPointerException("The file or server cannot be null.");

        File f=file.toFile(root_dir);
        if (f.exists())
        {
      //    System.out.println("COPPPPPP222" + file + " "+ server);
            delete(file);
        }
        /*
        else
        {
          System.out.println("COPPPPmmmeee" + file + " "+ server);
          throw new FileNotFoundException("File not found exception");
        }
        */
        if (f.isDirectory())
        {
          throw new FileNotFoundException("File is a directory");
        }

        boolean b =create(file);

      //  System.out.println("COPPPPrrrr" + file + " "+ server);
       if (!b)
       {
    //     System.out.println("COPPPPPP333" + file + " "+ server);
           throw new IOException("File creation failed");
       }

    //   System.out.println("COPPPPPP444" + file + " "+ server);
       long size = server.size(file);
       // int bytesToRead = 1024;
    //   System.out.println("COPPPPPiiiiii" + file + " "+ server);
       long offset=0;

       try {
           while (offset < size) {
               int toRead = (int) Math.min(size - offset,
                                           Integer.MAX_VALUE);
               byte[] data = server.read(file, offset, toRead);
               write(file, offset, data);
               offset += toRead;
           }
           return true;
       } catch (IOException e) {
           throw e;
       }

       catch (Exception e) {
      //   System.out.println("COPPPPPP555" + file + " "+ server);
           return false;
        // throw new UnsupportedOperationException("not implemented");
    }

    }

}
