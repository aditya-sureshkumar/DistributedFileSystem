package rmi;

import java.net.*;
import java.lang.reflect.*;
import java.net.InetSocketAddress;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;              


/** RMI skeleton

    <p>
    A skeleton encapsulates a multithreaded TCP server. The server's clients are
    intended to be RMI stubs created using the <code>Stub</code> class.

    <p>
    The skeleton class is parametrized by a type variable. This type variable
    should be instantiated with an interface. The skeleton will accept from the
    stub requests for calls to the methods of this interface. It will then
    forward those requests to an object. The object is specified when the
    skeleton is constructed, and must implement the remote interface. Each
    method in the interface should be marked as throwing
    <code>RMIException</code>, in addition to any other exceptions that the user
    desires.

    <p>
    Exceptions may occur at the top level in the listening and service threads.
    The skeleton's response to these exceptions can be customized by deriving
    a class from <code>Skeleton</code> and overriding <code>listen_error</code>
    or <code>service_error</code>.
*/
public class Skeleton<T>
{
	public int port=-1;
	public T srvr = null ;
    public InetSocketAddress sock_addr = null ;
    public Class<T> sclass = null;
    public ServerSocket listeningSocket = null;
    public listeningThreads serverthread=null;
    public boolean isServerStarted = false;
   
    /** Creates a <code>Skeleton</code> with no initial server address. The
        address will be determined by the system when <code>start</code> is
        called. Equivalent to using <code>Skeleton(null)</code>.

        <p>
        This constructor is for skeletons that will not be used for
        bootstrapping RMI - those that therefore do not require a well-known
        port.

        @param c An object representing the class of the interface for which the
                 skeleton server is to handle method call requests.
        @param server An object implementing said interface. Requests for method
                      calls are forwarded by the skeleton to this object.
        @throws Error If <code>c</code> does not represent a remote interface -
                      an interface whose methods are all marked as throwing
                      <code>RMIException</code>.
        @throws NullPointerException If either of <code>c</code> or
                                     <code>server</code> is <code>null</code>.
     */
    public Skeleton(Class<T> c, T server)
    {
    	if(c == null||server==null) 
    		throw new NullPointerException(" c  is null");
    	if(c.isInterface() != true)
    		throw new Error("c is not an interface");
	   
    	Method[] m = c.getMethods();
    	for (int i = 0; i < m.length; i++) 
    	{
    		Class<?>[] xType  = m[i].getExceptionTypes();
    		boolean flag = false;
    		for(int j=0; j< xType.length ; j++)
    		{
    			if(xType[j].getName().contains("RMIException"))
    				flag = true; 
    		}
    		if(!flag)
    			throw new Error("Not a remote interface");
    	}	    
	   
    	this.sclass = c;
    	this.srvr = server;	
    }
       
    

    /** Creates a <code>Skeleton</code> with the given initial server address.

        <p>
        This constructor should be used when the port number is significant.

        @param c An object representing the class of the interface for which the
                 skeleton server is to handle method call requests.
        @param server An object implementing said interface. Requests for method
                      calls are forwarded by the skeleton to this object.
        @param address The address at which the skeleton is to run. If
                       <code>null</code>, the address will be chosen by the
                       system when <code>start</code> is called.
        @throws Error If <code>c</code> does not represent a remote interface -
                      an interface whose methods are all marked as throwing
                      <code>RMIException</code>.
        @throws NullPointerException If either of <code>c</code> or
                                     <code>server</code> is <code>null</code>.
     */
    public Skeleton(Class<T> c, T server, InetSocketAddress address)
    {
    	if(c == null||server==null) 
    		throw new NullPointerException(" c  is null");
    	if(c.isInterface() != true)
    		throw new Error("c is not an interface");
    	
    	Method[] m = c.getMethods();
    	for (int i = 0; i < m.length; i++) 
    	{
    		Class<?>[] xType  = m[i].getExceptionTypes();
    		boolean flag = false;
    		for(int j=0; j< xType.length ; j++)
    		{
    			if(xType[j].getName().contains("RMIException"))
    				flag = true; 
    		}
    		if(!flag)
    			throw new Error("Not a remote interface");
    	}     
    
    	this.sclass = c;
    	this.srvr = server;
	   
    	if (address != null)
    	{
    		this.sock_addr = address;
    		port = sock_addr.getPort();
    	}
    }

    /** Called when the listening thread exits.

        <p>
        The listening thread may exit due to a top-level exception, or due to a
        call to <code>stop</code>.

        <p>
        When this method is called, the calling thread owns the lock on the
        <code>Skeleton</code> object. Care must be taken to avoid deadlocks when
        calling <code>start</code> or <code>stop</code> from different threads
        during this call.

        <p>
        The default implementation does nothing.

        @param cause The exception that stopped the skeleton, or
                     <code>null</code> if the skeleton stopped normally.
     */
    protected void stopped(Throwable cause)
	{
	}

    /** Called when an exception occurs at the top level in the listening
        thread.

        <p>
        The intent of this method is to allow the user to report exceptions in
        the listening thread to another thread, by a mechanism of the user's
        choosing. The user may also ignore the exceptions. The default
        implementation simply stops the server. The user should not use this
        method to stop the skeleton. The exception will again be provided as the
        argument to <code>stopped</code>, which will be called later.

        @param exception The exception that occurred.
        @return <code>true</code> if the server is to resume accepting
                connections, <code>false</code> if the server is to shut down.
     */
    protected boolean listen_error(Exception exception)
    {
        return false;
    }

    /** Called when an exception occurs at the top level in a service thread.

        <p>
        The default implementation does nothing.

        @param exception The exception that occurred.
     */
    protected void service_error(RMIException exception)
    {
    	
    	try 
    	{
			throw exception;
		} catch (RMIException e) 
    	{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
    }

    /** Starts the skeleton server.

        <p>
        A thread is created to listen for connection requests, and the method
        returns immediately. Additional threads are created when connections are
        accepted. The network address used for the server is determined by which
        constructor was used to create the <code>Skeleton</code> object.

        @throws RMIException When the listening socket cannot be created or
                             bound, when the listening thread cannot be created,
                             or when the server has already been started and has
                             not since stopped.
     */
    public synchronized void start() throws RMIException
    {
        if(isServerStarted == true) 
        	throw new RMIException("Server is already started");
        
    	try
    	{
    		if(sock_addr == null) 
    		{
    			listeningSocket = new ServerSocket(0);
        	    port= listeningSocket.getLocalPort();
        	    sock_addr = new InetSocketAddress(port);
    		}
    		else
    		{
        		listeningSocket = new ServerSocket(port);
    		}
    		
        	serverthread = new listeningThreads();
        	serverthread.start();
        	// new Thread(new listeningThreads()).start();	
  	
    	}
    	catch(Exception e)
        {
    		service_error(null);
        	//e.printStackTrace();
        }
    	
    }
    
    public class listeningThreads extends Thread implements Runnable, Serializable
    {
    	public void run()
    	{
    		isServerStarted = true;
			while(isServerStarted)
        	{
				try 
				{
					Socket ClientSocket = listeningSocket.accept();
					
					SingleClient newSClient = new SingleClient(ClientSocket);
	    			Thread ClientThread = new Thread(newSClient);
	    			ClientThread.start();
				} 
				catch (Exception e) 
				{
					service_error(null);
					//e.printStackTrace();
				}
				
        	}
    	}
    }
    
    public class SingleClient implements Runnable, Serializable
    {
    	Socket ClientSocket = null;
    	
	    ObjectInputStream in = null;
		ObjectOutputStream out = null;
		
		String methodName;
		Object[] params;
		Class<?>[] paramTypes;
		Object result,err=null;
		
		public SingleClient(Socket ClientSocket)
		{
			this.ClientSocket=ClientSocket;
		}
		public void run()
		{
			try
			{
				in = new ObjectInputStream(ClientSocket.getInputStream());
				out = new ObjectOutputStream(ClientSocket.getOutputStream());
				
				methodName = (String) in.readObject();
				params = (Object[]) in.readObject();
				paramTypes = (Class<?>[]) in.readObject();
				
				Method method=null;
				try
				{
					method = sclass.getMethod(methodName, paramTypes);
				}
				catch (Exception e)
				{
					out.close();
					in.close();
					ClientSocket.close();
					service_error(null);						
					//e.printStackTrace();
				}
				try
				{
					result = method.invoke(srvr, params);
				}
				catch(InvocationTargetException e)
				{
					result = (Object)e;
					//e.printStackTrace();
				}
				out.writeObject(result);
				out.close();
				in.close();
				ClientSocket.close();
			}
			catch(Exception e)
			{
				service_error(null);
				//e.printStackTrace();
			}
		}
    }
    
    public void checkServer()
    {
    	if(serverthread!=null)
    	{
    		try 
    		{
				serverthread.join();
			} 
    		catch (InterruptedException e) 
    		{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	else
    	{
    		serverthread=null;
    	}
    }
    /** Stops the skeleton server, if it is already running.

        <p>
        The listening thread terminates. Threads created to service connections
        may continue running until their invocations of the <code>service</code>
        method return. The server stops at some later time; the method
        <code>stopped</code> is called at that point. The server may then be
        restarted.
     * @throws InterruptedException 
     * @throws IOException 
     */
    
    public synchronized void stop()
    {
    	isServerStarted=false;
    	try 
    	{
			listeningSocket.close();
			stopped(null);
			checkServer();
		} 
    	catch (IOException e) 
    	{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
}
