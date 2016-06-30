package rmi;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.*;
import java.net.InetSocketAddress;
import java.net.Socket;

public class ClientStubDispatcher implements InvocationHandler , Serializable  
{
	InetSocketAddress srvrAddress;
	public ClientStubDispatcher(InetSocketAddress srvrAddress ) 
	{
		this.srvrAddress = srvrAddress;
	}
	
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable 
	{
		try 
		{
			boolean isRemoteFlag = false;
			Class[] exceptionTypes = method.getExceptionTypes();
			for(Class e : exceptionTypes) 
			{
				if(e.equals(RMIException.class)) 
				{
					isRemoteFlag = true;
		        }			
			}
			if(isRemoteFlag==true)
			{
				return invokeRemote(proxy,method,args);
			}
			if(method.getName().equals("equals")) 
			{
				return equals(proxy, method, args);	
			}
			ClientStubDispatcher handler = (ClientStubDispatcher) Proxy.getInvocationHandler(proxy);
	        
			if(method.getName().equals("hashCode")) 
	        {
				return handler.srvrAddress.hashCode() + proxy.getClass().hashCode();
	        }
			if(method.getName().contains("toString")) 
			{
				return "Address: "+ this.srvrAddress.toString()+"    ;     Class: "+this.getClass().getName()+"    ;     SimpleInterface: "+this.getClass().getInterfaces().toString()+"\n";
			}
			return null;
		} 
		catch (Exception e) 
		{
				throw e;
        }
	}

	private Object equals(Object proxy, Method method, Object[] args) 
	{
		 if(args.length != 1) 
		 {
             return Boolean.FALSE;
         }
  
         Object obj = args[0];
         if(obj == null) 
             return Boolean.FALSE;
        
         //  check proxy class and class type
         if(!Proxy.isProxyClass(obj.getClass()) ||(!proxy.getClass().equals(obj.getClass()) ) ) 
             return Boolean.FALSE;
         
         //  check handler type and remote address
         InvocationHandler handler = Proxy.getInvocationHandler(obj);
         if(!(handler instanceof ClientStubDispatcher) || (!srvrAddress.equals(((ClientStubDispatcher) handler).srvrAddress))) 
             return Boolean.FALSE;
         
         return Boolean.TRUE;
	}

	private Object invokeRemote(Object proxy, Method method, Object[] args) throws Throwable 
	{
		Socket socket   = null; 
        Integer result  = null;
        Object obj      = null;
        ObjectOutputStream out=null;
        ObjectInputStream in=null;
        
        try 
        {
            socket = new Socket(); 
            socket.connect(srvrAddress);
            
            out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            
            out.writeObject(method.getName());
            out.writeObject(args);
            out.writeObject(method.getParameterTypes());
            
            in = new ObjectInputStream(socket.getInputStream());
            obj     = in.readObject();
            //close socket
            socket.close();
        } 
        catch (Exception e) 
        {
        	e.printStackTrace();
            throw new RMIException(e);
        } 
        if(obj instanceof InvocationTargetException)
        {
        	Throwable ob = ((InvocationTargetException)obj).getTargetException();
        	throw ob;
        }
        else
        {
        	socket.close();
        	out.close();
        	in.close();
        	return obj;
        }
        
	}

}
