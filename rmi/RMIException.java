package rmi;

import java.lang.reflect.Method;

/** RMI exceptions. */
public class RMIException extends Exception
{
    /**
     * 
     */
    private static final long serialVersionUID = 8757387233615973524L;
    
    /** Creates an <code>RMIException</code> with the given message string. */
    public RMIException(String message) {
        super(message);
    }

    /** Creates an <code>RMIException</code> with a message string and the given
        cause. */
    public RMIException(String message, Throwable cause) {
        super(message, cause);
    }

    /** Creates an <code>RMIException</code> from the given cause. */
    public RMIException(Throwable cause) {
        super(cause);
    }
}