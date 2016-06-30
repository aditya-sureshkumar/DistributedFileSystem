package naming;

public class ClientId {
	public static int clientId;
	public boolean exc;
	private static int counter=0;

	ClientId(boolean e)
	{
		counter++;
		clientId=counter;
		exc=e;
	}
}
