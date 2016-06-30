package naming;


import java.io.*;
import java.net.*;
import java.util.*;
import java.util.Map.Entry;

import rmi.*;
import common.*;
//import javafx.util.Pair;
import storage.*;
import java.util.concurrent.ConcurrentHashMap;


/** Naming server.

    <p>
    Each instance of the filesystem is centered on a single naming server. The
    naming server maintains the filesystem directory tree. It does not store any
    file data - this is done by separate storage servers. The primary purpose of
    the naming server is to map each file name (path) to the storage server
    which hosts the file's contents.

    <p>
    The naming server provides two interfaces, <code>Service</code> and
    <code>Registration</code>, which are accessible through RMI. Storage servers
    use the <code>Registration</code> interface to inform the naming server of
    their existence. Clients use the <code>Service</code> interface to perform
    most filesystem operations. The documentation accompanying these interfaces
    provides details on the methods supported.

    <p>
    Stubs for accessing the naming server must typically be created by directly
    specifying the remote network address. To make this possible, the client and
    registration interfaces are available at well-known ports defined in
    <code>NamingStubs</code>.
 */
public class NamingServer implements Service, Registration
{
    private ConcurrentHashMap<Path,Storage> PSmap;
    private ConcurrentHashMap<Storage,Command> SCmap;
    private ConcurrentHashMap<Path, Integer> repCount;
    private ConcurrentHashMap<Path, HashSet<Storage>> repStorage;
    private HashSet<Path> p_files;
    private HashSet<Path> p_directories;
    private Skeleton<Service> service;
    private Skeleton<Registration> registration;
   //private int count=0;

    private ConcurrentHashMap<Path, PathLock> pathLocks;


    private static final int NoLock=0;
    private static final int SharedLock=1;
    private static final int ExcLock=2;
    /** Creates the naming server object.

        <p>
        The naming server is not started.
     */

    public NamingServer()
    {
        PSmap=new ConcurrentHashMap<Path,Storage>();
        SCmap=new ConcurrentHashMap<Storage,Command>();
        p_files=new HashSet<Path>();
        p_directories=new HashSet<Path>();

        pathLocks=new ConcurrentHashMap<Path,PathLock>();
        Path ini=new Path();
        p_directories.add(ini);
        pathLocks.put(ini, new PathLock());
        repCount = new ConcurrentHashMap<Path,Integer>();
        repStorage= new ConcurrentHashMap<Path, HashSet<Storage>>();

    }

    /** Starts the naming server.

        <p>
        After this method is called, it is possible to access the client and
        registration interfaces of the naming server remotely.

        @throws RMIException If either of the two skeletons, for the client or
                             registration server interfaces, could not be
                             started. The user should not attempt to start the
                             server again if an exception occurs.
     */
    public synchronized void start() throws RMIException
    {

        this.registration = new Skeleton<Registration>(Registration.class,this, new InetSocketAddress("127.0.0.1",NamingStubs.REGISTRATION_PORT));
        this.service = new Skeleton<Service>(Service.class, this,new InetSocketAddress("127.0.0.1",NamingStubs.SERVICE_PORT));

        service.start();
        registration.start();
       // throw new UnsupportedOperationException("not implemented");
    }

    /** Stops the naming server.

        <p>
        This method commands both the client and registration interface
        skeletons to stop. It attempts to interrupt as many of the threads that
        are executing naming server code as possible. After this method is
        called, the naming server is no longer accessible remotely. The naming
        server should not be restarted.
     */
    public void stop()
    {
        try
        {
            this.service.stop();
            this.registration.stop();
            this.stopped(null);
        }
        catch (Exception e)
        {
            this.stopped(e);
        }

        //throw new UnsupportedOperationException("not implemented");
    }

    /** Indicates that the server has completely shut down.

        <p>
        This method should be overridden for error reporting and application
        exit purposes. The default implementation does nothing.

        @param cause The cause for the shutdown, or <code>null</code> if the
                     shutdown was by explicit user request.
     */
    protected void stopped(Throwable cause)
    {
    }

    // The following public methods are documented in Service.java.
    @Override
    public void lock(Path path, boolean exclusive) throws FileNotFoundException
    {

        if(path==null)
            throw new NullPointerException("path null");

        if(!pathLocks.containsKey(path)){
            throw new FileNotFoundException("File not found");
        }
        /*
        if(path.toString().equals("/")) {
            //pathLocks.get("/").setLock(exclusive);
            if(pathLocks.containsKey(path))
                pathLocks.get(path).setLock(exclusive);
            else
                pathLocks.put(path,new PathLock());

            System.out.println("root value " + pathLocks.get(path));
        }*/



        ArrayList<Path> listOfDir = new ArrayList<Path>();
        listOfDir.add(path);
        Path path1 = new Path(path.toString());
        if(!path1.toString().equals("/")) {
            while (!path1.isRoot()) {
                Path temp = path1.parent();
                listOfDir.add(temp);
                path1 = temp;
            }
        }


        for(int i=0;i<listOfDir.size();i++)
        {
            if(pathLocks.get(listOfDir.get(i))==null) {
//                System.out.println(path + " does not exist so creating the object" );
                pathLocks.put(listOfDir.get(i), new PathLock());

            }
        }

//        System.out.println("Need "+exclusive +"for"+path.toString());

//        System.out.println("Hashtable current status:");
        //for(int i=0;i<pathLocks.size();i++)
        for (ConcurrentHashMap.Entry<Path,PathLock> i : pathLocks.entrySet()) {
//            System.out.println(i.getKey() + " " + i.getValue().readers);
            //System.out.println(i.getKey() + " " + i.getValue().writers);
//            System.out.println(i.getKey() + " " + i.getValue().ExcLockReq);
        }

        Collections.sort(listOfDir);
        int i=0;


        //if(listOfDir.size()>1) {
            for (i = 0; i < listOfDir.size(); i++) {
//                System.out.println("locking processing: " + listOfDir.get(i) + " " + i);
                if (exclusive && i == listOfDir.size() - 1) {
                    try {
//                        System.out.println("lock path " + path + " exclusive original " + exclusive + " subdirectory " + listOfDir.get(i) + " true");
                        pathLocks.get(listOfDir.get(i)).setLock(exclusive);//w
                    } catch (Exception e) {
                    }
                } else {
                    try {
//                        System.out.println("lock path " + path + " exclusive original " + exclusive + " subdirectory " + listOfDir.get(i) + " false");
                        pathLocks.get(listOfDir.get(i)).setLock(false);
                    } catch (Exception e) {
                    }
                }

            }

        //}

            // Replication
            if(p_files.contains(path)){ // path is a file
                if(!repStorage.containsKey(path)){
                    HashSet<Storage> temp = new HashSet<Storage>();
                    temp.add(PSmap.get(path));
                    repStorage.put(path, temp);
                    repCount.put(path, 1);
                }

            HashSet hs= new HashSet<Storage>();
            int c;
            if(!exclusive){ // read request
                if(repCount.containsKey(path)){
                    c=repCount.get(path).intValue();
                    c++;
                    if(c==20){
                        c=0;
                        // create a copy
                        //Random rand = new Random();
                        //int serInd = rand.nextInt(SCmap.size());

                        Iterator<Entry<Storage, Command>> it = SCmap.entrySet().iterator();

                        while(it.hasNext())
                        {
                            Entry<Storage, Command> entry = (Entry<Storage, Command>) it.next();

                            Storage s = entry.getKey();
                            Command sc = entry.getValue();

                            if(!repStorage.get(path).contains(s)){
                                Storage orig = (Storage) PSmap.get(path);
                                Command corig = (Command) SCmap.get(orig);
                                try {
                                    if(!sc.copy(path, orig))
                                        continue;
                                } catch (RMIException e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                                } catch (IOException e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                                }
                                hs = repStorage.get(path);
                                hs.add(s);
                                repStorage.put(path, hs);
                                PSmap.put(path, s);
                                break;
                            }
                        }

                        //update
                        repCount.put(path, c);
                    }
                    else{
                        repCount.put(path, c);
                    }

                    }// contains path
                }// read replication
            else{ // write

                HashSet hswrite = repStorage.get(path);
                hswrite.remove(PSmap.get(path));

                if(! hswrite.isEmpty()){
                    Iterator ithsw = hswrite.iterator();

                    while(ithsw.hasNext()){
                        Storage sw = (Storage) ithsw.next();
                        Command swc = (Command) SCmap.get(sw);

                        try {
                            swc.delete(path);
                        } catch (RMIException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                }
                hswrite.clear();
                hswrite.add(PSmap.get(path));
                repStorage.put(path, hswrite);

                }// write replication

            } // replication end for file





        //throw new UnsupportedOperationException("not implemented");
    } // end lock

    @Override
    public void unlock(Path path, boolean exclusive)
    {
        if(path==null)
            throw new NullPointerException("PAth is null");

        if(!pathLocks.containsKey(path)){
            throw new IllegalArgumentException("File not found");
        }


        ArrayList<Path> listOfDir = new ArrayList<Path>();
        listOfDir.add(path);
        Path path1 = new Path(path.toString());
        if(!path1.toString().equals("/"))
        {
            while (!path1.isRoot())
            {
                Path temp = path1.parent();
                listOfDir.add(temp);
                path1 = temp;
            }
        }

        for(int i=0;i<listOfDir.size();i++)
        {
            if(pathLocks.get(listOfDir.get(i))==null)
                pathLocks.put(listOfDir.get(i),new PathLock());
        }


        Collections.sort(listOfDir);
        Path ptemp;
        for(int it=listOfDir.size()-1 ; it>=0 ;it--){
//            System.out.println("unlocking processing: "+listOfDir.get(it));
            try {
                if (exclusive && it==listOfDir.size()-1 ) {
                    pathLocks.get(listOfDir.get(it)).unsetLock(exclusive);
                } else
                    pathLocks.get(listOfDir.get(it)).unsetLock(false);
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }

        //throw new UnsupportedOperationException("not implemented");
        //System.out.println("UNLOCKED");
    }

    @Override

    public boolean isDirectory(Path path) throws FileNotFoundException
    {
        if(path==null)
            throw new NullPointerException("Path is null");
        if(path.isRoot())
            return true;

        lock(path.parent(), false); //lock the parent ,shared

        if(p_directories.contains(path))
        {
            unlock(path.parent(), false); //unlock the parent , shared
            return true;
        }
        else if(p_files.contains(path))
        {
            unlock(path.parent(), false); //unlock the parent , shared
            return false;
        }


        /*
        Iterator<Path> i = p_directories.iterator();

        while(i.hasNext())
        {
            Path check=i.next();
            if(check.isSubpath(path))
                return true;
        }
        */
        throw new FileNotFoundException("Path not found, i.e it is not a directory nor a file");
    }

    @Override
    public String[] list(Path directory) throws FileNotFoundException
    {
        if(directory==null)
            throw new NullPointerException("Path is null");

        boolean checkcondition=false;
        if(p_directories.contains(directory))
            checkcondition=true;
        if(checkcondition)
        {
            //System.out.println("ENTEREDDD");
            lock(directory, false); //lock current , shared
            ArrayList<Path> fps=new ArrayList<Path>();
            ArrayList<String> rlist=new ArrayList<String>();

            ArrayList<Path> subdirps=new ArrayList<Path>();
            String directstr=directory.toString();
            //System.out.println("DIRECT"+directstr);
            Iterator<Path> j = p_directories.iterator();
            while(j.hasNext())
            {
                Path sd=j.next();
                if(sd.isSubpath(directory))
                    subdirps.add(sd);
            }

            int sbsize=subdirps.size();
            for(int k=0;k<sbsize;k++)
            {
                //System.out.println("LLLLLLLDIRECT"+subdirps.get(k));
                String cstr=subdirps.get(k).toString();
                cstr=cstr.substring(directstr.length());

                //System.out.println("StrAFTERdel"+cstr);

                int cslash=0;
                for(int l=0;l<cstr.length();l++)
                {
                    if(cstr.charAt(l)=='/'&&l!=0)
                        cslash++;
                }
                if(cslash<1&&!cstr.isEmpty())
                {
                    String addingstr=subdirps.get(k).toString();
                    addingstr=addingstr.substring(directstr.length());
                    addingstr=addingstr.replace("/", "");
                    rlist.add(addingstr);
                    //System.out.println("ADDED"+subdirps.get(k));

                }
            }


            Iterator<Path> i = p_files.iterator();
            while(i.hasNext())
            {
                Path f=i.next();
                if(f.isSubpath(directory))
                    fps.add(f);
            }
            int fpssize=fps.size();
            for(int k=0;k<fpssize;k++)
            {
                //System.out.println("FFFF"+fps.get(k));
                String cstr=fps.get(k).toString();
                cstr=cstr.substring(directstr.length());
                //System.out.println("StrAFTERdel"+cstr);
                int cslash=0;
                for(int l=0;l<cstr.length();l++)
                {
                    if(cstr.charAt(l)=='/'&&l!=0)
                        cslash++;
                }
                if(cslash<1&&!cstr.isEmpty())
                {
                    String addingstr=fps.get(k).toString();
                    addingstr=addingstr.substring(directstr.length());
                    addingstr=addingstr.replace("/", "");
                    rlist.add(addingstr);
                    //System.out.println("ADDEDFFFF"+fps.get(k));
                }
            }

            int rsize=rlist.size();
            String[] retlist = new String[rsize];
            for(int k=0;k<rsize;k++)
            {
                retlist[k]=rlist.get(k);
            }
            /*
            for(String s:retlist)
                System.out.println("\nLOLOLOL:\n\n"+s+"\t"+"\n\n");
                */
            unlock(directory, false); //unlock current , shared
            return retlist;
        }

        throw new FileNotFoundException("Directory does not exist");


    }

    @Override
    public boolean createFile(Path file)
            throws RMIException, FileNotFoundException
    {
        //Check whether file is null
        if(file==null)
        {
            //System.out.println("File is null");
            throw new NullPointerException();
        }


        if(file.isRoot())
        {
            //System.out.println("");
            return false;
        }

        //Check whether file already exists
        if(p_files.contains(file) || p_directories.contains(file))
        {
           // System.out.print("File already exists");
            return false;
        }

        //Check whether the parent directory exists
        if(!p_directories.contains(file.parent()))
        {
            //System.out.print("Parent directory does not exist");
            throw new FileNotFoundException();
        }
        lock(file.parent(), true); //lock the parent , exclusive

        try
        {

            //Pick a random server from the set
            Random rand = new Random();
            int serInd = rand.nextInt(SCmap.size());
          //  System.out.println(" Selected server  "+ serInd);
            int ind=0;

            Iterator<Entry<Storage, Command>> it = SCmap.entrySet().iterator();

            while(it.hasNext())
            {

                if(ind==serInd)
                {
                    Entry<Storage, Command> entry = (Entry<Storage, Command>) it.next();
                    Command sc= (Command) entry.getValue();
                    sc.create(file);
                    PSmap.put(file, entry.getKey());
                    p_files.add(file);
                    pathLocks.put(file, new PathLock());
                }
                ind++;
            }//end while

            unlock(file.parent(),true); //unlock the parent , exclusive
            return true;
        }//end try
        catch(RMIException e)
        {
            throw new RMIException("RMI EXception");
        }
                    //throw new UnsupportedOperationException("not implemented");
    }// end create file


        /** Creates the given directory, if it does not exist.

        <p>
        The parent directory should be locked for exclusive access before this
        operation is performed.

        @param directory Path at which the directory is to be created.
        @return <code>true</code> if the directory is created successfully,
                <code>false</code> otherwise. The directory is not created if
                a file or directory with the given name already exists.
        @throws FileNotFoundException If the parent directory does not exist.
        @throws RMIException If the call cannot be completed due to a network
                             error.
     */
    @Override
    public boolean createDirectory(Path directory) throws FileNotFoundException
    {

        //Check whether file is null
        if(directory==null)
        {
           // System.out.println("Directory is null");
            throw new NullPointerException();
        }
        if(directory.isRoot())
        {
           // System.out.println("Directory is root. Returning");
            return false;
        }

        //Check whether file already exists
        if(p_directories.contains(directory))
        {
           // System.out.print("Directory already exists");
            return false;
        }
        if(p_files.contains(directory))
        {
           // System.out.print("Directory given is a file");
            return false;
        }
        //Check whether the parent directory exists
        if(!p_directories.contains(directory.parent()))
        {
          //  System.out.print("Parent directory does not exist");
            throw new FileNotFoundException();
        }
       // System.out.println("PARENT OF DIRECTORY IS "+directory.parent());
        lock(directory.parent(),true); // lock the parent, for exclusive

       // System.out.println("HERE!");
        p_directories.add(directory);
        pathLocks.put(directory, new PathLock());

        unlock(directory.parent(),true); // unlock the parent, for exclusive
        return true;
        //throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean delete(Path path) throws FileNotFoundException
    {

        if(path == null)
            throw new NullPointerException("path is null");
        if(path.isRoot())
            return false;
        if(!p_files.contains(path) && !p_directories.contains(path)){
        	throw new FileNotFoundException(" Invalid PAth" );
        }
        /*
        if(PSmap.get(path)==null)
            throw new FileNotFoundException("Storage stub is not found");
        */
        if(p_files.contains(path)) // if path is a file
        {
	        HashSet<Storage> hstemp= repStorage.get(path);
	        Iterator hsit = hstemp.iterator();
	        int count=0;

	        while(hsit.hasNext()){
	        	Storage s = (Storage) hsit.next();
	        	Command c= SCmap.get(s);

	        	if (c==null)
	                throw new FileNotFoundException("Command stub is not found");

	            lock(path.parent(), true); // lock parent for exclusive
	            try
	            {
	                boolean b = c.delete(path);
	                if(b)
	                	count++;
	                unlock(path.parent(), true); // unlock parent for exclusiv
	            }
	            catch (RMIException e)
	            {
	                // TODO Auto-generated catch block
	                e.printStackTrace();
	            }
	        }

	        if(count==hstemp.size())
	        {
	            p_files.remove(path);
	            PSmap.remove(path);
	            pathLocks.remove(path);
	            repStorage.remove(path);
	            repCount.remove(path);
	         return true;
	        }
	        return false;
        } // for file

        //Directory
        HashSet<Storage> hsSt = new HashSet<Storage>();

        //Find all sub-files and delete them
        Iterator itfiles = p_files.iterator();

        //System.out.println("to be deleted "+ path.toString());

        //System.out.println("Printign asll files "+ p_files.toString());

        Path pt;
        while(itfiles.hasNext()){
        	pt=(Path) itfiles.next();
        	//System.out.println("Files : " + pt.toString());
        	if(pt.toString().trim().startsWith(path.toString().trim())){
        		//System.out.println("Found match : " + pt.toString());
        		if(repStorage.containsKey(pt)) {
        			hsSt.addAll(repStorage.get(pt));}
        		else if(PSmap.containsKey(pt)){
        			hsSt.add(PSmap.get(pt));
        		}
        	}
        }

        Iterator iths = hsSt.iterator();
        //System.out.println("Deleting from storage" + hsSt.toString());

        int checkcount=0;
        while(iths.hasNext()){
        	Storage dummyS = (Storage) iths.next();
        	Command dummyC = SCmap.get(dummyS);
        	try {

				boolean b= dummyC.delete(path);
				if(b)
					checkcount++;
			} catch (RMIException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        if(checkcount==hsSt.size())
        {
        	p_directories.remove(path);
        	return true;
        }

        return false;
    } // end delete

    @Override
    public Storage getStorage(Path file) throws FileNotFoundException
    {
        if (file == null)
            throw new NullPointerException("file is null");
        //Storage s =
        if(PSmap.get(file) == null)
            throw new FileNotFoundException("File not in the naming server");
        return PSmap.get(file);

        //throw new UnsupportedOperationException("not implemented");
    }

    // The method register is documented in Registration.java.
    @Override
    public Path[] register(Storage client_stub, Command command_stub,
                           Path[] files)
    {

        //count++;
        if(command_stub==null)
            throw new NullPointerException();
        if(client_stub==null)
            throw new NullPointerException();
        if(files==null)
            throw new NullPointerException();

        if(SCmap.containsKey(client_stub))
        {
            throw new IllegalStateException("Duplicate registeration");
        }
        SCmap.put(client_stub, command_stub);

        ArrayList<Path> duplicatelist=new ArrayList<Path>();
        Path[] dpaths;
        for(Path f : files)
        {
            if(!f.isRoot())
            {
                if(p_files.contains(f)||p_directories.contains(f))
                {
                    duplicatelist.add(f);
                }
                else
                {

                    Iterator<String> i = f.iterator();
                    Path p=new Path();
                    while(i.hasNext())
                    {
                        String n=i.next();
                        if(i.hasNext())
                        {
                            Path d=new Path(p,n);
                            p=d;
                            p_directories.add(d);
                            pathLocks.put(d, new PathLock());
                        }
                    }
                    p_files.add(f);
                    pathLocks.put(f, new PathLock());
                    PSmap.put(f, client_stub);
                }
            }
        }

        int s=duplicatelist.size();
        dpaths=new Path[s];
        for(int i=0;i<s;i++)
        {
            dpaths[i]=duplicatelist.get(i);
        }
        /*
        for(Path p:dpaths)
            System.out.println("\nLOLOLOL:\n\n"+p.toString()+"\t"+count+"\n\n");
        */
        return dpaths;

    }
}
