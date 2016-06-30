package common;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.rmi.*;
import java.lang.Object;

/** Distributed filesystem paths.

    <p>
    Objects of type <code>Path</code> are used by all filesystem interfaces.
    Path objects are immutable.

    <p>
    The string representation of paths is a forward-slash-delimeted sequence of
    path components. The root directory is represented as a single forward
    slash.

    <p>
    The colon (<code>:</code>) and forward slash (<code>/</code>) characters are
    not permitted within path components. The forward slash is the delimeter,
    and the colon is reserved as a delimeter for application use.
 */
public class Path implements Iterable<String>, Comparable<Path>, Serializable
{
    private static final String root="/";
    static String delim="/";
    private ArrayList<String> path;

    /** Creates a new path which represents the root directory. */
    public Path()
    {
        this.path=new ArrayList<String>();
        path.add(root);

        //throw new UnsupportedOperationException("not implemented");
    }

    /** Creates a new path by appending the given component to an existing path.

        @param path The existing path.
        @param component The new component.
        @throws IllegalArgumentException If <code>component</code> includes the
                                         separator, a colon, or
                                         <code>component</code> is the empty
                                         string.
    */
        public Path(Path path, String component)
    {
        if(component.isEmpty()){
            throw new IllegalArgumentException("Component is empty ");
        }
        boolean checkdel = component.contains("/");
        boolean checkcol = component.contains(":");

        if (checkdel || checkcol){
            throw new IllegalArgumentException("Component has ':' and '/' ");
        }

        this.path=new ArrayList<String>();

        //this.path=path.path.add(delim);
        this.path.add(root);

        Iterator<String> it_st = path.iterator();

        while(it_st.hasNext()){
            this.path.add(it_st.next());
        }

        this.path.add(component);
        //path.path.a
        //this.path=path.path.add("/")
            //  this.path.add(component);
        //throw new UnsupportedOperationException("not implemented");
    }

    /** Creates a new path from a path string.

        <p>
        The string is a sequence of components delimited with forward slashes.
        Empty components are dropped. The string must begin with a forward
        slash.

        @param path The path string.
        @throws IllegalArgumentException If the path string does not begin with
                                         a forward slash, or if the path
                                         contains a colon character.
     */
    public Path(String path)
    {
            boolean checkcol = path.contains(":");
        if (path.isEmpty() || path.charAt(0)!='/' || checkcol){
            throw new IllegalArgumentException("Path begins with '/' or has ':' ");
        }

        this.path=new ArrayList<String>();
        this.path.add(this.root);
        String[] dummy = path.split("/");
        //System.out.println("String **** "+ path);
        //System.out.println("String Length **** "+ dummy.length);
        for(int i =0; i<dummy.length ;i++){
            //System.out.println("compon "+ i + "");
                if(!dummy[i].isEmpty()){
            this.path.add(dummy[i]);
            //System.out.println("Adding"+ i + " "+dummy[i] );
            }
        }
        //throw new UnsupportedOperationException("not implemented");
    }


    /** Returns an iterator over the components of the path.

        <p>
        The iterator cannot be used to modify the path object - the
        <code>remove</code> method is not supported.

        @return The iterator.
     */
    @Override
    public Iterator<String> iterator()
    {
            final int pathLength=this.path.size();

        Iterator<String> pathIt = new Iterator<String>() {

            private int index = 1;

            @Override
            public boolean hasNext() {
                if(index>=pathLength){
                    return false;
                }
                return true;
            }

            @Override
            public String next() {
                if(hasNext()){
                    return path.get(index++);
                }
                else
                    throw new NoSuchElementException("Iterator out of bounds");
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };

        return pathIt;
        //throw new UnsupportedOperationException("not implemented");
    }

    /** Lists the paths of all files in a directory tree on the local
        filesystem.

        @param directory The root directory of the directory tree.
        @return An array of relative paths, one for each file in the directory
                tree.
        @throws FileNotFoundException If the root directory does not exist.
        @throws IllegalArgumentException If <code>directory</code> exists but
                                         does not refer to a directory.
     */
         public static Path[] list(File directory) throws FileNotFoundException
         {

            if(!directory.exists()){
                throw new FileNotFoundException(directory.toString() + " does not exist");
            }
            if(!directory.isDirectory()){
                throw new IllegalArgumentException(directory.toString() + " is not a directory");
            }


            ArrayList<Path> retPath = list_helper(directory, directory.getPath().length());

            Path[] finPath=new Path[retPath.size()];

            return (retPath.toArray(finPath));

             //throw new UnsupportedOperationException("not implemented");
         }

         public static ArrayList<Path> list_helper(File directory, int pdirLength){

            ArrayList<Path> retPath = new ArrayList<Path>();

            File[] subFiles= directory.listFiles();

            for(int i =0; i < subFiles.length ; i++){
                File f = subFiles[i];

                if(f.isDirectory()){
                    ArrayList<Path> tempPath = list_helper(f,pdirLength);
                    for(int ti=0; ti<tempPath.size(); ti++){
                        retPath.add(tempPath.get(ti));
                    }

                }
                else{
                    retPath.add(new Path(f.getPath().substring(pdirLength)));
                }

            }
            return retPath;
         }
    /** Determines whether the path represents the root directory.

        @return <code>true</code> if the path does represent the root directory,
                and <code>false</code> if it does not.
     */
    public boolean isRoot()
    {
            if(this.toString().equals(root))
                return true;
            else
            return false;
    //throw new UnsupportedOperationException("not implemented");
    }

    /** Returns the path to the parent of this path.

        @throws IllegalArgumentException If the path represents the root
                                         directory, and therefore has no parent.
     */
    public Path parent()
    {
            if(this.isRoot())
        throw new IllegalArgumentException("Root has no parent");

        StringBuilder str=new StringBuilder();
        for(int i=0; i< this.path.size()-1; i++){
        //  System.out.println(this.path.get(i));
            str.append(this.path.get(i));
            if(i!=0){
            str.append(delim);
            }
        }
        int strlen=str.length();
        if(strlen>1){
            str.deleteCharAt(strlen-1);
        }

    //  System.out.println("Parent "+ str.toString());
        Path parPath=new Path(str.toString());
        return parPath;
    //throw new UnsupportedOperationException("not implemented");
        //throw new UnsupportedOperationException("not implemented");
    }

    /** Returns the last component in the path.

        @throws IllegalArgumentException If the path represents the root
                                         directory, and therefore has no last
                                         component.
     */
    public String last()
    {
        String lst;

        if(this.path.size()==1){
            throw new IllegalArgumentException(" path represents the root "+
                                         "directory, and has no last"+
                                         "component ");
        }

        return this.path.get(this.path.size()-1);

        //throw new UnsupportedOperationException("not implemented");
    }

    /** Determines if the given path is a subpath of this path.

        <p>
        The other path is a subpath of this path if it is a prefix of this path.
        Note that by this definition, each path is a subpath of itself.

        @param other The path to be tested.
        @return <code>true</code> If and only if the other path is a subpath of
                this path.
     */
    public boolean isSubpath(Path other)
    {
        if(this.toString().startsWith(other.toString())){
            return true;
        }

        return false;
    }

    /** Converts the path to <code>File</code> object.

        @param root The resulting <code>File</code> object is created relative
                    to this directory.
        @return The <code>File</code> object.
     */
    public File toFile(File root)
    {
        //CHANGE
        if(root!=null){
        return (new File(root,this.toString()));
        }
        else
            return (new File(this.toString()));
        //throw new UnsupportedOperationException("not implemented");
    }

    /** Compares this path to another.

        <p>
        An ordering upon <code>Path</code> objects is provided to prevent
        deadlocks between applications that need to lock multiple filesystem
        objects simultaneously. By convention, paths that need to be locked
        simultaneously are locked in increasing order.

        <p>
        Because locking a path requires locking every component along the path,
        the order is not arbitrary. For example, suppose the paths were ordered
        first by length, so that <code>/etc</code> precedes
        <code>/bin/cat</code>, which precedes <code>/etc/dfs/conf.txt</code>.

        <p>
        Now, suppose two users are running two applications, such as two
        instances of <code>cp</code>. One needs to work with <code>/etc</code>
        and <code>/bin/cat</code>, and the other with <code>/bin/cat</code> and
        <code>/etc/dfs/conf.txt</code>.

        <p>
        Then, if both applications follow the convention and lock paths in
        increasing order, the following situation can occur: the first
        application locks <code>/etc</code>. The second application locks
        <code>/bin/cat</code>. The first application tries to lock
        <code>/bin/cat</code> also, but gets blocked because the second
        application holds the lock. Now, the second application tries to lock
        <code>/etc/dfs/conf.txt</code>, and also gets blocked, because it would
        need to acquire the lock for <code>/etc</code> to do so. The two
        applications are now deadlocked.

        @param other The other path.
        @return Zero if the two paths are equal, a negative number if this path
                precedes the other path, or a positive number if this path
                follows the other path.
     */
    @Override
    public int compareTo(Path other)
    {
        if(isSubpath(other)){
            return 1;
        }
        if(other.toString().equals(this.toString())){
            return 0;
        }

        return -1;

        //throw new UnsupportedOperationException("not implemented");
    }

    /** Compares two paths for equality.

        <p>
        Two paths are equal if they share all the same components.

        @param other The other path.
        @return <code>true</code> if and only if the two paths are equal.
     */
    @Override
    public boolean equals(Object other)
    {
            if(! (other instanceof Path)){
            throw new IllegalArgumentException("Parameter not of type Path");
        }

        Path otemp = (Path) other;
        /*
        for(int i=0; i< this.path.size(); i++){
            if(! this.path.get(i).equals(otemp.path.get(i))){
                return false;
            }
        }
        */
        //  System.out.println("Equals Path 1" + this.toString() );
            //System.out.println("Equals Path 2" + other.toString() );
        if(this.toString().equals(other.toString())){
            return true;
        }
        else{
            return false;
        }

        //throw new UnsupportedOperationException("not implemented");
    }

    /** Returns the hash code of the path. */
    @Override
    public int hashCode()
    {
        int result=0;
        for(int i=0; i< this.path.size(); i++){
            result += this.path.get(i).hashCode();
        }
        return result;
        //throw new UnsupportedOperationException("not implemented");
    }

    /** Converts the path to a string.

        <p>
        The string may later be used as an argument to the
        <code>Path(String)</code> constructor.

        @return The string representation of the path.
     */
    @Override
        public String toString()
    {
        StringBuilder str=new StringBuilder();
        for(int i=0; i< this.path.size(); i++){
            //System.out.println("******Component " +this.path.get(i));
            str.append(this.path.get(i));
                if(i!=0){
            str.append(delim);
            }
        }
        int strlen=str.length();

            if(strlen>1){
            str.deleteCharAt(strlen-1);
        }
            //System.out.println("******To string output" +str.toString());
        //throw new UnsupportedOperationException("not implemented");/Users/shrestha/SP16/DS/DSProj2/starter
        return str.toString();
    }
}
