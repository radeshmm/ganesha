----- DESCRIPTION -----

   Ganesha is a sleek NoSQL database written in Java.

   It requires no external libraries and provides synchronized binary, object, object attribute, and list based storage across a self-healing redundant cluster of server nodes. All required source is provided here.

   Ganesha has been used in production since August 2013 on a 5 server cluster by DrawCast, a social network for artists (for iOS and Android).  The cluster currently processes an average of 3000 database API calls per second, or 8 billion calls per month with an average load of .25 for each server (standard deviation of .6).



----- FEATURES -----

   Replication of Data - data is replicated automatically across as many servers as you want it to be.

   Auto-Healing Data - any time a piece of data is accessed, any server that doesn't have the most recent copy of the data will be updated.  Also, servers keep logs of failed data updates (for example if another server was temporarily down) so that the update can be sent later on.
   
   Built in Object Support - in addition to strictly storing bytes, a robust object architecture is in place which provides a built in mechanism for storing and retrieving lists, ints, longs, bytes, and strings from and to objects.

   Checksum/Timestamp Consistency Checks - both checksums and timestamps are used to ensure that all relevant servers have the correct copy of a given piece of data.   
   
   Support for Non-Homogeneous Servers - storage/load is based proportionally on individual server storage capacity and allows clusters to be built of machines with mixed storage capacities.
   
   Lists - built in synchronized support for storing lists of object ids as well as inserting sorted ids based on a given developer-defined metric. 
   
   Object Locking/Synchronization- built in object locking and synchronization keeps data consistent and prevents data corruption.
   
   Support for Growable Clusters - live clusters can be grown easily, have no inherent limit in size and require no additional work or data splitting when new nodes are added.
   
   Optional In-Memory-Only Data - useful for demanding tasks such as news feeds.  Unlike normally handled data, no disk usage of any kind is used for the in-memory based data. In-memory-only data, like normal data, is self-healing and persistent as long as all servers holding a piece of data are not simultaneously down.
   
   Long Integer IDs for Data - long integer ids are used for simplicity and efficiency, while at the same time providing built in timestamps and server-load statistics using a novel id-generation technique.

   

----- INSTALLATION (Linux) -----

# On each server, download the recent jar and create the config directory and ip_address file
   curl -O https://github.com/danielcota/ganesha/blob/master/ganesha_all.jar
   
   mkdir config
   /sbin/ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}' > config/ip_address



# On just the first machine in the cluster, create the cluster map with an initial replicationFactor of 3 (or whatever)
   java -cp ganesha_all.jar cota.ganesha.MapServer create 3
   
# Start Ganesha on the first machine (using heap space here of 6GB - note that 6GB is the minimum)
   java -cp ganesha_all.jar -Xms6G -Xmx6G cota.ganesha.Ganesha

# On the first machine, edit config/whitelisted_ips to contain the IPs of other machines that will be added
# 'cat config/ip_address' on the other servers to see what IP addresses they have
# One per line, for example:
   10.63.16.13
   10.147.3.198
   


# On additional machines, join the cluster using a call similar to:
   java -cp ganesha_all.jar cota.ganesha.MapServer join 10.146.234.76 5

# The last number specifies the time delay in minutes before the new server begins storing data 
# on the cluster (5 minutes in the example above).
# Until that time, the previous set of servers from the map will be used.
# Note that the IP can refer to any server in the cluster (though it will need the whitelisted_ips file)

# Don't forget to start the newly added server
   java -cp ganesha_all.jar -Xms6G -Xmx6G cota.ganesha.Ganesha



# The replication factor for newly added data can be changed using something like the following call:
   java -cp ganesha_all.jar cota.ganesha.MapServer use_rf 2 5
   
   1st arg: replication factor
   2nd arg: time delay in minutes



----- BASIC API USAGE -----

Ganesha provides a direct API for bytes and list manipulation.  Objects will be discussed below.


   // BYTES
   byte[] bytesToStore = new byte[10];
   
   long newID = Ganesha.addBytes( bytesToStore );
   byte[] retrievedBytes = Ganesha.getBytes( newID )



   // LISTS (further functionality found in Ganesha.java)
   long listID = Ganesha.createEmptyList();

   // Just zeroed arrays of bytes for simplicity sake
   long newID1 = Ganesha.addBytes( new byte[1] );
   long newID2 = Ganesha.addBytes( new byte[5] );
   long newID3 = Ganesha.addBytes( new byte[10] );

   Ganesha.appendID( listID, newID1 );
   Ganesha.appendID( listID, newID2 );
   Ganesha.appendID( listID, newID3 );

   Queue ids = Ganesha.getIDs( listID );
   for ( int i = 0; i < ids.size(); i++ )
      {
      long id = (Long) ids.elementAt( i );

      byte[] bytes = Ganesha.getBytes( id );
      System.out.println( i + ": " + bytes.length );
      }



----- OBJECTS AND OBJECT ATTRIBUTES -----

Objects (known as Gobs, short for Ganesha Objects) can be accessed in two ways:
   
   1 - explicitly by their long id
   2 - using a name based key (described in then next section)
      
All objects contain attributes (which allow specific pieces of data to be accessed from within an object)

The examples/TestGob.java file shows you the basic way of extending the Gob class.  Pay close attention to how the attributes and GOB_TYPE are defined.  They MUST be defined as indicated to allow error detection safeguards to function correctly.

Once defined, objects can be used as follows (further API functionality found in Gob.java):   

   // Create the objects
   TestGob sun = new TestGob( "sun" );
   TestGob cloud = new TestGob( "cloudddd" );
   TestGob sky = new TestGob( "sky" );
   
   // correct the name
   cloud.putString( TestGob.name,"cloud" ); 

   // test increment
   sun.increment( TestGob.numberSeen );
   sky.increment( TestGob.numberSeen );

   for( int i = 0; i < 100; i++ )
      cloud.increment( TestGob.numberSeen );
   
   // test lists
   sky.appendID( TestGob.parts, sun.id );
   sky.appendID( TestGob.parts, cloud.id );


   // Retrieve the sky by id
   TestGob sky2 = new TestGob( sky.id );
   sky2.print();
   
   
Test via TestGob class
   java -cp ganesha_all.jar cota.ganeshatest.TestGob store
   
   java -cp ganesha_all.jar cota.ganeshatest.TestGob print one_of_the_ids
   
   
   
----- OBJECTS ACCESSED BY A NAME-BASED KEY -----

ALL objects are stored by long ids, but it is sometimes necessary to store them by names as well. 
   
Named objects are stored within a Workspace/Table/Name hierarchy. The workspaces allow the same Gob classes to be used with different applications.  For example you might have Friendland/User/Daniel object and ChatWorld/User/Daniel object. These two "keys" refer to completely different objects contained within two different workspaces.
   
examples/TestGob2.java shows you what you need to get name based object references to work correctly. 

   
Name-based objects can be used as follows:   

   // Store the objects by name
   TestGob2 sun = new TestGob2( "galaxy", "sun", "orangeish", 3300000000000L );
   TestGob2 otherSun = new TestGob2( "other_galaxy", "sun", "red", 10000000000L );

   // Modify
   sun.putString( color, "orange" );
   otherSun.putLong( weight, 99999999999999L );

   
   // Retrieve by name and print
   TestGob2 sun2 = new TestGob2( "galaxy", "sun" );
   TestGob2 otherSun2 = new TestGob2( "other_galaxy", "sun" );

   sun2.print();
   otherSun2.print();
   

Test via TestGob2 class
   java -cp ganesha_all.jar cota.ganeshatest.TestGob2 store

   java -cp ganesha_all.jar cota.ganeshatest.TestGob2 print

   

A note about name based objects: anytime you need to reference an object that refers to something in the real world (like a user, a device id, a country, etc), you'll use the name-based reference to store/retrieve the objects.  Objects created within Ganesha that have no real-world analogy (like comments, uploaded photos, news feed items, etc) can simply be accessed by their long id (as there is no way to refer to them by name...they have no 'name', just an id).



----- RUNNING ON AMAZON EC2 CLOUD -----

   Check out examples/ec2_cloud.txt to see how to create 3 node cluster using Amazon EC2.
