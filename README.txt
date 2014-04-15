----- DESCRIPTION -----

   Ganesha is a sleek NoSQL database written in Java.

   It requires no external libraries and provides synchronized binary, object/attribute, and list based storage across a self-healing redundant cluster of server nodes. All required source is provided here.

	
----- FEATURES -----

   Replication of Data - data is replicated automatically across as many servers as you want it to be.

   Auto-Healing Data - any time a piece of data is accessed, any server that doesn't have the most recent copy of the data will be updated.  Also, servers keep logs of failed data updates (for example if another server was temporarily down) so that the update can be sent later on.
   
   Built in Object Support - in addition to strictly storing bytes, a robust object architecture is in place which provides a built in mechanism for storing and retrieving lists, ints, longs, bytes, other objects, and strings from and to objects.

   Checksum/Timestamp Consistency Checks - both checksums and timestamps are used to ensure that all relevant servers have the correct copy of a given piece of data.   
   
   Support for Non-Homogeneous Servers - storage/load is based proportionally on individual server storage capacity and allows clusters to be built of machines with mixed storage capacities.
   
   Lists - built in synchronized support for storing lists of objects/ids as well as inserting sorted objects based on a given developer-defined metric. 
   
   Object Locking/Synchronization- built in object locking and synchronization keeps data consistent and prevents data corruption.
   
   Support for Growable Clusters - live clusters can be grown easily, have no inherent limit in size and require no additional work or data splitting when new nodes are added.
   
   Optional In-Memory-Only Data - useful for demanding tasks such as news feeds.  Unlike normally handled data, no disk usage of any kind is used for in-memory based data. In-memory-only data, like normal data, is self-healing and persistent as long as all servers holding a piece of data are not simultaneously down.
   
   Long Integer IDs for Data - long integer ids are used for simplicity and efficiency, while at the same time providing built in timestamps and server-load statistics using a novel id-generation technique.



----- INSTALLATION (Linux) -----

# On each server, download the recent jar
   sudo yum install git

   git clone https://github.com/danielcota/ganesha
   cd ganesha
   
# Create the config directory and ip_address file   
   mkdir config
   /sbin/ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}' > config/ip_address

   cat config/ip_address



# On the first machine, edit config/whitelisted_ips to contain the IPs of other machines that will be added
# One per line, for example:
   10.63.16.13
   10.147.3.198

# On the first machine in the cluster, create the cluster map with an initial replicationFactor of 3 (or whatever)
   java -cp ganesha_all.jar cota.ganesha.MapServer create 3
   
# Start Ganesha on the first machine (using heap space here of 6GB - note that 6GB is the minimum)
   java -cp ganesha_all.jar -Xms6G -Xmx6G cota.ganesha.Ganesha



# On additional machines, join the cluster using a call similar to:
   java -cp ganesha_all.jar cota.ganesha.MapServer join 10.146.234.76 5

The last number specifies the time delay in minutes before the new server begins storing data on the cluster (5 minutes in the example above).

Until that time, the previous set of servers from the map will be used.

Note that the IP can refer to any server in the cluster (though it will need the whitelisted_ips file)


# Don't forget to start the newly added servers
   java -cp ganesha_all.jar -Xms6G -Xmx6G cota.ganesha.Ganesha



# The replication factor for newly added data can be changed using the following call:
   java -cp ganesha_all.jar cota.ganesha.MapServer use_rf 2 5
   
   1st arg: replication factor
   2nd arg: time delay in minutes



----- BENCHMARKS -----

Ganesha has been used in production since August 2013 on a 5 server cluster by DrawCast, a social network for artists (for iOS and Android).  That cluster currently processes an average of 3000 database API calls per second, or 8 billion calls per month with an average load of .25 for each server (standard deviation of .6).

Here are some benchmarks running on Amazon EC2 using m3.xlarge nodes:

    On a single node cluster:
       1-way-replicated writes: ~7000 writes/s
       1-way-replicated reads: ~31000 reads/s

    On a three node cluster:
       3-way-replicated writes: ~23000 writes/s
       3-way-replicated reads: ~42000 reads/s

    On a five node cluster:
       3-way-replicated writes: ~27000 writes/s
       3-way-replicated reads: ~44000 reads/s

    On a five node cluster:
       5-way-replicated writes: ~15000 writes/s
       5-way-replicated reads: ~42000 reads/s


Keep in mind that all reads/writes are checked with all relevant servers for consistency and correctness (using timestamps/checksums).

You can repeat these tests by running:

   java -cp ganesha_all.jar cota.ganeshatest.Test gen

   java -cp ganesha_all.jar cota.ganeshatest.Test writing

   java -cp ganesha_all.jar cota.ganeshatest.Test reading



----- BASIC API USAGE -----

Ganesha provides a direct API for byte array storage and list manipulation.  Objects will be discussed below.


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
      
All objects contain attributes (which allow specific pieces of data to be accessed from within an object).  These attributes are referred to by name (further details can be found in Gob.java).

Example:   

   // Create the objects
   Gob cloud = new Gob();
   cloud.put( "name", "cloud" );

   Gob sky = new Gob();
   sky.put( "name", "sky" );

   // test increment and put
   sky.increment( "seenCount" );
   cloud.put( "seenCount", 100 );

   // test lists
   sky.appendID( "parts", cloud.id );


   // Retrieve the sky by id
   Gob sky2 = new Gob( sky.id );
   
   
Test via TestGob class
   java -cp ganesha_all.jar cota.ganeshatest.TestGob store
   
   java -cp ganesha_all.jar cota.ganeshatest.TestGob print one_of_the_ids
   
   
   
----- OBJECTS ACCESSED BY A NAME-BASED KEY -----

ALL objects are stored by long ids, but it is sometimes necessary to refer to them by names as well. 
   
Named objects employ a workspace/table/name mechanism to generate a unique key. The workspaces allow the same Gob classes to be used with different applications.  For example you might have Friendland/User/Daniel object and ChatWorld/User/Daniel object. These two "keys" refer to completely different objects contained within two different workspaces.

If needed, you can find the long id corresponding to a named object using the following:
   long id = Translator.translate( workspace, table, name );
   

Example:   

   // Create the objects by name
   Gob sun = new Gob( "galaxy", "stars", "sun" );
   sun.put( "color", "orangeish" );
   sun.put( "weight", 3300000000000L );

   Gob otherSun = new Gob( "other_galaxy", "stars", "sun" );
   otherSun.put( "color", "red" );
   otherSun.put( "weight", 10000000000L );

   // Modifications overwrite as expected
   sun.put( "color", "orange" );
   

   // Retrieve by name and print
   Gob sun2 = new Gob( "galaxy", "stars", "sun" );

   System.out.println( "id: " + sun2.id );
   System.out.println( "name: " + sun2.getString( "name" ) );
   System.out.println( "color: " + sun2.getString( "color" ) );
   System.out.println( "weight: " + sun2.getLong( "weight" ) );


Test via TestGob2 class
   java -cp ganesha_all.jar cota.ganeshatest.TestGob2 store

   java -cp ganesha_all.jar cota.ganeshatest.TestGob2 print

   

---- NESTED OBJECTS AND LISTS -----

Objects and lists can be nested as need to create complex data structures.

Simple example:

   // Store
   Gob sun = new Gob( "universe", "stars", "the sun" );

   Gob earth = new Gob( "universe", "planets", "earth" );
   Gob venus = new Gob( "universe", "planets", "venus" );

   sun.append( "planets", earth );
   sun.append( "planets", venus );

   earth.put( "star", sun );
   venus.put( "star", sun );


   // Retrieve
   Gob earth2 = new Gob( "universe", "planets", "earth" );
   Gob sun2 = earth2.getGob( "star" );

   Queue planets = sun2.getGobs( "planets" );
   for ( int i = 0; i < planets.size(); i++ )
      {
      Gob planet = (Gob) planets.elementAt( i );

      System.out.println( planet.getString( "name" ) + " orbits around " + sun2.getString( "name" ) );
      }


Test via TestGob2 class
   java -cp ganesha_all.jar cota.ganeshatest.TestGob2 planets

   java -cp ganesha_all.jar cota.ganeshatest.TestGob2 orbits



----- RUNNING ON AMAZON EC2 CLOUD -----

   Check out examples/ec2_cloud.txt to see how to prepare a 3 node cluster on Amazon EC2.
