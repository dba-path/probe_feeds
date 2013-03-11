/**
 * Copyright (c) 2012, Regents of the University of California
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 **/

/**
* Probe Reader ( probeReader.java)
*
* written 10/10/2012 by WJS (wjs@berkeley.edu)
*
* This program reads data from a socket connection and stores it in a database.
*
**/

package edu.berkeley.path.ora_navteq_probe_raw;

import java.util.ArrayList;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.io.IOException;
import java.io.DataInputStream;
import java.util.*;
import java.sql.*;


public class probeReader {

  private static Socket lsnrSocket = null ;
  private static DataInputStream probeData = null ;
  private static InetSocketAddress navteq ;
  private static Connection oraDB = null ;
  private static int longestString = 0 ;
  private static Boolean setDebug = false ;
  private static int procRes = 0 ;
  private static int count = 0 ;
  private static int waitStates = 0 ;
  private static Connection MonDBConn = null ;
  private static int pointsRecorded = 0 ;
  private static int dupeCount = 0 ;
  private static long totProbeReads = 0 ;
  //private static PreparedStatement  oraProc ;


  private static CallableStatement  oraProc ;

  private static  String DBConnectString = "jdbc:oracle:thin:";
  private static  String DBIPAddress = "ccoradb.path.berkeley.edu";
  private static  String DBPortNumber = "1521";
  private static  String DBInstance = "via" ;
  private static  String DBUserName = null ;
  private static  String DBPassword = null ;
  
  public static final int maj__ver = 0 ;
  public static final int min__ver = 9 ;
  public static final int deb__ver = 0 ;

  /**
  *
  *This is the defintion of the record that is read from the socket.
  *The socket is binary data.  See the NAVTEQ documentation from
  *the precise meaning of each data element.  Some of the element
  *names are quite possibly assumed.
  *
  **/

  public static class NavteqRecord{

	static int	rmob_name ;
	static int	rmob_version ;
	static int	link_set_version ;
	static int	server_time ; 
	static int	measure_dt ;
	static int	longitude ;
	static int	latitude ;
	static int	heading ;
	static int	speed ;
	static int	mapped_flag ;
	static long	link_id ;
	static int	link_offset ;
	static int	heading_offset ;
	static int	lanes_to_from ;
	static int	functional_class ;
	static int	percent_from_ref ;
	static int	time_zone_offset ;
	static long	ref_loc ;
	static long	nonref_loc ;
	static int	accuracy_radius ;
	static int 	hdop ;
	static int 	satellites ;
	static int 	probe_data_prov_desc_type ;
	static String	probe_data_provider_name ;
	static int	probe_id_type ;
	static String	probe_id_size ;
	static int	tmc_code_type ;
	static String	tmc_codes ;
	static String	tables_ids ;
  }

/**
*
* The data definition for the elements needed to connect tot he monitoring system
*
**/

public static class monitorConnData{

	static String uName ;
	static String uPass ;
	static String connStr ;
	static String port ;
	static String instance ;
	static String server ;
}


  /** Read a string of size from stream. */
  private static String readString( int size ) throws IOException {

     StringBuilder sb = new StringBuilder();

	if(size > longestString) longestString += size ;

        while( 0 < size ) {
            size--;
            sb.append( (char) probeData.readUnsignedByte() );
        }

        return sb.toString();
   }


/**
*
* Connect to the Oracle DB.  If this fails, we are done.
*
**/
private static Connection OraConnect(String uname,String upass) throws SQLException {

	try{
		DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
		return DriverManager.getConnection(DBConnectString + uname + "/" + upass + "@" + DBIPAddress + ":" + DBPortNumber + ":" + DBInstance);
	} catch (SQLException e){
		writeMonData("Could not connect to the main database.  This is fatal. Error = " +  e.getMessage() + "Exiting.");
		System.exit(2);
		return null;
	}          
}

/**
*
* Smae function as above, but failure is not fatal.
*
**/
private static Connection MonitorConnect() {


	try{
		DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());

		return DriverManager.getConnection(monitorConnData.connStr + 
						   monitorConnData.uName + 
						   "/" + 
						   monitorConnData.uPass + 
						   "@" + 
						   monitorConnData.server + 
						   ":" + 
						   monitorConnData.port + 
						   ":" + monitorConnData.instance);
	} catch (SQLException e){
		writeMonData("Could not connect to the monitoring data.  Error = "+ e.getMessage());
		return null;
	}          
}


/**
*
* Call the stored procedure on the database to write the data out to the
* monitoring system.
*
**/
private static void writeMonData(String note){

  try{

  System.out.println(note);
  CallableStatement MonProc = MonDBConn.prepareCall("{ call SP_MONITORSAVE(?,?,?,?,?,?,?,?) }");
  MonProc.setString(1,"255.255.255.255");
  MonProc.setInt(2,0);
  MonProc.setString(3,"probeReader");
  MonProc.setString(4,"0.90");
  MonProc.setString(5,"Oracle");
  MonProc.setInt(6,0);
  MonProc.setString(7,"Via.probes_raw");
  MonProc.setString(8,note);
  MonProc.execute();
  MonProc.close();
} catch(SQLException e){writeMonData("Error " + e.getMessage() + "Occured while attemtping to write to the monitor!" );}
}


/**
*
* Call the stored procedure on the database to write the data out to the
* monitoring system.
*
**/
private static void writeMonHealthy(boolean healthy){

  try{

  System.out.println("Is Healhty = " + healthy);
  CallableStatement MonProc = MonDBConn.prepareCall("{ call SP_MONITOR_HEARTBEAT(?,?,?,?,?,?,?,?) }");
  MonProc.setString(1,"255.255.255.255");
  MonProc.setInt(2,0);
  MonProc.setString(3,"probeReader");
  MonProc.setString(4,"0.90");
  MonProc.setString(5,"Oracle");
  MonProc.setInt(6,0);
  MonProc.setString(7,"Via.probes_raw");
  if( healthy ){
  MonProc.setString(8,"Y");
  }else{
    MonProc.setString(8,"N");
  }
  MonProc.execute();
  MonProc.close();
} catch(SQLException e){writeMonData("Error " + e.getMessage() + "Occured while attemtping to write to the monitor!" );}
}





public static void printProbeRaw(){

   try{

      System.out.println("");
      System.out.println(probeData.readInt());	    
      System.out.println(probeData.readInt());	
      System.out.println(probeData.readShort());
      System.out.println(probeData.readInt());	
      System.out.println(probeData.readInt());	
      System.out.println("Latitude = " + probeData.readInt());	
      System.out.println("Longitude = " + probeData.readInt());	
      System.out.println(probeData.readInt());	
      System.out.println(probeData.readInt());	
      System.out.println(probeData.readByte());	
      System.out.println(probeData.readLong());	
      System.out.println(probeData.readInt());	
      System.out.println(probeData.readInt());	
      System.out.println(probeData.readByte());	
      System.out.println(probeData.readByte());	
      System.out.println(probeData.readByte());	
      System.out.println(probeData.readByte());	
      System.out.println(probeData.readLong());	
      System.out.println(probeData.readLong());	
      System.out.println(probeData.readInt());	
      System.out.println(probeData.readInt());	
      System.out.println(probeData.readInt());	
      System.out.println(probeData.readByte()); //probe_data_provid
      System.out.println(readString(probeData.readUnsignedByte()));
      System.out.println(probeData.readByte()); //PROBE_ID TYPE
      System.out.println(readString(probeData.readUnsignedByte()));
      System.out.println(probeData.readByte()); //TMC_CODES TYPE
      System.out.println(readString(probeData.readUnsignedByte()));
      System.out.println(readString(probeData.readUnsignedByte()));
   }catch(IOException e){}
}


public static boolean dataAvailable(){

  try{


    // this parameter should be fetched from the database...
    if( probeData.available() > 250 ){

	return true;

     }else{

       return false ;
      }

  }catch(IOException e){

      writeMonData("Error on Data Availability Count = " + (e.getMessage()) );
      return false ;
  }

 }


  private static void getMonitorDBConnValues(Connection dbConn){

    ResultSet rs = null ;

    try{

    CallableStatement getMon = dbConn.prepareCall("{ call VIA.SP_GET_MONITOR_CONN(?,?,?,?,?,?) }");
        
    getMon.registerOutParameter(1,java.sql.Types.VARCHAR);
    getMon.registerOutParameter(2,java.sql.Types.VARCHAR);
    getMon.registerOutParameter(3,java.sql.Types.VARCHAR);
    getMon.registerOutParameter(4,java.sql.Types.VARCHAR);
    getMon.registerOutParameter(5,java.sql.Types.VARCHAR);
    getMon.registerOutParameter(6,java.sql.Types.VARCHAR);

    getMon.execute();

    monitorConnData.uName    = getMon.getString(1);
    monitorConnData.uPass    = getMon.getString(2);
    monitorConnData.connStr  = getMon.getString(3) ;
    monitorConnData.port     = getMon.getString(4);
    monitorConnData.instance = getMon.getString(5);
    monitorConnData.server   = getMon.getString(6);
    
    getMon.close();
	
    }catch(SQLException e){
	writeMonData("Error getting Monitor connect values "+ e);
    }

  }



  public static boolean writeProbeData() {    

    if( dataAvailable() && getProbeSocketData()){

      try{	

	CallableStatement oraProc = oraDB.prepareCall("{ call VIA.SP_SAVE_PROBES_RAW(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) }");

//        printProbeRaw();
	oraProc.setInt(1,1);
	oraProc.setLong(2,NavteqRecord.rmob_name) ;
	oraProc.setInt(3,NavteqRecord.rmob_version) ;
	oraProc.setInt(4,NavteqRecord.link_set_version) ;
	oraProc.setInt(5,NavteqRecord.server_time) ; 
	oraProc.setInt(6,NavteqRecord.measure_dt) ;
	oraProc.setDouble(7,( NavteqRecord.latitude)) ;
	oraProc.setDouble(8,(NavteqRecord.longitude)) ;
	oraProc.setInt(9,NavteqRecord.heading) ;
	oraProc.setInt(10,NavteqRecord.speed) ;
	oraProc.setInt(11,NavteqRecord.mapped_flag) ;
	oraProc.setLong(12,NavteqRecord.link_id) ;
	oraProc.setInt(13,NavteqRecord.link_offset) ;
	oraProc.setInt(14,NavteqRecord.heading_offset) ;
	oraProc.setInt(15,NavteqRecord.lanes_to_from) ;
	oraProc.setInt(16,NavteqRecord.functional_class) ;
	oraProc.setInt(17,NavteqRecord.percent_from_ref) ;
	oraProc.setInt(18,NavteqRecord.time_zone_offset) ;
	oraProc.setLong(19,NavteqRecord.ref_loc) ;
	oraProc.setLong(20,NavteqRecord.nonref_loc) ;
	oraProc.setInt(21,NavteqRecord.accuracy_radius) ;
	oraProc.setInt(22,NavteqRecord.hdop) ;
	oraProc.setInt(23,NavteqRecord.satellites) ;
	oraProc.setInt(24,NavteqRecord.probe_data_prov_desc_type);
	oraProc.setString(25,NavteqRecord.probe_data_provider_name);
	oraProc.setInt(26,NavteqRecord.probe_id_type) ;
	oraProc.setString(27,NavteqRecord.probe_id_size) ;
	oraProc.setInt(28,NavteqRecord.tmc_code_type) ;
	oraProc.setString(29,NavteqRecord.tmc_codes) ;
	oraProc.setString(30,NavteqRecord.tables_ids) ;

	// Setup the retun code
	oraProc.registerOutParameter(31,java.sql.Types.INTEGER);

	//System.out.println("Finished moving the fields");	

	//oraProc.executeUpdate();

	oraProc.execute();
      
	dupeCount+= oraProc.getInt(30) / 2 ;

	oraProc.close();

        pointsRecorded++ ;

      } catch(SQLException e){

	  writeMonData("Error on OraWrite Message = " + (e.getMessage()) + "Stack Trace Follows...");
	  e.printStackTrace();
	  return false ;

	 }

	return true ;

    }

    return false ;

}


    public static void ProbeSocketSetup(){

      try{

	navteq 		= new InetSocketAddress("probefeed.traffic.com",58809);
        lsnrSocket 	= new Socket();
	lsnrSocket.setSoTimeout(60);
	lsnrSocket.setKeepAlive(true);
	lsnrSocket.setSoLinger(false,0);
	lsnrSocket.setReuseAddress( true );
	lsnrSocket.setReceiveBufferSize(1024*1024*1024 / 2);
	
      } catch(Exception e){writeMonData("Error on socket Setup "+e.getMessage());}

    }


    public static void probeConnect(){

	try{

	  ProbeSocketSetup() ;

	  lsnrSocket.connect(navteq,5000);

	  //System.out.println("Connection = "+ lsnrSocket.isConnected() );

	  probeData = new DataInputStream(lsnrSocket.getInputStream());

	} catch(Exception e){writeMonData("Error on socket connect "+e.getMessage());}

    }


public static void probeConnectorClose(){

	try{
		probeData.close();
		probeData = null ;
		lsnrSocket.close();
		lsnrSocket = null ;

	 } catch(Exception e){writeMonData("Error on socket close "+e.getMessage());}
    
}


    public static void cycleProbeSocket(){

	try{
	    writeMonData("Attempting to close the socket...");	    
	    probeConnectorClose();
	    writeMonData("Socket closed...");
	    doWait(1000);
	    System.out.println("Attempting to open the socket...");
	    probeConnect();

	}catch(Exception e){
          //System.out.println("Error on socket cycle "+e.getMessage());
	  writeMonData("Exception "+e.getMessage()+" occured Recycling the socket.  This is fatal.  Exiting.");
	  System.exit(2);
	}
    }



    public static boolean getProbeSocketData(){

      try{

	 
	      NavteqRecord.rmob_name		= probeData.readInt();		// rmob_name
	      NavteqRecord.rmob_version		= probeData.readInt();		// rmob_version
	      NavteqRecord.link_set_version	= probeData.readShort();	// link_set_version
	      NavteqRecord.server_time		= probeData.readInt();		// server time           
	      NavteqRecord.measure_dt		= probeData.readInt();		// sample date
	      NavteqRecord.latitude		= probeData.readInt();		// latitude
	      NavteqRecord.longitude		= probeData.readInt();		// logintude
	      NavteqRecord.heading		= probeData.readInt();		// heading           
	      NavteqRecord.speed		= probeData.readInt();		// Speed
	      NavteqRecord.mapped_flag		= probeData.readByte();		// mapped_flag
	      NavteqRecord.link_id		= probeData.readLong();		// link_id 
	      NavteqRecord.link_offset		= probeData.readInt();		// link_offset     
	      NavteqRecord.heading_offset	= probeData.readInt();		// heading_offset        
	      NavteqRecord.lanes_to_from	= probeData.readByte();		// lanes_to_from       
	      NavteqRecord.functional_class	= probeData.readByte();		// funcional_class          
	      NavteqRecord.percent_from_ref	= probeData.readByte();		// percent_from_ref          
	      NavteqRecord.time_zone_offset	= probeData.readByte();		// time_zone_offset          
	      NavteqRecord.ref_loc		= probeData.readLong();		// ref_loc 
	      NavteqRecord.nonref_loc		= probeData.readLong();		// nonref_loc    
	      NavteqRecord.accuracy_radius	= probeData.readInt();		// accuracy_radius         
	      NavteqRecord.hdop			= probeData.readInt();		// hdop
	      NavteqRecord.satellites		= probeData.readInt();		// satellites

	      NavteqRecord.probe_data_prov_desc_type 	= probeData.readByte(); //probe_data_provider_desc type
	      NavteqRecord.probe_data_provider_name	= readString(probeData.readUnsignedByte());

	      NavteqRecord.probe_id_type	= probeData.readByte(); //PROBE_ID TYPE
	      NavteqRecord.probe_id_size	= readString(probeData.readUnsignedByte());
	      NavteqRecord.tmc_code_type	= probeData.readByte(); //TMC_CODES TYPE
	      NavteqRecord.tmc_codes		= readString(probeData.readUnsignedByte());
	      NavteqRecord.tables_ids		= readString(probeData.readUnsignedByte());
	      
	    }catch(Exception e){
		writeMonData("Error While Moving socket Data "+e.getMessage());
		// this will triger on a time out for the read so we simply return false;
		return false;
	    }
      totProbeReads++ ;
      return true ;
    }


  public static void doWait(int period){


    try{

	java.util.concurrent.TimeUnit.MILLISECONDS.sleep((long) period );

    }catch(InterruptedException e){
	writeMonData("Error executing a wait! Halting!" + e.getMessage()); 
	System.exit(1);
    }

  }



  public static void main(String[] args){


	DBConnectString =	System.getProperty("dbconnstr","jdbc:oracle:thin:");
	DBIPAddress =		System.getProperty("dbsdd","ccoradb.path.berkeley.edu");
	DBPortNumber =		System.getProperty("dbport","1521");
	DBInstance =		System.getProperty("dbinst","via") ;
	DBUserName =		System.getProperty("dbuser",null) ;
	DBPassword =		System.getProperty("dbpass",null) ;

 	try{oraDB = OraConnect(DBUserName,DBPassword);}catch(SQLException e){
 	System.out.println(e.getMessage());}


	try{

	oraProc = oraDB.prepareCall("{ call VIA.SP_SAVE_PROBES_RAW(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) }");

	//oraProc = oraDB.prepareStatement("insert into via.probes_raw values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

	}
	catch (SQLException e){
		writeMonData(e.getMessage());
	}

        getMonitorDBConnValues(oraDB);
        MonDBConn = MonitorConnect();
        writeMonData("Starting up the new probe feed");


	try{
	  ProbeSocketSetup();
	} catch (Exception e){}
   
	probeConnect();
    
        writeMonData("Connected to the feed source. Begining the endless loop.");
	writeMonHealthy(true);

        while( true ) {

	      //System.out.print("O");	      

	      if( writeProbeData() ){

		// pointsRecorded++ ;
		waitStates = 0 ;
		if ( pointsRecorded  == 100 ){
		
		  writeMonData("Wrote 100 records records of which "+ dupeCount + " were duplicates. Total Probe Socket Reads this run = " + totProbeReads  );
                  writeMonHealthy(true);
                  pointsRecorded = 0 ;
                  dupeCount = 0 ;

		}

	      }else{

		  if(waitStates > 0) {
			writeMonData("Probe feed is stalled. RW =  "+ pointsRecorded + ", DR = " + dupeCount + " WS = "+ waitStates + " TPR = " + totProbeReads +" Pausing ~ 5 Seconds...");
			pointsRecorded = 0 ;
                        dupeCount = 0 ;
		  }
		  waitStates++ ;
		  doWait(5000);
	      }

	      // too many waitstates discouunect and re-connect.
	      if (waitStates > 12 ){

		  writeMonData("More then 12 wait states. Restarting the connection. Number of probe reads before reset = " + totProbeReads );
		  cycleProbeSocket();
                  totProbeReads = 0 ;
		  waitStates = 0 ;
		  dupeCount = 0 ;
                  writeMonHealthy(false);

                }
       } // end of the main while loop.
  } // end of main

} // end of the class
