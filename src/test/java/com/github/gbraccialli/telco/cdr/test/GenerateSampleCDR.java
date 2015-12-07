package com.github.gbraccialli.telco.cdr.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;

public class GenerateSampleCDR {


	private static Random rnd = new Random();
	private static SimpleDateFormat cdrDataFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
	private static int numberOfRowsPerInteration = 50;
	private static long initialTimestamp = 0;
	//private static long sleepInterval = 1*60*1000;
	private static long sleepInterval = 60000;
	private static long milisBetweenRows = sleepInterval / numberOfRowsPerInteration;
	private static String targetFolder = "/root/telco-cdr-monitoring/data";

	final static String[] cellIds = new String[]{ 
		"cell A",
		"cell B",
		"cell C",
		"cell D",
		"cell E",
		"cell F",
		"cell G",
		"cell H",
		"cell I",
		"cell J"
	};

	final static String[] simCards = new String[]{ 
		"SIM-00001",
		"SIM-00002",
		"SIM-00003",
		"SIM-00004",
		"SIM-00005",
		"SIM-00006",
		"SIM-00007",
		"SIM-00008",
		"SIM-00009",
		"SIM-00010"
	};

	final static String[] phoneNumbers = new String[]{ 
		"PHONE-00001",
		"PHONE-00002",
		"PHONE-00003",
		"PHONE-00004",
		"PHONE-00005",
		"PHONE-00006",
		"PHONE-00007",
		"PHONE-00008",
		"PHONE-00009",
		"PHONE-00010"
	};

	final static String[] dropReasons = new String[]{ 
		"REASON 1",
		"REASON 2",
		"REASON 3",
		"REASON 4",
		"REASON 5",
		"REASON 6",
		"REASON 7",
		"REASON 8",
		"REASON 9",
		"REASON 10"
	};


	public static void main(String[] args) throws Exception{

		SimpleDateFormat dateFormatWithoutTime = new SimpleDateFormat("dd/MM/yyyy");

		rnd.setSeed(System.currentTimeMillis());

		//Calendar cal = Calendar.getInstance();
		//starts at midnight current day
		//cal.setTimeInMillis(System.currentTimeMillis());
		//initialTimestamp = cdrDataFormat.parse(dateFormatWithoutTime.format(cal.getTime()) + " 00:00:00").getTime();
		//cal.setTimeInMillis(initialTimestamp);

		//starts at 1 hour ago
		initialTimestamp = System.currentTimeMillis() - 30*60*1000;

		generateCDRdata();

	}

	public static void generateCDRdata() throws Exception{

		//layout
		//chargingid,servedimsi,numero_de_linea,recordopeningtime,duration,cgi,rattype
		//session number,sim card number,phone number,opening time,duration,cell,network type


		long lastTimestamp = initialTimestamp;
		BufferedWriter writer = null;
		try {
			while (true){

				long currentTimestamp = lastTimestamp;
				//create a temporary file
				String fileName = "cdr_genarated_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime()) + ".csv";
				File cdrFile = new File("/tmp/" + fileName);
				writer = new BufferedWriter(new FileWriter(cdrFile));

				// This will output the full path where the file will be written to...
				System.out.println("writing file:" + cdrFile.getCanonicalPath());

				long rows = 0;
				rows = numberOfRowsPerInteration + rnd.nextInt((int)(numberOfRowsPerInteration*0.5));
				System.out.println("rnd=" + rows);
				for (long i =0; i < rows; i++){
					writer.write(randomRow(currentTimestamp));
					currentTimestamp += milisBetweenRows;
				}
				writer.flush();

				rows = rnd.nextInt((int)(numberOfRowsPerInteration*0.6))+1;
				System.out.println("drop=" + rows);
				for (long i =0; i < rows; i++){
					writer.write(dropoffPair(currentTimestamp,i));
					currentTimestamp += milisBetweenRows;
				}

				rows = rnd.nextInt((int)(numberOfRowsPerInteration*0.6))+1;
				System.out.println("change=" + rows);
				for (long i =0; i < rows; i++){
					writer.write(networkChangeRowsPair(currentTimestamp));
					currentTimestamp += milisBetweenRows;
				}
				writer.close();
				cdrFile.renameTo(new File(targetFolder + "/" + fileName));
				System.out.println("---sleep---");
				lastTimestamp += sleepInterval;
				Thread.sleep(sleepInterval);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				// Close the writer regardless of what happens...
				writer.close();
			} catch (Exception e) {
			}
		}
	}		

	public static String networkChangeRowsPair(long timestmap){

		String sessionnumber = "change-" + UUID.randomUUID().toString();
		String simcard = simCards[rnd.nextInt(10)];
		String phonenumber = phoneNumbers[rnd.nextInt(10)];
		String openingtime = cdrDataFormat.format(timestmap);
		String duration = String.valueOf(rnd.nextInt(300));
		int cellid = rnd.nextInt(10);
		int cellid2 = (cellid + rnd.nextInt(10)) % 10;
		String cell = cellIds[cellid];
		String cell2 = cellIds[cellid2];
		String networktype = rnd.nextInt(2) == 0 ? "1" : "2";
		String networktype2 = networktype.equals("1") ? "2" : "1";
		String dropReason = dropReasons[rnd.nextInt(10)];

		return sessionnumber + "," + simcard + "," + phonenumber + "," + openingtime + "," + duration + "," + cell + "," + networktype + "," + dropReason + "\n" +
		sessionnumber + "," + simcard + "," + phonenumber + "," + openingtime + "," + duration + "," + cell2 + "," + networktype2 + "," + dropReason + "\n";

	}

	public static String randomRow(long timestmap){

		String sessionnumber = "rnd-"+ UUID.randomUUID().toString();
		String simcard = simCards[rnd.nextInt(10)];
		String phonenumber = phoneNumbers[rnd.nextInt(10)];
		String openingtime = cdrDataFormat.format(timestmap);
		String duration = String.valueOf(rnd.nextInt(300)) + 61;
		int cellid = rnd.nextInt(10);
		String cell = cellIds[cellid];
		String networktype = rnd.nextInt(2) == 0 ? "1" : "2";
		String dropReason = dropReasons[rnd.nextInt(10)];


		return sessionnumber + "," + simcard + "," + phonenumber + "," + openingtime + "," + duration + "," + cell + "," + networktype + "," + dropReason + "\n";

	}

	public static String dropoffPair(long timestamp, long sequence){

		String sessionnumber = "drop-" + UUID.randomUUID().toString();
		String simcard = simCards[rnd.nextInt(10)] + "-" + sequence;
		String phonenumber = phoneNumbers[rnd.nextInt(10)] + "-" + sequence;
		String openingtime = cdrDataFormat.format(timestamp);
		long duration = rnd.nextInt(10);
		String openingtime2 = cdrDataFormat.format(timestamp + duration*1000 + rnd.nextInt(5)*1000);
		long duration2 = rnd.nextInt(300);
		int cellid = rnd.nextInt(10);
		String cell = cellIds[cellid];
		String networktype = rnd.nextInt(2) == 0 ? "1" : "2";
		String dropReason = dropReasons[rnd.nextInt(10)];


		return sessionnumber + "," + simcard + "," + phonenumber + "," + openingtime + "," + duration + "," + cell + "," + networktype + "," + dropReason + "\n" +
		sessionnumber + "," + simcard + "," + phonenumber + "," + openingtime2 + "," + duration2 + "," + cell + "," + networktype + "," + dropReason + "\n";

	}



}
