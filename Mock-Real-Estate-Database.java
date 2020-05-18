/**
 * Maia Prince
 * November 5, 2019
 * Final project for Database Systems. Connects with a locally hosted mySQL database and executes some pre-determined queries.
**/


import java.sql.*;
import java.util.*;

public class PrincePart3 {
	//Database connection variables for your convenience
	private static String url = "jdbc:mysql://localhost/prince_realtors";
	private static String username = "mv";
	private static String password = "h";
	private static Connection connection;

	public static void doMenu() {
		System.out.println("Select query (0 to exit):");
		System.out.println(
				"1 - Show the realtor id and realtor name for all realtors that are working with only buying clients.");
		System.out.println(
				"2 - For a particular land buying client, show all properties that match the client’s interests.");
		System.out.println(
				"6 - Show the realtor id and name for any realtor involved in more than three transactions as the buying realtor.");
		System.out.println(
				"7 - Show the property id for the property that gained the most value (selling price exceeded listing price).");
		System.out.println(
				"10 - Show the realtor that made the most money as a buying realtor involved in a transaction.");

		String sel = "-1";
		Scanner sc = new Scanner(System.in);
		sel = sc.next();

		while (!sel.equals("0")) {

			if (sel.equals("1")) {
				doQuery1();
			} else if (sel.equals("2")) {
				// get user input
				System.out.println("Enter land buying client ID:");
				String clientId = sc.next();
				doQuery2(clientId);
			} else if (sel.equals("6")) {
				doQuery6();
			} else if (sel.equals("7")) {
				doQuery7();
			} else if (sel.equals("10")) {
				doQuery10();
			} else if (!sel.equals("0")) {
				System.out.println("Invalid selection.");
			}
			sel = sc.next();
		}
		sc.close();
	}

	public static void doQuery1() {
		PreparedStatement stmt = null;
		ResultSet result = null;

		// build query
		String query = "select r.rid, r.rname " + 
                        "from realtor r " +
                            "where r.rid not in " +
                    "(select c.rid " +
                        "from client c " +
                            "where c.cid in " +
                               "(select sc.cid " +
                                    "from sellclient sc)) " +
			                	    "and r.rid in " +
                               "(select c.rid " +
                                   "from client c)";
		try {
			// execute query
			stmt = connection.prepareStatement(query);
			result = stmt.executeQuery();

			// print results
			System.out.format("%8s%16s\n", "id", "name");
			while (result.next()) {
				System.out.format("%8s%16s\n", result.getString("r.rid"), result.getString("r.rname"));
			}
		} catch (SQLException e) {
			System.out.println("Exception: " + e.getMessage());
		}
	}

	public static void doQuery2(String clientId) {
		PreparedStatement stmt = null;
		ResultSet result = null;

		// build query
		String query = "select listing.lid, listing.price, listing.address, landlisting.acreage " +
                      "from landlisting, listing, landbuyclient t " +
                          "where landlisting.lid = listing.lid " +
                          "and t.cid = " + clientId + 
                         " and acreage between minsize and maxsize";
        
		try {
			// execute query
			stmt = connection.prepareStatement(query);
			result = stmt.executeQuery();

			// read results
			System.out.format("%8s%16s%24s%16s\n", "id", "price", "address", "acreage");
			while (result.next()) {
				System.out.format("%8s%16s%24s%16s\n", result.getString("listing.lid"),
						result.getString("listing.price"), result.getString("listing.address"),
						result.getString("landlisting.acreage"));
			}
		} catch (SQLException e) {
			System.out.println("SQL Exception: " + e.getMessage());
		}
	}

	public static void doQuery6() {
		PreparedStatement stmt = null;
		ResultSet result = null;

		// build query
		String query = "select bc.rid as buyrid, r.rname, t.listing " +
				                "from transaction t join client bc join realtor r " + 
                            "where t.buycid = bc.cid and bc.rid = r.rid " +
				           "group by r.rid having count(*) > 3";
		try {
			// execute query
			stmt = connection.prepareStatement(query);
			result = stmt.executeQuery();

			// print results
			System.out.format("%8s%16s%16s\n", "id", "name", "listing");
			while (result.next()) {
				System.out.format("%8s%16s%16s\n", result.getString("buyrid"), result.getString("r.rname"),
						result.getString("t.listing"));
			}
		} catch (SQLException e) {
			System.out.println("SQL Exception: " + e.getMessage());
		}
	}

	public static void doQuery7() {
		PreparedStatement stmt = null;
		ResultSet result = null;

		// build query
		String query = "select listing.lid from transaction join listing " + 
                        "where transaction.listing = listing.lid " +
				                "and (sellprice - price) >= all " + 
                            "(select (sellprice - price) as pricedif " +
				                         "from transaction join listing " + 
                                      "where transaction.listing = listing.lid)";
		try {
			// execute query
			stmt = connection.prepareStatement(query);
			result = stmt.executeQuery();

			// print results
			System.out.format("%8s\n", "id");
			while (result.next()) {
				System.out.format("%8s\n", result.getString("listing.lid"));
			}
		} catch (SQLException e) {
			System.out.println("SQL Exception: " + e.getMessage());
		}
	}

	public static void doQuery10() {
		PreparedStatement stmt = null;
		ResultSet result = null;

		// build query
		String query = "select c1.rid as buyrid, r.rname, sum(sellprice*0.03) as earnings " +
				                "from client c1 join listing l1 join realtor r join transaction t1 " +
				                    "on l1.lid = t1.listing and c1.cid = t1.buycid and c1.rid = r.rid " +
				                "group by c1.rid having sum(sellprice*0.03) >= all " + 
                            "(select sum(sellprice*0.03) as earnings " +
				                        "from client c join listing l join transaction t" + 
                                      " on l.lid = t.listing and c.cid = t.buycid " +
				                        "group by c.rid)";
		try {
			// execute query
			stmt = connection.prepareStatement(query);
			result = stmt.executeQuery();

			// print results
			System.out.format("%8s%16s%16s\n", "id", "name", "earnings");
			while (result.next()) {
				System.out.format("%8s%16s%16s\n", result.getString("buyrid"), result.getString("r.rname"),
						result.getString("earnings"));
			}
		} catch (SQLException e) {
			System.out.println("SQL Exception: " + e.getMessage());
		}
	}

	public static void main(String[] args) {
		// Connect to driver
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (Exception e) {
			System.out.println("Can't load driver");
		}

		// Connect to database
		try {
			connection = DriverManager.getConnection(url, username, password);
		} catch (SQLException e) {
			System.out.println("SQL Exception: " + e.getMessage());
		}
		doMenu();
	}
}

