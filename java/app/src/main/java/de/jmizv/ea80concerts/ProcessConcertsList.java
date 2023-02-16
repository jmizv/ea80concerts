package de.jmizv.ea80concerts;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.ibatis.jdbc.ScriptRunner;
import org.postgresql.core.BaseConnection;

/**
 *
 * @author Alex
 */
public class ProcessConcertsList {

    private static final String SCHEMA_FILE = "./../sql/01_schema.sql";

    static final Map<String, String> country = new HashMap<>();
    static String password = "";

    static {
        country.put("CH", "Schweiz");
        country.put("AT", "Österreich");
        country.put("NL", "Niederlande");
    }

    public static void main(String[] args) throws Exception {
        Class.forName("org.postgresql.Driver");
        boolean create = false; // set to true to create initially the schemas
        boolean reset = false; // set to true to drop all tables and recreate the DB schema.
        try (BaseConnection connection = (BaseConnection) DriverManager.getConnection("jdbc:postgresql://localhost:5432/ea80", "postgres", password)) {
            if (create) {
                createSchema(connection);
                insertEntries(connection);
            } else if (reset) {
                try (Statement stmt = connection.createStatement()) {
                    stmt.executeUpdate("DELETE FROM concert");
                    stmt.executeUpdate("DELETE FROM venue");
                    stmt.executeUpdate("DELETE FROM city");
                    stmt.executeUpdate("DELETE FROM band");
                }
                insertEntries(connection);
            }
            printToWikiTable(connection);
            printStatisticsForCity(connection);
            printStatisticsForBand(connection);
        }
    }
    
    static void createSchema(BaseConnection connection) throws FileNotFoundException {
        //Initialize the script runner
        ScriptRunner sr = new ScriptRunner(connection);
        //Creating a reader object
        Reader reader = new BufferedReader(new FileReader(SCHEMA_FILE));
        //Running the script
        sr.setSendFullScript(true);
        sr.setAutoCommit(true);
        sr.runScript(reader);
    }

    static void insertEntries(BaseConnection connection) throws IOException, SQLException {
        try (PreparedStatement stmt = connection.prepareCall("CALL createconcert(?,?,?,?,?,?,?)");
                PreparedStatement getLastInsert = connection.prepareCall("SELECT max(id) FROM concert");
                PreparedStatement updateDateFormat = connection.prepareCall("UPDATE concert SET dateformat=? WHERE id=?")) {
            for (String[] line : getLines()) {
                stmt.clearParameters();
                stmt.setString(1, line[0]);
                stmt.setString(2, line[1]);
                stmt.setString(3, line[2].isEmpty() ? "?" + line[1] : line[2]);
                if (line[3].length() > 0) {
                    Array array = connection.createArrayOf("varchar", line[3].replace("\"", "").split("\\s*,\\s*"));
                    stmt.setArray(4, array);
                } else {
                    stmt.setNull(4, Types.ARRAY, "varchar[]");
                }
                stmt.setBoolean(5, Boolean.parseBoolean(line[4]));
                if (line[5].isEmpty()) {
                    stmt.setNull(6, Types.NUMERIC);
                } else {
                    stmt.setBigDecimal(6, new BigDecimal(line[5]));
                }
                if (line[6].isEmpty()) {
                    stmt.setNull(7, Types.NUMERIC);
                } else {
                    stmt.setBigDecimal(7, new BigDecimal(line[6]));
                }
                try {
                    stmt.executeUpdate();
                } catch (SQLException ex) {
                    ex.printStackTrace(System.err);
                    break;
                }
                if (!line[7].isEmpty()) {
                    getLastInsert.clearParameters();
                    updateDateFormat.clearParameters();
                    ResultSet executeQuery = getLastInsert.executeQuery();
                    executeQuery.next();
                    BigDecimal lastId = executeQuery.getBigDecimal(1);
                    updateDateFormat.setString(1, line[7]);
                    updateDateFormat.setBigDecimal(2, lastId);
                    updateDateFormat.executeUpdate();
                }
            }
        }
    }

    static List<String[]> getLines() throws IOException {
        List<String[]> result = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(
                new FileInputStream("../data/ea80concerts.csv"), StandardCharsets.UTF_8))) {
            String string;
            while ((string = br.readLine()) != null) {
                result.add(string.split("\t", 9));
            }
        }
        return result;
    }

    static Map<Long, String> getBandCache(BaseConnection conn) throws SQLException {
        Map<Long, String> result = new HashMap<>();
        try (var stmt = conn.prepareStatement("SELECT id, name FROM BAND")) {
            ResultSet executeQuery = stmt.executeQuery();
            while (executeQuery.next()) {
                long aInt = executeQuery.getInt(1);
                String name = executeQuery.getString(2);
                result.put(aInt, name);
            }
        }
        return result;
    }

    private static void printStatisticsForCity(BaseConnection conn) throws SQLException, IOException {
        printStatistic(conn,
                "SELECT count(*), city.name FROM concert c,venue v,city WHERE c.venueid=v.id and v.cityid=city.id GROUP BY city.name ORDER BY 1 DESC,2 DESC",
                "Ort"
        );
    }

    private static void printStatisticsForBand(BaseConnection conn) throws SQLException, IOException {
        printStatistic(conn,
                "SELECT count(*), b.name FROM (SELECT unnest(otherbands) \"id\" FROM concert)c,band b WHERE c.id=b.id GROUP BY b.id ORDER BY 1 DESC,2 DESC",
                "Band"
        );
    }

    private static void printStatistic(BaseConnection conn, String sql, String header) throws SQLException, IOException {
        try (var stmt = conn.prepareStatement(sql)) {
            ResultSet executeQuery = stmt.executeQuery();
            StringBuilder sb = new StringBuilder();
            sb.append("| # | ").append(header).append(" | Anzahl |\n");
            sb.append("|---|---|---|\n");
            int counter = 0;
            while (executeQuery.next()) {
                sb.append("| ").append(++counter);
                sb.append(" | ").append(executeQuery.getString(2));
                sb.append(" | ").append(executeQuery.getInt(1)).append(" |\n");
                if (counter >= 20) {
                    break;
                }
            }

            sb.append("\n");
            output(sb, "../data/statistics_" + header + ".md");
        }
    }

    static void printToWikiTable(BaseConnection conn) throws SQLException, IOException {
        Map<Long, String> bandCache = getBandCache(conn);
        try (var stmt = conn.prepareStatement(
                "SELECT to_char(dateofconcert,dateformat),city.name,venue.name,otherbands,"
                + "case when canceled='0' then '' else 'ja' end "
                + "FROM concert,city,venue "
                + "WHERE concert.venueid=venue.id AND venue.cityid=city.id "
                + "ORDER BY dateofconcert DESC")) {
            ResultSet executeQuery = stmt
                    .executeQuery();
            StringBuilder sb = new StringBuilder();

            sb.append("| Datum | Ort | Venue | Andere Bands | Abgesagt? |\n");
            sb.append("|-------|-----|-------|--------------|-----------|\n");
            String lastYear = "3000";
            int counter = 0;
            while (executeQuery.next()) {
                ++counter;
                String date = executeQuery.getString(1);
                String thisYear = date.substring(date.length() - 4);
                if (!thisYear.equals(lastYear)) {
                    sb.append("| **").append(thisYear).append("** |\n");
                    lastYear = thisYear;
                }
                sb.append("| ").append(date);
                sb.append(" | ").append(executeQuery.getString(2));
                sb.append(" | ").append(formatVenue(executeQuery.getString(3)));
                sb.append(" | ").append(createBand(bandCache, executeQuery.getArray(4)));
                String canceled = executeQuery.getString(5);
                if ("ja".equals(canceled)) {
                    --counter;
                }
                sb.append(" | ").append(canceled);
                sb.append(" |\n");
            }

            sb.append("\n");
            sb.insert(0, " Konzerte stattgefunden:\n\n").insert(0, counter).insert(0, "Bisher haben ");
            System.out.println(counter + " Concerts");
            output(sb, "../data/ea80concerts.md");
        }
    }

    private static void output(StringBuilder sb, String filelocation) throws IOException {
        try (BufferedWriter br = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(filelocation), StandardCharsets.UTF_8))) {
            br.write(sb.toString(), 0, sb.length());
        }
    }

    private static String formatVenue(String input) {
        if (input.startsWith("?")) {
            return "";
        }
        return input;
    }

    private static String createBand(Map<Long, String> bandCache, Array string) throws SQLException {
        if (string == null) {
            return "";
        }
        Long[] array = (Long[]) string.getArray();

        return Stream.of(array).map(bandCache::get)
                .collect(Collectors.joining(", "));
    }
}
