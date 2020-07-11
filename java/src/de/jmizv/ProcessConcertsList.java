package de.jmizv;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import org.postgresql.core.BaseConnection;

/**
 *
 * @author Alex
 */
public class ProcessConcertsList {

    static final Map<String, String> country = new HashMap<>();
    static final Set<String> visitedCities = new HashSet<>();
    static final Set<String> visitedBands = new HashSet<>();

    static {
        country.put("CH", "Schweiz");
        country.put("AT", "Österreich");
        country.put("NL", "Niederlande");
    }

    private static String formatCity(String city) {
        if (visitedCities.contains(city)) {
            return city;
        }
        visitedCities.add(city);
        if (city.contains("/")) {
            String[] splitted = city.split("/");

            return "[[" + splitted[0] + "]]/[[" + country.get(splitted[1]) + "|" + splitted[1] + "]]";
        }
        return "[[" + city + "]]";
    }

    public static void main(String[] args) throws Exception {
        Class.forName("org.postgresql.Driver");
        boolean reset = false;
        try ( BaseConnection connection = (BaseConnection) DriverManager.getConnection("jdbc:postgresql://localhost:5432/ea80", "postgres", "KwiTiU07")) {
            if (reset) {
                Statement stmt = connection.createStatement();
                stmt.executeUpdate("DELETE FROM concert");
                stmt.executeUpdate("DELETE FROM venue");
                stmt.executeUpdate("DELETE FROM city");
                stmt.executeUpdate("DELETE FROM band");

                insertEntries(connection);
            }
            printToWikiTable(connection);
            printStatisticsForCity(connection);
            printStatisticsForBand(connection);

        }
    }

    static void insertEntries(BaseConnection connection) throws IOException, SQLException {
        PreparedStatement stmt = connection.prepareCall("CALL createConcert(?,?,?,?,?,?,?)");
        PreparedStatement getLastInsert = connection.prepareCall("SELECT max(id) FROM concert");
        PreparedStatement updateDateFormat = connection.prepareCall("UPDATE concert SET dateformat=? WHERE id=?");
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
                //System.err.println("Could not execute " + call);
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

    static List<String[]> getLines() throws IOException {
        List<String[]> result = new ArrayList<>();
        try ( BufferedReader br = new BufferedReader(new InputStreamReader(
                new FileInputStream("../data/ea80concerts.csv"), "UTF-8"))) {
            String string;
            while ((string = br.readLine()) != null) {
                result.add(string.split("\t", 9));
            }
        }
        return result;
    }

    static Map<Long, String> getBandCache(BaseConnection conn) throws SQLException {
        ResultSet executeQuery = conn.prepareStatement("SELECT id, name FROM BAND").executeQuery();
        Map<Long, String> result = new HashMap<>();
        while (executeQuery.next()) {
            long aInt = executeQuery.getInt(1);
            String name = executeQuery.getString(2);
            result.put(aInt, name);
        }
        return result;
    }

    private static void printStatisticsForCity(BaseConnection conn) throws SQLException, IOException {
        printStatistic(conn,
                "SELECT count(*), city.name FROM concert c,venue v,city WHERE c.venueid=v.id and v.cityid=city.id GROUP BY city.name ORDER BY 1,2 DESC",
                "Ort"
        );
    }

    private static void printStatisticsForBand(BaseConnection conn) throws SQLException, IOException {
        printStatistic(conn,
                "select count(*), b.name from (select unnest(otherbands) \"id\" from concert)c,band b WHERE c.id=b.id group by b.id order by 1,2 desc",
                "Band"
        );
    }

    private static void printStatistic(BaseConnection conn, String sql, String header) throws SQLException, IOException {
        ResultSet executeQuery = conn.prepareStatement(sql).executeQuery();
        StringBuilder sb = new StringBuilder();
        sb.append("{|class=\"wikitable\"\n!#\n!").append(header).append("\n!Anzahl\n|-\n");
        int counter = 0;
        while (executeQuery.next()) {
            sb.append("| ").append(++counter).append("\n");
            sb.append("| ").append(executeQuery.getString(2)).append("\n");
            sb.append("| ").append(executeQuery.getInt(1)).append("\n");
            sb.append("|-").append("\n");
            if (counter >= 10) {
                break;
            }
        }
        sb.delete(sb.length() - 3, sb.length());
        sb.append("|}").append("\n");
        output(sb, "../data/statistics_" + header + ".txt");
    }

    static void printToWikiTable(BaseConnection conn) throws SQLException, IOException {
        Map<Long, String> bandCache = getBandCache(conn);
        ResultSet executeQuery = conn.prepareStatement(
                "SELECT to_char(dateofconcert,dateformat),city.name,venue.name,otherbands,case when canceled='0' then '' else 'ja' end FROM concert,city,venue WHERE concert.venueid=venue.id AND venue.cityid=city.id ORDER BY dateofconcert DESC").executeQuery();
        StringBuilder sb = new StringBuilder();

        sb.append("{|class=\"wikitable\"\n"
                + "!Datum\n"
                + "!Ort\n"
                + "!Venue\n"
                + "!Andere Bands\n"
                + "!Abgesagt?\n"
                + "|-\n");
        String lastYear = "3000";
        int counter = 0;
        while (executeQuery.next()) {
            ++counter;
            String date = executeQuery.getString(1);
            String thisYear = date.substring(date.length() - 4);
            if (!thisYear.equals(lastYear)) {
                sb.append("! colspan=\"5\" |").append(thisYear).append("\n");
                sb.append("|-").append("\n");
                lastYear = thisYear;
            }
            sb.append("| style=\"text-align: right;\" |").append(date).append("\n");
            sb.append("| ").append(formatCity(executeQuery.getString(2))).append("\n");
            sb.append("| ").append(formatVenue(executeQuery.getString(3))).append("\n");
            sb.append("| ").append(createBand(bandCache, executeQuery.getArray(4))).append("\n");
            String canceled = executeQuery.getString(5);
            if ("ja".equals(canceled)) {
                --counter;
            }
            sb.append("| ").append(canceled).append("\n");
            sb.append("|-").append("\n");
        }
        sb.delete(sb.length() - 3, sb.length());
        sb.append("|}").append("\n");
        sb.insert(0, " Konzerte stattgefunden:\n\n").insert(0, counter).insert(0, "Bisher haben ");
        System.out.println(counter + " Concerts");
        output(sb, "../data/ea80concerts.txt");
    }

    private static void output(StringBuilder sb, String filelocation) throws IOException {
        try ( BufferedWriter br = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(filelocation), "UTF-8"))) {
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
        StringBuilder sb = new StringBuilder();

        for (Long l : array) {
            String band = bandCache.get(l);
            if (!visitedBands.contains(band)) {
                visitedBands.add(band);
                sb.append("[[").append(band).append("]]");
            } else {
                sb.append(band);
            }
            sb.append(", ");
        }

        return sb.delete(sb.length() - 2, sb.length()).toString();
    }
}
