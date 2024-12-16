import java.sql.*;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class DatabaseUtils {


    public static Connection con;
    public static String url;
    public static Properties prop;





  public   static  void loadAndGetConnection() {
        try {
            // Load the JDBC driver
            Class.forName("org.postgresql.Driver");
            // Create a properties object with username and password
            prop = new Properties();
            prop.setProperty("user", "postgres");
            prop.setProperty("password", "changeme");
            // Set the JDBC URL
            url = "jdbc:postgresql://postgresql:5432/northwind?useSSL=false";
            // Get the connection
            con = DriverManager.getConnection(url, prop);
        } catch (ClassNotFoundException e) {
            // Handle exception
        } catch (SQLException e) {
            // Handle exception
        }
    }


   public  static void getAllRows(){
        try {

            Statement st = con.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM customers");
            while (rs.next()) {
                System.out.print("Column 1 returned ");
                System.out.println(rs.getString(1));
            }
            rs.close();
            st.close();

        } catch(SQLException e) {
            // Handle exception
          e.printStackTrace();
        }
    }

   public  static void InsertRow(int id, String name){
        try {


            long start = System.currentTimeMillis();
          /*  Random rnd = new Random();
            Customer cust = new Customer(rnd.nextInt(), UUID.randomUUID().toString());*/

           /* String sql = "insert into customers" +
                    "VALUES (" + String.valueOf(cust.getID()) + ","+ cust.getName() +");";*/


            String insertSQL = "INSERT INTO customers (id, name) VALUES (?, ?)";
            PreparedStatement st  = con.prepareStatement(insertSQL);

            st.setString(1, String.valueOf(id));
            st.setString(2,name);
            st.executeUpdate();


            // st.executeQuery();


            //st.executeUpdate(sql);
         /*   while (rs.next()) {
                System.out.print("Column 1 returned ");
                System.out.println(rs.getString(1));
            }*/


            st.close();
            // con.commit();
            //con.close();
            // con.close();

            long end = System.currentTimeMillis();
            System.out.println( "db time  " + (end - start));


        } catch(SQLException e) {
            // Handle exception
            e.printStackTrace();
        }
    }
}
