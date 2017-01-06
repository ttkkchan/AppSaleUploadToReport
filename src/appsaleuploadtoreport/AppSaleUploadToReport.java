/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package appsaleuploadtoreport;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author tkchan
 */
public class AppSaleUploadToReport {

  /**
   * @param args the command line arguments
   */
  public static void main(String[] args) throws ParseException {
    AppSaleUploadToReport myMain = new AppSaleUploadToReport();
    //Date start = sdfymd.parse()
    Calendar start = Calendar.getInstance();
    Calendar end = Calendar.getInstance();
    if (args.length == 0) {
      start.setTimeInMillis(end.getTimeInMillis() - (1 * 24 * 60 * 60 * 1000L));
    } else if (args.length == 1) {
      start.setTimeInMillis(sdfymd.parse(args[0]).getTime());
      end.setTimeInMillis(start.getTimeInMillis() + (24 * 60 * 60 * 1000L));
    } else if (args.length == 2) {
      //System.out.println( "start="+args[0]+" end="+args[1]);
      start.setTimeInMillis(sdfymd.parse(args[0]).getTime());
      end.setTimeInMillis(sdfymd.parse(args[1]).getTime());
    }

    System.out.println("start=" + AppSaleUploadToReport.sdfymd.format(start.getTimeInMillis()) + " end=" + AppSaleUploadToReport.sdfymd.format(end.getTime()));
    myMain.fillAppSale(start, end);
  }
  private Connection connReport = null;
  private Connection connFmg = null;
  private Connection connLLWeb = null;
  private Connection connLLVoice = null;
  private Connection connPhoneApp = null;
  private Connection connChatApp = null;
//  private String fmgSqlServer = "jdbc:jtds:sqlserver://172.17.12.11:1433/Reports"; //;instance=BAKDB";
  private String fmgSqlServer = "jdbc:jtds:sqlserver://172.17.12.17:1433/Reports"; //;instance=BAKDB";
  private String fmgSqlServerUser = "ccbackend";
  private String fmgSqlServerUserPasswd = "Change2notsa";
  private String phoneAppDbServer = "jdbc:mysql://10.9.2.31:3306/ihub2_production"; //
  private String phoneAppDbServerUser = "ihub2_read";
  private String phoneAppDbServerUserPasswd = "newnk4cash";
  //private String chatAppDbServer = "jdbc:mysql://10.9.2.32:4100/api"; //
  private String chatAppDbServer = "jdbc:mysql://172.17.12.14:4100/api"; //
  private String chatAppDbServerUser = "api_read_only";
  private String chatAppDbServerUserPasswd = "d1al0g1c";
  private String llWebSqlServer = "jdbc:mysql://10.50.4.103:3100/Accounting";
  private String llWebSqlServerUser = "web";
  private String llWebSqlServerUserPasswd = "L0nd0n2o12";
  public static final SimpleDateFormat sdfymd = new SimpleDateFormat("yyyyMMdd");
  public static final SimpleDateFormat sdfmdy = new SimpleDateFormat("MMM.dd.yyyy");
  public static final SimpleDateFormat sdflabel = new SimpleDateFormat("MMM dd/yy");
  public static final SimpleDateFormat sdfheading = new SimpleDateFormat("EEEEEEEE, MMM dd - hh:mm aa");
  public static final SimpleDateFormat sdfmydate = new SimpleDateFormat("yyyy-MM-dd");

  private void fillAppSale(Calendar start, Calendar end) {
    try {
      Class.forName("net.sourceforge.jtds.jdbc.Driver");
      Class.forName("com.mysql.jdbc.Driver");

      connPhoneApp = DriverManager.getConnection(phoneAppDbServer, phoneAppDbServerUser, phoneAppDbServerUserPasswd);
      connChatApp = DriverManager.getConnection(chatAppDbServer, chatAppDbServerUser, chatAppDbServerUserPasswd);
      connReport = DriverManager.getConnection(fmgSqlServer, fmgSqlServerUser, fmgSqlServerUserPasswd);
      connLLWeb = DriverManager.getConnection(llWebSqlServer, llWebSqlServerUser, llWebSqlServerUserPasswd);
      try {
        Statement repStmt = connReport.createStatement();
        Statement stmt = connPhoneApp.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery(
                // "SELECT YEAR(app.created_at) as cyear,MONTH(app.created_at) as cmonth,DAY(app.created_at) as cday,WEEK(app.created_at,4) as cwknum"
                "SELECT DATE(app.created_at) AS `date`, " // ,package_id
                + "sum(CASE package_id "
                + " WHEN 'qc_and_min_first_60'  THEN 9.99"
                + " WHEN 'qc_and_min_first_90'  THEN 14.50"
                + " WHEN 'qc_and_min_first_160' THEN 24.50"
                + " WHEN 'qc_and_min_first_420' THEN 49.50"
                + " WHEN 'qc_and_min_reg_20'    THEN 9.99"
                + " WHEN 'qc_and_min_reg_90'    THEN 29.00"
                + " WHEN 'qc_and_min_reg_160'   THEN 49.00"
                + " WHEN 'qc_and_min_reg_420'   THEN 99.00"
                + " WHEN 'ne_and_min_first_60'    THEN 9.99"
                + " WHEN 'ne_and_min_first_100'   THEN 16.00"
                + " WHEN 'ne_and_min_first_240'   THEN 38.00"
                + " WHEN 'ne_and_min_first_380'   THEN 55.00"
                + " WHEN 'ne_and_min_reg_20'      THEN 9.99"
                + " WHEN 'ne_and_min_reg_70'      THEN 32.00"
                + " WHEN 'ne_and_min_reg_180'     THEN 76.00"
                + " WHEN 'ne_and_min_reg_360'     THEN 110.00"
                + " WHEN 'nln_and_min_first_60'   THEN 9.99"
                + " WHEN 'nln_and_min_first_90'   THEN 14.50"
                + " WHEN 'nln_and_min_first_160'  THEN 24.50"
                + " WHEN 'nln_and_min_first_420'  THEN 49.50"
                + " WHEN 'nln_and_min_reg_20'     THEN 9.99"
                + " WHEN 'nln_and_min_reg_90'     THEN 29.00"
                + " WHEN 'nln_and_min_reg_160'    THEN 49.00"
                + " WHEN 'nln_and_min_reg_420'    THEN 99.00"
                + " END)  AS saleTotal, count(*) AS total"
                + ", CASE accounts.client_application_id WHEN 1 THEN 1 WHEN 11 THEN 5 WHEN 13 THEN 9 END AS product"
                + " FROM account_playstore_purchase app "
                + "      INNER JOIN accounts ON accounts.id = app.account_id"
                + "      WHERE accounts.client_application_id IN (1,11,13)"
                + " AND app.created_at >= '" + sdfymd.format(start.getTime()) + "'"
                + " AND app.created_at < '" + sdfymd.format(end.getTime()) + "'"
                + " GROUP BY DATE(app.created_at),CASE accounts.client_application_id WHEN 1 THEN 1 WHEN 11 THEN 5 WHEN 13 THEN 9 END"
                + " ORDER BY DATE(app.created_at)");
        if (rs != null) {
          try {
            int i = 0;
            while (rs.next()) {
              //rs.next();
              try {
                System.out.println("** date=" + rs.getString(1)
                        //+ " package code="+rs.getString(2)
                        + " price =" + rs.getString(2)
                        + " total =" + rs.getString(3)
                        + " product=" + rs.getString(4));
                String payType = "1";
                String insertSale = "INSERT DailyRevenueReport VALUES('" + rs.getString(1) + "'," + rs.getString(4) + ",1," + rs.getString(2) + ",1";
                // System.out.println("insertSale =" + insertSale);
                repStmt.executeUpdate(insertSale);
              } catch (Exception e) {
                System.out.println("Exception:" + e);

              }
            }
          } catch (SQLException ex) {
            //Logger.getLogger(myMain.class.getName()).log(Level.SEVERE, null, ex);
          }
        }
      } catch (Exception e) {
        System.out.println("Exception:" + e);
        //conn.close();
      } finally {
        //conn.close();
      }
      try {
        Statement stmt = connChatApp.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        Statement repStmt = connReport.createStatement();

        ResultSet rs = stmt.executeQuery(
                "SELECT DATE(b.time_completed) AS `date` "
                + " ,cast( sum(b.price) as decimal(10,2) ) as saleTotal, count(*) AS total, bp.product AS product, platform "
                + " FROM billing_purchases b join billing_packages bp on b.package_code = bp.code "
                + " WHERE b.user_id > 1000150 AND b.time_completed >= '" + sdfymd.format(start.getTime()) + "'"
                + "  AND b.time_completed < '" + sdfymd.format(end.getTime()) + "'"
                + " GROUP BY DATE(b.time_completed), bp.product ,platform" // b.package_code, bp.code "
                + " ORDER BY date ");
        if (rs != null) {
          try {
            int i = 0;
            while (rs.next()) {
              //rs.next();
              try {
                System.out.println(" date=" + rs.getString(1)
                        //+ " package code="+rs.getString(2)
                        + " price =" + rs.getString(2)
                        + " total =" + rs.getString(3)
                        + " product=" + rs.getString(4)
                        + " platform=" + rs.getString(5));
                String payType = "18";
                if ("ios".equalsIgnoreCase(rs.getString(5))) { // 18 - Android and //19 = ios
                  payType = "19";
                }
                String insertSale = "INSERT DailyRevenueReport VALUES('" + rs.getString(1) + "'," + rs.getString(4) + "," + payType + "," + rs.getString(2) + ",1)";
                repStmt.executeUpdate(insertSale);
              } catch (Exception e) {
                System.out.println("Exception:" + e);

              }
            }
          } catch (SQLException ex) {
//            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
          }
        }
      } catch (Exception e) {
        System.out.println("Exception:" + e);
        //conn.close();
      } finally {
        //conn.close();
      }

      Connection connWeb = connLLWeb;
      try {
        Statement repStmt = connReport.createStatement();
        if (connWeb != null) {
          Statement stmtWeb = connWeb.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
          //System.out.println("connectedWeb");
          TimeZone tz = start.getTimeZone();
          int offsetStart = (int) (tz.getOffset(start.getTimeInMillis()) / 3600000);
          int offsetEnd = (int) (tz.getOffset(end.getTimeInMillis()) / 3600000);

          String xx = "select date_format(date_add(dateCreated, INTERVAL '" + offsetStart + "' HOUR),'%Y-%m-%d') saleDate"
                  + ",  CAST( SUM(cost) as decimal(10,2)) totalSale, COUNT(*) as total" // -- , 157 brand, 1 paymentType"
                  + ", case currencyId when 0 then 2 when 1 then 1 when 3 then 4 when 4 then 1 end countryCode "
                  + "  from Accounting.Purchase p "
                  + "where  xactionTypeId in (6,31,32,47,55,56,59) "
                  //+ "  and dateCreated < 'feb 2, 2011 5:00:00' "
                  + "and dateCreated >= '" + sdfmydate.format(start.getTime()) + " " + ("0" + String.valueOf(Math.abs(offsetStart))) + ":00:00' " //'jan 31, 2011' "
                  + "and dateCreated < '" + sdfmydate.format(end.getTime()) + " " + ("0" + String.valueOf(Math.abs(offsetEnd))) + ":00:00' "
                  + " group by date_format(date_add(dateCreated, INTERVAL '" + offsetStart + "' HOUR),'%Y-%m-%d') "
                  + ",case currencyId when 0 then 2 when 1 then 1 when 3 then 4 when 4 then 1 end "
                  + "order by dateCreated ";

          //System.out.println(xx);
          ResultSet rsWeb = stmtWeb.executeQuery(xx);
          if (rsWeb != null) {
            int iWeb = 0;
            while (rsWeb.next()) {
              System.out.println(" date=" + rsWeb.getString(1)
                      //+ " package code="+rs.getString(2)
                      + " price =" + rsWeb.getString(2)
                      + " total =" + rsWeb.getString(3)
                      + " product= 157" // + rsWeb.getString(4)
                      + " countryCode=" + rsWeb.getString(4));
              String insertSale = "INSERT DailyRevenueReport VALUES('" + rsWeb.getString(1) + "',157,1,"+rsWeb.getString(2)+","+ rsWeb.getString(4) + ")";
              repStmt.executeUpdate(insertSale);

//            LavalifeWebSale lws = new LavalifeWebSale(rsWeb);
//            if (saleHashMap.get(lws.getHashKey()) == null) {
//              saleHashMap.put(lws.getHashKey(), lws);
//            }
            }
          }
        }
      } catch (SQLException ex) {
        Logger.getLogger(AppSaleUploadToReport.class.getName()).log(Level.SEVERE, null, ex);
      }









    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}
