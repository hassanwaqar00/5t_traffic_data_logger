package it.polito.5t.trafficdatalogger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class TrafficDataLogger {

    static String url = "http://opendata.5t.torino.it/get_fdt";

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            new MyThread().start();
            System.out.println("Sleeping for " + 5 * 60 * 1000 + "ms");
            Thread.sleep(5 * 60 * 1000);
        }
    }

    public static class MyThread extends Thread {

        public void run() {
            DBHelper dbHelper = new DBHelper();
            try {
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                DocumentBuilder db = dbf.newDocumentBuilder();
                Document doc;
                doc = db.parse(new URL(url).openStream());
                doc.normalize();
                dbHelper.jdbcConnect();
                NodeList nlist = doc.getElementsByTagName("traffic_data");
                Node node = nlist.item(0);
                String start_time = getAttrValue(node, "start_time").replace("T", " ");
                start_time = start_time.substring(0, start_time.length() - 6);
                String end_time = getAttrValue(node, "end_time").replace("T", " ");
                end_time = end_time.substring(0, end_time.length() - 6);
                nlist = doc.getElementsByTagName("FDT_data");
                System.out.println("Got: " + start_time + " " + end_time + " " + nlist.getLength());
                for (int i = 0; i < nlist.getLength(); i++) {
                    node = nlist.item(i);
                    int lcd1 = Integer.valueOf(getAttrValue(node, "lcd1"));
                    int Road_LCD = Integer.valueOf(getAttrValue(node, "Road_LCD"));
                    String Road_name = getAttrValue(node, "Road_name");
                    int offset = Integer.valueOf(getAttrValue(node, "offset"));
                    String direction = getAttrValue(node, "direction");
                    double lat = Double.valueOf(getAttrValue(node, "lat"));
                    double lng = Double.valueOf(getAttrValue(node, "lng"));
                    int accuracy = Integer.valueOf(getAttrValue(node, "accuracy"));
                    int period = Integer.valueOf(getAttrValue(node, "period"));
                    Node childNode = node.getChildNodes().item(1);
                    double speed = Double.valueOf(getAttrValue(childNode, "speed"));
                    double flow = Double.valueOf(getAttrValue(childNode, "flow"));
                    dbHelper.insertTrafficData(start_time, end_time, lcd1, Road_LCD, Road_name, offset, direction, lat, lng, accuracy, period, flow, speed);
                }
            } catch (MalformedURLException ex) {
                Logger.getLogger(TrafficDataLogger.class.getName()).log(Level.WARNING, null, ex);
            } catch (ParserConfigurationException | SAXException | IOException | SQLException ex) {
                Logger.getLogger(TrafficDataLogger.class.getName()).log(Level.WARNING, null, ex);
            } finally {
                dbHelper.jdbcClose();
            }
        }
    }

    static private String getAttrValue(Node node, String attrName) {
        if (!node.hasAttributes()) {
            return "";
        }
        NamedNodeMap nmap = node.getAttributes();
        if (nmap == null) {
            return "";
        }
        Node n = nmap.getNamedItem(attrName);
        if (n == null) {
            return "";
        }
        return n.getNodeValue();
    }

}
