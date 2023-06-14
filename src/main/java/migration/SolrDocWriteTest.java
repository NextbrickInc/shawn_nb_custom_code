package migration;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
//import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.json.simple.JSONArray;
import org.noggit.JSONUtil;
import org.json.simple.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
//import org.apache.noggit.JSONUtil;

import static constants.DataMigrationConstants.VERSION;

public class SolrDocWriteTest {



    public static void main(String[] args) throws IOException, SolrServerException, ParseException {
        final String dirName = "D:\\";
        final String fileFormat = "yyyyMMdd-HHmmss-S";
        String targetCollection = "cms";

        SolrDocumentList docs = getResult();


        Iterator<SolrDocument> iter = docs.iterator();

        ArrayList<SolrInputDocument> inputDocList = new ArrayList<SolrInputDocument>();
        while (iter.hasNext()) {
            SolrInputDocument solrInputDocument = new SolrInputDocument();
            SolrDocument solrDocument = iter.next();
            for (String name : solrDocument.getFieldNames()) {
                if (!name.equals(VERSION))
                    solrInputDocument.addField(name, solrDocument.getFieldValue(name));
            }
            inputDocList.add(solrInputDocument);
        }


        String fileName = new SimpleDateFormat(fileFormat).format(new Date());
        FileWriter myWriter = new FileWriter(dirName + targetCollection + fileName+ ".json");
//        String inputDocListJSON = JSONUtil.toJSON(inputDocList); //this has the json documents
        JSONArray inputDocListJSonArray = new JSONArray();


        Iterator<SolrInputDocument> inputDocListItr = inputDocList.iterator();
        while (inputDocListItr.hasNext()) {
            SolrInputDocument solrInputDocument = new SolrInputDocument();
            SolrInputDocument solrinputDocument = inputDocListItr.next();
            JSONObject inputDocListJSonOBJ = new JSONObject();
            for (String name : solrinputDocument.getFieldNames()) {
                String simpleName = solrinputDocument.getFieldValue(name).getClass().getSimpleName();
                if(simpleName.equals("Date")){
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
//                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

                    System.out.println(solrinputDocument.getFieldValue(name));

                    SimpleDateFormat formatter6 =new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
                    Date date1=formatter6.parse(String.valueOf(solrinputDocument.getFieldValue(name)));
                    System.out.println(sdf.format(date1));
                    final TimeZone utc = TimeZone.getTimeZone("UTC");
                    sdf.setTimeZone(utc);
                    inputDocListJSonOBJ.put(name, sdf.format(date1));
                }
                else{
                    inputDocListJSonOBJ.put(name, solrinputDocument.getFieldValue(name));
                }
//                System.out.println("name " + name + " getSimpleName "+ solrinputDocument.getFieldValue(name).getClass().getSimpleName());

            }
            inputDocListJSonArray.add(inputDocListJSonOBJ);
        }

//        StringBuffer strBfr = new StringBuffer();
//        List<Map<String, Object>> tempListMap = new ArrayList<Map<String, Object>>();
//        Iterator<SolrDocument> docsIter = docs.iterator();
//        while (docsIter.hasNext()) {
//            Map<String, Object> tempMap = new HashMap<String, Object>();
//            SolrDocument solrDocument = docsIter.next();
//            solrDocument.removeFields(VERSION);
//            tempMap = solrDocument.getFieldValueMap();
//            tempListMap.add(tempMap);
//        }
//        strBfr.append(tempListMap.toString());

//        System.out.println(strBfr);
//        myWriter.write(String.valueOf(strBfr));


        System.out.println(inputDocListJSonArray);
        myWriter.write(String.valueOf(inputDocListJSonArray));
        myWriter.close();

    }

    private static SolrDocumentList getResult() throws IOException, SolrServerException {
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRows(2);
        solrQuery.set("q", "*:*");
        solrQuery.setFilterQueries("mimetype:jpeg");
        String sourceSolrServerStr = "http://localhost:8985/solr/";
        String sourceCollection = "cms";
//        HttpSolrServer SourceSolrServer = new HttpSolrServer(sourceSolrServerStr);
        SolrClient SourceSolrServer = new HttpSolrClient.Builder(sourceSolrServerStr).build();
        QueryResponse queryResponse = SourceSolrServer.query(sourceCollection, solrQuery);
        return queryResponse.getResults();

    }
}
