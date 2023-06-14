package migration;

import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
//import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import static constants.DataMigrationConstants.*;

public class SolrManagerThreaded implements Runnable {
    private static Logger logger = Logger.getLogger(SolrManagerThreaded.class);

    final static String dirName = "/npc/home/hverma/data-migration-jar/delta-collection-data/";
    final static String fileFormat = "yyyyMMdd-HHmmss-S";
    final static TimeZone utc = TimeZone.getTimeZone("UTC");
    final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    final static SimpleDateFormat formatter6 = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");

    SolrDocumentList docs;
    SolrClient targetSolrServer;
    String targetCollection;
    int batchCommitSize;
    SolrQuery solrQuery;


    public SolrManagerThreaded(SolrDocumentList docs, SolrClient targetSolrServer, String targetCollection, int batchCommitSize, SolrQuery solrQuery) {
        this.docs = docs;
        this.targetSolrServer = targetSolrServer;
        this.targetCollection = targetCollection;
        this.batchCommitSize = batchCommitSize;
        this.solrQuery = solrQuery;
        logger = SolrIndexHDFSToSOLR.logger;
    }


    @Override
    public void run() {
        try {
            boolean isAdditionSuccess = false;
            try {
                isAdditionSuccess = addDocsToTargetSolr(docs);
            } catch (Exception e) {
                logger.error("Error while inserting doc Target Server ");
            }
            logger.info(SOLRstatusinsert + (isAdditionSuccess));
        } catch (Exception e) {
            logger.error("Exception while DR insert - " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
	private boolean addDocsToTargetSolr(SolrDocumentList docs) throws InterruptedException, IOException, ParseException {
        ArrayList<SolrInputDocument> inputDocList = new ArrayList<SolrInputDocument>();
        int count = 0;
        @SuppressWarnings("unused")
		int maxTries = 3;
        while (true) {
            try {
                Iterator<SolrDocument> iter = docs.iterator();
                UpdateResponse res = null;
                while (iter.hasNext()) {
                    SolrInputDocument solrInputDocument = new SolrInputDocument();
                    SolrDocument solrDocument = iter.next();
                    for (String name : solrDocument.getFieldNames()) {
                        if (!name.equals(VERSION))
                            solrInputDocument.addField(name, solrDocument.getFieldValue(name));
                    }
                    inputDocList.add(solrInputDocument);
                }
                res = targetSolrServer.add(targetCollection, inputDocList);
//            targetSolrServer.commit(targetCollection);  // periodically flush
                if (res.getStatus() == 0) {
                    logger.info(DocumenthasbeenindexedSuccessfullyIn + COLON + QTIME + COLON + +res.getQTime());
                    docs.clear();
                    inputDocList.clear();
                    return true;
                } else {
                    logger.error("docs could not be added in solr for base url ");
                }
            } catch (Exception e) {
                count = count + 1;
                logger.error("Sleeping for 5000 MS Retry Attempt " + count + " Facing Error" + e.getMessage() + "\nsolrQuery" + COLON + solrQuery);
                Thread.sleep(5000);
                if (count == 3) {
                    JSONArray inputDocListJSonArray = new JSONArray();
                    Iterator<SolrInputDocument> inputDocListItr = inputDocList.iterator();
                    while (inputDocListItr.hasNext()) {
                        SolrInputDocument solrinputDocument = inputDocListItr.next();
                        JSONObject inputDocListJSonOBJ = new JSONObject();
                        for (String name : solrinputDocument.getFieldNames()) {
                            String simpleName = solrinputDocument.getFieldValue(name).getClass().getSimpleName();
                            if (DATE.equals(simpleName)) {
                                Date date1 = formatter6.parse(String.valueOf(solrinputDocument.getFieldValue(name)));
                                sdf.setTimeZone(utc);
                                inputDocListJSonOBJ.put(name, sdf.format(date1));
                            } else {
                                inputDocListJSonOBJ.put(name, solrinputDocument.getFieldValue(name));
                            }
                        }
                        inputDocListJSonArray.add(inputDocListJSonOBJ);
                    }

                    String fileName = new SimpleDateFormat(fileFormat).format(new Date());
                    FileWriter myWriter = new FileWriter(dirName + targetCollection + fileName + JSON);
                    myWriter.write(String.valueOf(inputDocListJSonArray));
                    myWriter.close();

                    logger.error("Unable to save record After " + count + " Attempt  solrQuery::\n" + solrQuery + "\n" + e.getMessage());

                    inputDocListJSonArray = null;
                    docs.clear();
                    inputDocList.clear();
                    return false;
                }
            }
//            finally {
//                docs.clear();
//                inputDocList.clear();
//            }
        }
    }


}
