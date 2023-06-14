package migration;

import org.apache.log4j.*;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
//import org.apache.solr.client.solrj.impl.HttpSolrServer;
//import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CursorMarkParams;

import java.io.IOException;

import static constants.DataMigrationConstants.*;

public class SolrIndexHDFSToSOLR {
    public static final Logger logger = Logger.getLogger(SolrIndexHDFSToSOLR.class);

    public static final String UNIQUE_ID_FIELD = "id";
    public static int MAX_ROWS = 1001;
    public static int batchCommitSize = 1000;
    public static String logFileAppender = null;
    public static int gb = 1024 * 1024 * 1024;

    public static void main(String[] args) throws IOException, SolrServerException, InterruptedException {

        long startTime = System.currentTimeMillis();
        logger.setLevel(Level.ALL);
        String sourceSolrServerStr = null;
        String targetSolrServerStr = null;
        String jaasConfFilePath = null;
        String sourceCollection = null;
        String targetCollection = null;
        String externalSolrParam = null;
        if (args.length > 0) {
            sourceSolrServerStr = args[0];
            targetSolrServerStr = args[1];
            jaasConfFilePath = args[2];
            sourceCollection = args[3];
            targetCollection = args[4];
            externalSolrParam = args[5];
            try {
                if (args.length > 6 && !args[6].isEmpty()) {
                    MAX_ROWS = Integer.parseInt(args[6]);
                } else {
                    logger.error("args[6] argument is not present");
                }
            } catch (NumberFormatException e) {
                logger.error("Argument MAX_ROWS args[6]::" + args[6] + "::must be an integer");
            }
            try {
                if (args.length > 7 && !args[7].isEmpty()) {
                    batchCommitSize = Integer.parseInt(args[7]);
                } else {
                    logger.error("args[7] argument is not present");
                }
            } catch (NumberFormatException e) {
                logger.error("Argument batchCommitSize args[7]  ::" + args[7] + "::must be an integer");
            }

            logFileAppender = args[8];
            PatternLayout layout = new PatternLayout();
            layout.setConversionPattern("%d{ISO8601} [%t] (%F:%L) - %m%n");
            FileAppender appender = new FileAppender(layout, logFileAppender, false);
            logger.addAppender(appender);

            logger.info("\nsourceSolrServerStr :: " + sourceSolrServerStr + "\ntargetSolrServerStr :: " + targetSolrServerStr + "\njaasConfFilePath :: " + jaasConfFilePath + "\nsourceCollection :: " + sourceCollection + "\ntargetCollection :: " + targetCollection + "\nexternalSolrParam :: " + externalSolrParam + "\n" +
                    "" +
                    "MAX_ROWS :: " + MAX_ROWS + "\nbatchCommitSize :: " + batchCommitSize + "\nlogFileAppender :: " + logFileAppender);

        } else {
            logger.info("no arguments passed, please pass arguments ");
        }

//        System.setProperty("java.security.auth.login.config", "/opt/solr2/jaas.conf");
        System.setProperty("java.security.auth.login.config", jaasConfFilePath);
//        String solrUrl = "https://tpmci1clw001.npc.lan:8985/solr";
//        HttpClientUtil.setConfigurer(new Krb5HttpClientConfigurer());
//        System.setProperty("java.security.auth.login.config", "/ntst/services/config/jaas-client.conf");
//        HttpClientUtil.setConfigurer(new Krb5HttpClientConfigurer());
//        String sourceSolrServerStr = "https://tpmci1clw001.npc.lan:8985/solr";
//        HttpSolrServer SourceSolrServer = new HttpSolrServer(sourceSolrServerStr);
        SolrClient SourceSolrServer = new HttpSolrClient.Builder(sourceSolrServerStr).build();
        SolrClient targeSolrServer = new HttpSolrClient.Builder(targetSolrServerStr).build();

//        String targetSolrServerStr = "https://tpmci1clw001.npc.lan:9985/solr";
//        HttpSolrServer targeSolrServer = new HttpSolrServer(targetSolrServerStr);

//        SolrQuery solrQuery = new SolrQuery();
//        ModifiableSolrParams params = new ModifiableSolrParams();
//        params.set("q", ":");
//        params.set("rows", "10");
//        params.set("wt", "json");
//        solrQuery.add(params);
//
//        QueryResponse response = SourceSolrServer.query("CQIProcedure", solrQuery, METHOD.GET);
//        System.out.println(response.getResults().getNumFound());

        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRows(MAX_ROWS);
        solrQuery.set("q", "*:*");
        solrQuery.setSort(SolrQuery.SortClause.desc(UNIQUE_ID_FIELD));
//        solrQuery.setFields("*,orig_version_l:version");
        if (externalSolrParam != null && !externalSolrParam.equals("")) {
            solrQuery.setFilterQueries(externalSolrParam);
        }


        // You can't use "TimeAllowed" with "CursorMark"
        // The documentation says "Values <= 0 mean
        // no time restriction", so setting to 0.
        solrQuery.setTimeAllowed(0);

        String cursorMark = CursorMarkParams.CURSOR_MARK_START;
        boolean done = false;
        QueryResponse queryResponse = null;
        int recordProcessedCounter = 0;
        long sourCollectionNumFound = SourceSolrServer.query(sourceCollection, solrQuery).getResults().getNumFound();
        logger.info("\n\n***** Total NumFound Docs in :: " + sourceCollection + " collection is ::" + sourCollectionNumFound + "::\n\n");
        while (!done) {
            solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
            int responseResultSize = 0;
            try {
                queryResponse = null;
                logger.info(newLine + SOLRQUERY + COLON + solrQuery);
//                System.exit(0);// Comment this for testing of solr queries
                queryResponse = SourceSolrServer.query(sourceCollection, solrQuery);
                responseResultSize = queryResponse.getResults().size();
                logger.info(queryResponseSize + COLON + responseResultSize + COLON + QTIME + COLON + queryResponse.getQTime());
            } catch (SolrServerException e) {
                logger.error("Solr Query Exception", e);
                return;
            }
            recordProcessedCounter = recordProcessedCounter + responseResultSize;
            if (responseResultSize > 0) {
                writeDataInSolr(queryResponse, targeSolrServer, targetCollection, solrQuery);
                logger.info(totalRecordsProcessed + COLON + recordProcessedCounter);
            }
            String nextCursorMark = queryResponse.getNextCursorMark();
            if (cursorMark.equals(nextCursorMark)) {
                done = true;
                long endTime = System.currentTimeMillis();
                logger.info("\n\n*****Source Collection " + sourceCollection + " NumFound ::" + sourCollectionNumFound + "::");
                logger.info("*****Target Collection " + targetCollection + " NumFound ::" + queryResponse.getResults().getNumFound() + "::");
                logger.info("*********INGESTION PROCESS FINISHED -- Total Records Processed :: " + recordProcessedCounter + "::*********");
                logger.info("*********Time consumed in Seconds::" + (endTime - startTime) / 1000 + "::*********");
                logger.info("*********Time consumed in Minutes::" + (endTime - startTime) / 1000 / 60 + "::*********");
            } else {
                cursorMark = nextCursorMark;
            }
        }
    }

    private static void writeDataInSolr(QueryResponse queryResponse, SolrClient targeSolrServer, String targetCollection, SolrQuery solrQuery) {
        try {
            SolrManagerThreaded solrInsert = new SolrManagerThreaded(queryResponse.getResults(), targeSolrServer, targetCollection, batchCommitSize, solrQuery);
            Thread targetDataInsertThread = new Thread(solrInsert);
            targetDataInsertThread.start();
            logger.info(TargetDataInsertThreadID + targetDataInsertThread.getId());
        } catch (Exception e) {
            logger.error("Exception while saving in Target Server-" + e.getMessage());
        }
    }
}