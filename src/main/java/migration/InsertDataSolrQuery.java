//package migration;
//
//import org.apache.log4j.FileAppender;
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
//import org.apache.log4j.PatternLayout;
//import org.apache.solr.client.solrj.SolrClient;
//import org.apache.solr.client.solrj.SolrQuery;
//import org.apache.solr.client.solrj.SolrServerException;
//import org.apache.solr.client.solrj.impl.HttpClientUtil;
//import org.apache.solr.client.solrj.impl.HttpSolrClient;
//import org.apache.solr.client.solrj.impl.HttpSolrServer;
////import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
//import org.apache.solr.client.solrj.response.QueryResponse;
//
//import java.io.IOException;
//
//public class InsertDataSolrQuery extends SolrIndexHDFSToSOLR {
//    public static final Logger logger = Logger.getLogger(SolrIndexHDFSToSOLR.class);
//
//    public static void main(String[] args) throws IOException, SolrServerException {
//
//        long startTime = System.currentTimeMillis();
//        logger.setLevel(Level.ALL);
//        String sourceSolrServerStr = null;
//        String targetSolrServerStr = null;
//        String jaasConfFilePath = null;
//        String sourceCollection = null;
//        String targetCollection = null;
//        String externalSolrParam = null;
//        if (args.length > 0) {
//            sourceSolrServerStr = args[0];
//            targetSolrServerStr = args[1];
//            jaasConfFilePath = args[2];
//            sourceCollection = args[3];
//            targetCollection = args[4];
//            externalSolrParam = args[5];
//            try {
//                if (args.length > 6 && !args[6].isEmpty()) {
//                    MAX_ROWS = Integer.parseInt(args[6]);
//                } else {
//                    logger.error("args[6] argument is not present");
//                }
//            } catch (NumberFormatException e) {
//                logger.error("Argument MAX_ROWS args[6]::" + args[6] + "::must be an integer");
//            }
//            try {
//                if (args.length > 7 && !args[7].isEmpty()) {
//                    batchCommitSize = Integer.parseInt(args[7]);
//                } else {
//                    logger.error("args[7] argument is not present");
//                }
//            } catch (NumberFormatException e) {
//                logger.error("Argument batchCommitSize args[7]  ::" + args[7] + "::must be an integer");
//            }
//
//            logFileAppender = args[8];
//            PatternLayout layout = new PatternLayout();
//            layout.setConversionPattern("%d{ISO8601} [%t] (%F:%L) - %m%n");
//            FileAppender appender = new FileAppender(layout, logFileAppender, false);
//            logger.addAppender(appender);
//
//            logger.info("\nsourceSolrServerStr :: " + sourceSolrServerStr + "\ntargetSolrServerStr :: " + targetSolrServerStr + "\njaasConfFilePath :: " + jaasConfFilePath + "\nsourceCollection :: " + sourceCollection + "\ntargetCollection :: " + targetCollection + "\nexternalSolrParam :: " + externalSolrParam + "\n" +
//                    "" +
//                    "MAX_ROWS :: " + MAX_ROWS + "\nbatchCommitSize :: " + batchCommitSize + "\nlogFileAppender :: " + logFileAppender);
//
//        } else {
//            logger.info("no arguments passed, please pass arguments ");
//        }
//
//        System.setProperty("java.security.auth.login.config", jaasConfFilePath);
////        HttpClientUtil.setConfigurer(new Krb5HttpClientConfigurer());
////        HttpSolrServer SourceSolrServer = new HttpSolrServer(sourceSolrServerStr);
////        HttpSolrServer targeSolrServer = new HttpSolrServer(targetSolrServerStr);
//
//        SolrClient SourceSolrServer = new HttpSolrClient.Builder(sourceSolrServerStr).build();
//        SolrClient targeSolrServer = new HttpSolrClient.Builder(targetSolrServerStr).build();
//
//
//        SolrQuery solrQuery = new SolrQuery();
//        String solrQueryStr = "rows=25000&q=*:*&sort=id desc&fq={!frange l=1 u=1}mod(_version_, 9)&timeAllowed=0&cursorMark=AoE/CFNhbk1hdGVvIUxJVkU6UFJPRCE5NTg4MTR8fE5PVDY1MjA1LjAwMQ==";
//
//        String[] solrQueryArr = solrQueryStr.split("&");
//
//        for (String queryParam : solrQueryArr) {
//            System.out.println(queryParam);
//            String[] param = queryParam.split("=");
//            solrQuery.set(param[0], param[1]);
//        }
//
//        logger.info("solrQuery " + solrQuery);
////        solrQuery.set("rows",2);
////
////        solrQuery.setQuery("*:*");
////        solrQuery.setFields("id");
//        long sourCollectionNumFound = SourceSolrServer.query(sourceCollection, solrQuery).getResults().getNumFound();
//        logger.info("sourCollectionNumFound" + sourCollectionNumFound);
//        QueryResponse queryResponse = null;
//        queryResponse = SourceSolrServer.query(sourceCollection, solrQuery);
//        logger.info("queryResponse.getResults().size() " + queryResponse.getResults().size());
//        logger.info("queryResponse.getResults() " + queryResponse.getResults());
//
////        writeDataInSolr(queryResponse, targeSolrServer, targetCollection, solrQuery);
//    }
//
//
//}
