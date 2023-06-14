package migration;

import static constants.DataMigrationConstants.COLON;
import static constants.DataMigrationConstants.QTIME;
import static constants.DataMigrationConstants.SOLRQUERY;
import static constants.DataMigrationConstants.TargetDataInsertThreadID;
import static constants.DataMigrationConstants.newLine;
import static constants.DataMigrationConstants.queryResponseSize;
import static constants.DataMigrationConstants.totalRecordsProcessed;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CursorMarkParams;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command public class SolrReIndex implements Runnable {
    public static final Logger logger = Logger.getLogger(SolrReIndex.class);

    @CommandLine.Option(names = { "-sss", "--sourceSolrServer"}, description = "ex. value : https://urldefense.com/v3/__http://localhost:8983/solr__;!!E5G8dfB9hj4!4BF6hRleB3zEuZXnGFX5VVOKwMYzbsFtobvTl5FGtnl7NLxghfN6quEQPuMY_0nq9EnTWHPN-O1onENK9aiQR5VFfw$ ")
    static String sourceSolrServerStr;

    @CommandLine.Option(names = { "-tss", "--targetSolrServer"}, description = "ex. value : https://urldefense.com/v3/__http://localhost:8983/solr__;!!E5G8dfB9hj4!4BF6hRleB3zEuZXnGFX5VVOKwMYzbsFtobvTl5FGtnl7NLxghfN6quEQPuMY_0nq9EnTWHPN-O1onENK9aiQR5VFfw$ ")
    String targetSolrServerStr;

    @CommandLine.Option(names = { "-sssu", "--sourceSolrServerUser"}, defaultValue = "", description = "ex. value : admin")
    String sourceSolrServerUser;

    @CommandLine.Option(names = { "-sssp", "--sourceSolrServerPassword"}, defaultValue = "", description = "ex. value : password123")
    String sourceSolrServerPassword;

    @CommandLine.Option(names = { "-tssu", "--targetSolrServerUser"}, defaultValue = "", description = "ex. value : admin")
    String targetSolrServerUser;

    @CommandLine.Option(names = { "-tssp", "--targetSolrServerPassword"}, defaultValue = "", description = "ex. value : password123")
    String targetSolrServerPassword;

    @CommandLine.Option(names = { "-sc", "--sourceCollection"}, description = "ex. value : test")
    String sourceCollection;

    @CommandLine.Option(names = { "-tc", "--targetCollection"}, description = "ex. value : demo")
    String targetCollection;

    @CommandLine.Option(names = { "-esp", "--externalSolrParam"}, defaultValue = "", description = "Filter Query to be applied")
    String externalSolrParam;

    @CommandLine.Option(names = { "-mr", "--maxRows"}, defaultValue = "1000", description = "rows in every solr query param")
    int MAX_ROWS;

    @CommandLine.Option(names = { "-bcs", "--batchCommitSize"}, defaultValue = "50000")
    static int batchCommitSize;

    @CommandLine.Option(names = { "-lfp", "--logFilePath"}, defaultValue = "logs/default-file.log")
    String logFileAppender;

    @CommandLine.Option(names = { "-uid", "--uniqueId"}, defaultValue = "id")
    String UNIQUE_ID_FIELD;

    @CommandLine.Option(names = { "-v", "--verbose"}, description = "Verbose mode. Helpful for troubleshooting. Multiple -v options increase the verbosity.")
    private boolean[] verbose;

    @CommandLine.Option(names = { "-h", "--help", "-?", "-help"}, usageHelp = true, description = "Display this help and exit")
    private boolean help;

//    public static final String UNIQUE_ID_FIELD = "id";
//    public static int gb = 1024 * 1024 * 1024;

    public static void main(final String[] args) {
        new CommandLine(new SolrReIndex()).execute(args);
    }

    @Override
    public void run() {

        final long startTime = System.nanoTime();
        logger.setLevel(Level.ALL);

            final PatternLayout layout = new PatternLayout();
            layout.setConversionPattern("%d{ISO8601} [%t] (%F:%L) - %m%n");
            FileAppender appender = null;
            try {
                appender = new FileAppender(layout, logFileAppender, false);
            } catch (final IOException e) {
                e.printStackTrace();
            }
            logger.addAppender(appender);

//        SolrClient sourceSolrClient = new HttpSolrClient.Builder(sourceSolrServerStr).build();
//        SolrClient targeSolrClient = new HttpSolrClient.Builder(targetSolrServerStr).build();

        final Http2SolrClient sourceSolrClient = new Http2SolrClient.Builder(sourceSolrServerStr).withConnectionTimeout(10, TimeUnit.SECONDS).useHttp1_1(true).withBasicAuthCredentials(sourceSolrServerUser, sourceSolrServerPassword).build();
        final Http2SolrClient targetSolrClient = new Http2SolrClient.Builder(targetSolrServerStr).withConnectionTimeout(10, TimeUnit.SECONDS).useHttp1_1(true).withBasicAuthCredentials(targetSolrServerUser, targetSolrServerPassword).build();

        final SolrQuery solrQuery = new SolrQuery();
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
        long sourceCollectionNumFound = 0;
        try {
            sourceCollectionNumFound = sourceSolrClient.query(sourceCollection, solrQuery).getResults().getNumFound();
        } catch (final Exception e) {
            e.printStackTrace();
        }

        System.out.println("\n\n***** Total NumFound Docs in :: " + sourceCollection + " collection is ::" + sourceCollectionNumFound + "::\n\n");
        logger.info("\n\n***** Total NumFound Docs in :: " + sourceCollection + " collection is ::" + sourceCollectionNumFound + "::\n\n");
        while (!done) {
            solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
            int responseResultSize = 0;
            try {
                queryResponse = null;
                logger.info(newLine + SOLRQUERY + COLON + solrQuery);
//                System.exit(0);// Comment this for testing of solr queries
                queryResponse = sourceSolrClient.query(sourceCollection, solrQuery);
                responseResultSize = queryResponse.getResults().size();
                logger.info(queryResponseSize + COLON + responseResultSize + COLON + QTIME + COLON + queryResponse.getQTime());
            } catch (SolrServerException | IOException e) {
                logger.error("Solr Query Exception", e);
                return;
            }
            recordProcessedCounter = recordProcessedCounter + responseResultSize;
            if (responseResultSize > 0) {
                writeDataInSolr(queryResponse, targetSolrClient, targetCollection, solrQuery);
                logger.info(totalRecordsProcessed + COLON + recordProcessedCounter);
            }
            final String nextCursorMark = queryResponse.getNextCursorMark();
            if (cursorMark.equals(nextCursorMark)) {
                done = true;
                final long elapsedNanos = System.nanoTime() - startTime;
                logger.info("\n\n*****Source Collection " + sourceCollection + " NumFound ::" + sourceCollectionNumFound + "::");
                logger.info("*****Target Collection " + targetCollection + " NumFound ::" + queryResponse.getResults().getNumFound() + "::");
                logger.info("*********INGESTION PROCESS FINISHED -- Total Records Processed :: " + recordProcessedCounter + "::*********");
                logger.info("*********Time consumed in Seconds::" + TimeUnit.SECONDS.convert(elapsedNanos,TimeUnit.NANOSECONDS) + "::*********");
                logger.info("*********Time consumed in Minutes::" + TimeUnit.MINUTES.convert(elapsedNanos,TimeUnit.NANOSECONDS) + "::*********");
            } else {
                cursorMark = nextCursorMark;
            }
        }
        try {
            sourceSolrClient.close();
            targetSolrClient.commit(targetCollection);
            targetSolrClient.close(); }
        catch (final Exception e) {
            logger.error("Got error closing things down", e);
		}
    }

    private static void writeDataInSolr(final QueryResponse queryResponse, final SolrClient targeSolrServer, final String targetCollection, final SolrQuery solrQuery) {
        try {
            final SolrManagerThreaded solrInsert = new SolrManagerThreaded(queryResponse.getResults(), targeSolrServer, targetCollection, batchCommitSize, solrQuery);
            final Thread targetDataInsertThread = new Thread(solrInsert);
            targetDataInsertThread.start();
            logger.info(TargetDataInsertThreadID + targetDataInsertThread.getId());
        } catch (final Exception e) {
            logger.error("Exception while saving in Target Server-" + e.getMessage());
        }
    }
}