package migration;

import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
//import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.SolrQuery;
//import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

public class SecureSolrJQuery {
//    private static String sslCertificatePath = "/opt/cloudera/security/jks_solr/keystore.jks";
    private static String sslCertificatePath = "D:\\my-data\\NB\\keystore.jks";

    public static void main(String[] args)
            throws IOException, SolrServerException, CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {

        String queryParameter = args.length == 1 ? args[0] : "*";
        System.setProperty("java.security.auth.login.config", "D:\\my-data\\NB\\jaas-clientcCache.conf");

        File file = new File(sslCertificatePath);
        SSLContextBuilder sslBuilder = SSLContexts.custom();
        sslBuilder = sslBuilder.loadTrustMaterial(file);
        SSLContext sslContext = sslBuilder.build();
        SSLConnectionSocketFactory sslConSocFactory = new SSLConnectionSocketFactory(sslContext, new NoopHostnameVerifier());
        HttpClientBuilder clientBuilder = HttpClients.custom();
        clientBuilder.setSSLSocketFactory(sslConSocFactory);


        String urlString = "https://tpmci1clw001.npc.lan:8985/solr/CQIProcedure";
//        SolrServer solr = new HttpSolrServer(urlString);
//        HttpSolrClient solrClient = new HttpSolrClient(urlString);
        SolrClient solrClient = new HttpSolrClient.Builder(urlString).build();

        SolrQuery query = new SolrQuery();
        query.set("q", "text:" + queryParameter);

        QueryResponse response = solrClient.query(query);
        SolrDocumentList results = response.getResults();
        for (int i = 0; i < results.size(); ++i) {
            System.out.println(results.get(i));
        }
    }
}


