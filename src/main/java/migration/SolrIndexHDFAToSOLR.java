//package migration;
//
//import org.apache.http.HttpEntity;
//import org.apache.http.auth.AuthScope;
//import org.apache.http.auth.UsernamePasswordCredentials;
//import org.apache.http.client.CredentialsProvider;
//import org.apache.http.client.methods.CloseableHttpResponse;
//import org.apache.http.client.methods.HttpGet;
//import org.apache.http.conn.ssl.NoopHostnameVerifier;
//import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
//import org.apache.http.impl.client.*;
//import org.apache.http.ssl.SSLContextBuilder;
//import org.apache.http.ssl.SSLContexts;
//import org.apache.solr.client.solrj.SolrServerException;
//import org.apache.solr.client.solrj.impl.HttpClientUtil;
//import org.apache.solr.client.solrj.impl.HttpSolrClient;
//import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
//import org.apache.solr.client.solrj.response.QueryResponse;
//import org.apache.solr.common.params.ModifiableSolrParams;
//import org.apache.solr.common.params.SolrParams;
//import org.apache.http.client.config.AuthSchemes;
//
//import javax.net.ssl.SSLContext;
//import java.io.File;
//import java.io.IOException;
//import java.security.KeyManagementException;
//import java.security.KeyStoreException;
//import java.security.NoSuchAlgorithmException;
//import java.security.cert.CertificateException;
//
//public class SolrIndexHDFAToSOLR {
//    //private static String sslCertificatePath = "C:\\Users\\heman\\Desktop\\NetSmart\\keystore.jks";
//    private static String sslCertificatePath = "D:\\my-data\\NB\\keystore.jks";
////    private static String sslCertificatePath = "/opt/cloudera/security/jks_solr/keystore.jks";
//
//    public static void main(String[] args) throws IOException, SolrServerException, CertificateException,
//            NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
//        System.out.println("data");
//
//        //Preparing the Solr client
////        System.setProperty("java.security.auth.login.config", "/opt/solr2/jaas.conf");
//        System.setProperty("java.security.auth.login.config", "D:\\my-data\\NB\\jaas-client.conf");
//        String solrUrl = "https://tpmci1clw001.npc.lan:8985/solr";
//        HttpClientUtil.setConfigurer(new Krb5HttpClientConfigurer());
//        //HttpSolrServer server = new HttpSolrServer(solrUrl);
//
//        File file = new File(sslCertificatePath);
//        SSLContextBuilder SSLBuilder = SSLContexts.custom();
//        SSLBuilder = SSLBuilder.loadTrustMaterial(file);
//        SSLContext sslContext = SSLBuilder.build();
//        SSLConnectionSocketFactory sslConSocFactory = new SSLConnectionSocketFactory(sslContext, new NoopHostnameVerifier());
//        HttpClientBuilder clientBuilder = HttpClients.custom();
//        clientBuilder.setSSLSocketFactory(sslConSocFactory);
//
//		/*CredentialsProvider provider = new BasicCredentialsProvider();
//		provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("hverma", "hV082820aB"));
//		CloseableHttpClient httpClient = clientBuilder.setDefaultCredentialsProvider(provider).build();*/
//
//        ModifiableSolrParams params = new ModifiableSolrParams();
//        params.set("q", ":");
//        params.set("rows", "10");
//        params.set("wt", "json");
//
//        CloseableHttpClient httpClient = HttpClientUtil.createClient(params);
//        httpClient = clientBuilder.build();
//
//	    /*HttpGet requestCluster = new HttpGet(solrUrl);
//	    CloseableHttpResponse response = httpClient.execute(requestCluster);
//	    HttpEntity entity = response.getEntity();
//	    System.out.println(entity);*/
//
//        //AuthSchemes.KERBEROS
//        HttpSolrClient solrClient = new HttpSolrClient(solrUrl, httpClient);
//        QueryResponse response = solrClient.query("CQIProcedure", params);
//        System.out.println(response.getResults().getNumFound());
//
//        //Builder solrClientBuilder = new HttpSolrClient.Builder(solrUrl);
//        //SolrClient client = solrClientBuilder.withHttpClient(httpClient).build();
//        //HttpEntity entity = response.getEntity();
//        //System.out.println(solrClient.query("client", query).getResults().getNumFound());
//
//    }
//}