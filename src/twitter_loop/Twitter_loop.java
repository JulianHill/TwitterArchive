package twitter_loop;
/**
 * Created with IntelliJ IDEA.
 * User: julian
 * Date: 20.10.13
 * Time: 13:01
 * To change this template use File | Settings | File Templates.
 */
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Scanner;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.UserMentionEntity;
import twitter4j.conf.ConfigurationBuilder;


        //TEST!!!
public class Twitter_loop {

    /**
     *
     * author Muhammad.Saifuddin
     */


    private ConfigurationBuilder cb;
    private DB db;
    private DBCollection items;



    public Twitter_loop() {



    }

    /**
     * static block used to construct a connection with tweeter with twitter4j
     * configuration with provided settings. This configuration builder will be
     * used for next search action to fetch the tweets from twitter.com.
     */
    static {

    }

    public static void main(String[] args) throws InterruptedException {

        Twitter_loop taskObj = new Twitter_loop();
        taskObj.loadMenu();
    }

    public void loadMenu() throws InterruptedException {

        System.out.println("=========\n\tTwitter Archive\n===========");

        System.out.print("Please choose your Keyword:\t");

        Scanner input = new Scanner(System.in);
        String keyword = input.nextLine();


        connectdb(keyword);

        int i = 0;

        while(i < 1)
        {
            cb = new ConfigurationBuilder();
            cb.setDebugEnabled(true);
            cb.setOAuthConsumerKey("XXX");
            cb.setOAuthConsumerSecret("XXX");
            cb.setOAuthAccessToken("XXX");
            cb.setOAuthAccessTokenSecret("XXX");

            getTweetByQuery(true,keyword);
            cb = null;




            Thread.sleep(60 * 1000);              // wait

        }


    }

    public void connectdb(String keyword)
    {
        try {
            // on constructor load initialize MongoDB and load collection
            initMongoDB();
            items = db.getCollection(keyword);
            items = db.getCollection(keyword);
            BasicDBObject index = new BasicDBObject("tweet_ID", 1);
            items.ensureIndex(index, new BasicDBObject("unique", true));

        } catch (MongoException ex) {
            System.out.println("MongoException :" + ex.getMessage());
        }

    }


    /**
     * initMongoDB been called in constructor so every object creation this
     * initialize MongoDB.
     */
    public void initMongoDB() throws MongoException {
        try {
            System.out.println("Connecting to Mongo DB..");
            Mongo mongo;
            mongo = new Mongo("127.0.0.1");
            db = mongo.getDB("tweetDB");
        } catch (UnknownHostException ex) {
            System.out.println("MongoDB Connection Error :" + ex.getMessage());
        }
    }

    /**
     * void getTweetByQuery method used to fetch records from twitter.com using
     * Query class to define query for search param with record count.
     * QueryResult persist result from twitter and provide into the list to
     * iterate records 1 by one and later on item.insert is call to store this
     * BasicDBObject into MongoDB items Collection.
     *
     */
    public void getTweetByQuery(boolean loadRecords, String keyword) throws InterruptedException {


        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();



        if (cb != null) {

            try {
                Query query = new Query(keyword);
                query.setCount(50);
                QueryResult result;
                result = twitter.search(query);
                System.out.println("Getting Tweets...");
                List<Status> tweets = result.getTweets();

                for (Status tweet : tweets) {
                    BasicDBObject basicObj = new BasicDBObject();
                    basicObj.put("user_name", tweet.getUser().getScreenName());
                    basicObj.put("retweet_count", tweet.getRetweetCount());
                    basicObj.put("tweet_followers_count", tweet.getUser().getFollowersCount());
                    basicObj.put("source",tweet.getSource());
                    basicObj.put("coordinates",tweet.getGeoLocation());


                    UserMentionEntity[] mentioned = tweet.getUserMentionEntities();
                    basicObj.put("tweet_mentioned_count", mentioned.length);
                    basicObj.put("tweet_ID", tweet.getId());
                    basicObj.put("tweet_text", tweet.getText());

                   // if (mentioned.length > 0) {
//                    System.out.println("Mentioned length " + mentioned.length + " Mentioned: " + mentioned[0].getName());
                   // }
                    try {
                        items.insert(basicObj);
                    } catch (Exception e) {
                        System.out.println("MongoDB Connection Error : " + e.getMessage());
                        //loadMenu();
                    }
                }
                // Printing fetched records from DB.
                if (loadRecords) {
                    getTweetsRecords();
                }

            } catch (TwitterException te) {
                System.out.println("te.getErrorCode() " + te.getErrorCode());
                System.out.println("te.getExceptionCode() " + te.getExceptionCode());
                System.out.println("te.getStatusCode() " + te.getStatusCode());
                if (te.getStatusCode() == 401) {
                    System.out.println("Twitter Error : \nAuthentication credentials (https://dev.twitter.com/pages/auth) were missing or incorrect.\nEnsure that you have set valid consumer key/secret, access token/secret, and the system clock is in sync.");
                } else {
                    System.out.println("Twitter Error : " + te.getMessage());
                }



            }
        } else {
            System.out.println("MongoDB is not Connected! Please check mongoDB intance running..");
        }
    }






    /**
     * void method print fetched records from mongodb This method use the
     * preloaded items (Collection) for fetching records and print them on
     * console.
     */
    public void getTweetsRecords() throws InterruptedException {
        BasicDBObject fields = new BasicDBObject("_id", true).append("user_name", true).append("tweet_text", true);
        DBCursor cursor = items.find(new BasicDBObject(), fields);

        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
        //loadMenu();
    }

}

