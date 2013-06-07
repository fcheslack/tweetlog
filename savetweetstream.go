package main

import (
    //    "bufio"
    //    "bytes"
    "encoding/json"
    "flag"
    "fmt"
    //    "github.com/araddon/httpstream"
    "database/sql"
    "github.com/fcheslack/webtypes/twitter"
    "github.com/kurrik/oauth1a"
    "github.com/fcheslack/tweetlog/tweetstore"
    _ "github.com/mattn/go-sqlite3"
    "io/ioutil"
    "net/http"
    //    "os"
    "strings"
    "time"
)

var _ = ioutil.ReadAll //DEBUG
var _ = flag.Parse     //DEBUG

var ts = tweetstore.SqliteTweetStore{}
var tr = TwitterClient{}

var (
    dbname        *string = flag.String("dbname", "", "SQLite3 DB")
    trackarg      *string = flag.String("track", "", "Search Terms")
    screennamearg *string = flag.String("screen_name", "", "Screen name for user timeline")
    configfile    *string = flag.String("config", "archiveconfig.json", "Path to configuration file")
)

type ArchiveConfig struct {
    Track        []string
    ScreenName   string
    DBName       string
    AccessToken  string `json:"token"`
    AccessSecret string `json:"secret"`
}

type AppConfig struct {
    ConsumerKey    string
    ConsumerSecret string
}

func main() {
    flag.Parse()
    command := flag.Arg(0)

    //load config files and possibly override with command line args
    var appConfig = AppConfig{}
    LoadJsonFile("appconfig.json", &appConfig)

    var archiveConfig = ArchiveConfig{}
    LoadJsonFile("archiveconfig.json", &archiveConfig)

    track := archiveConfig.Track
    screenname := archiveConfig.ScreenName
    if *trackarg != "" {
        track = strings.Split(*trackarg, ",")
    }
    if *screennamearg != "" {
        screenname = *screennamearg
    }
    if *dbname != "" {
        archiveConfig.DBName = *dbname
    }

    db, err := sql.Open("sqlite3", archiveConfig.DBName)
    if err != nil {
        fmt.Println("Error opening sqlite3: %s\n", err)
        return
    }
    defer db.Close()
    ts.Initialize(db)

    httpClient := new(http.Client)

    //set up oauth for signing requests
    service := &oauth1a.Service{
        RequestURL:   "https://api.twitter.com/oauth/request_token",
        AuthorizeURL: "https://api.twitter.com/oauth/request_token",
        AccessURL:    "https://api.twitter.com/oauth/request_token",
        ClientConfig: &oauth1a.ClientConfig{
            ConsumerKey:    appConfig.ConsumerKey,
            ConsumerSecret: appConfig.ConsumerSecret,
            CallbackURL:    "localhost:10000",
        },
        Signer: new(oauth1a.HmacSha1Signer),
    }

    token := archiveConfig.AccessToken
    secret := archiveConfig.AccessSecret
    userConfig := oauth1a.NewAuthorizedConfig(token, secret)

    tr.HttpClient = httpClient
    tr.Service = service
    tr.UserConfig = userConfig

    switch {
    case command == "backfillsearch":
        fmt.Printf("Back-Filling search\n")
        results := tr.FillSearch(*trackarg, 0)
        err := ts.SaveTweets(results)
        if err != nil {
            fmt.Printf("Error saving search tweet")
            //fmt.Printf("Error saving search tweet: %s\n", err)
        }
        fmt.Printf("%d tweets from search retrieved.\n", len(results))
    case command == "backfillusertimeline":
        fmt.Printf("Back-Filling usertimeline\n")
        results := tr.FillUserTimeline(screenname, 0)
        err := ts.SaveTweets(results)
        if err != nil {
            fmt.Printf("Error saving usertimeline tweet: %s\n", err)
        }
        fmt.Printf("%d tweets from user timeline retrieved.\n", len(results))
    case command == "backfillhometimeline":
        fmt.Printf("Back-Filling hometimeline\n")
        results := tr.FillHomeTimeline(0)
        err := ts.SaveTweets(results)
        if err != nil {
            fmt.Printf("Error saving home timeline tweet: %s\n", err)
        }
        fmt.Printf("%d tweets from home timeline retrieved.\n", len(results))
    case command == "stream":
        //get last tweetid to fill the hole before streaming starts
        lastId := ts.LatestTweetId()
        fmt.Printf("Last tweetid currently in DB: %d\n", lastId)
        //fill home timeline and searches since lastId
        fmt.Printf("back-Filling home timeline\n")
        homeRows := tr.FillHomeTimeline(lastId)
        err := ts.SaveTweets(homeRows)
        if err != nil {
            fmt.Printf("Error saving home timeline tweet: %s\n", err)
        }
        fmt.Printf("%d rows backfilled\n", len(homeRows))
        for _, searchTerm := range track {
            fmt.Printf("back-Filling search for %s\n", searchTerm)
            searchRows := tr.FillSearch(searchTerm, lastId)
            err := ts.SaveTweets(searchRows)
            if err != nil {
                fmt.Printf("Error saving search tweet: %s\n", err)
            }
            fmt.Printf("%d rows backfilled\n", len(searchRows))
        }

        //update lastId to keep track between first round of filling and streaming start
        lastId = ts.LatestTweetId()
        //start a streaming connection
        fmt.Printf("Streaming\n")
        //make channel to accept twitter streaming lines
        lc := make(chan []byte, 100)
        //tell twitter client to start the stream, and reconnect while it can
        go tr.MaintainUserStream(track, lc)
        //process the twitter streaming lines that come through
        ProcessLines(lc)

        //TODO: last round of REST requests to make sure we didnt miss anything

    }
    /*
       results := tr.FillSearch([]string{"thatcamp"}, nil)
       fmt.Printf("Got Search Results\n\n\n")
       for _, t := range results {
           fmt.Printf("%s : %s: %s - %s\n", t.Id_str, t.User.Screen_name, t.Text, t.Source)
       }
    */
    return
}

func ProcessLines(linechan chan []byte) {
    for {
        line := <-linechan
        msg := TryTwitterTypes(line)

        switch msg := msg.(type) {
        case *twittertypes.Tweet:
            //fmt.Printf("Successfully unmarshalled tweet\n")
            now := time.Now()
            fmt.Printf("%s %s: %s - %s\n\n", now.Format("03:04:05"), msg.User.Screen_name, msg.Text, msg.Source)
            msg.RawBytes = line
            err := ts.SaveTweet(msg)
            if err != nil {
                fmt.Printf("Tweet saved.\n")
            }
        case *twittertypes.FriendList:
            fmt.Printf("Got Friendlist\n")
        case *twittertypes.Event:
            fmt.Printf("Got Event: %s\n", msg.Event)
        default:
            fmt.Printf("Unhandled type\n")
        }
    }
}

func TryTwitterTypes(line []byte) interface{} {
    tweet := &twittertypes.Tweet{}
    friendlist := &twittertypes.FriendList{}
    twitterevent := &twittertypes.Event{}

    /*
       deletion := &twittertypes.Tweet{}
       limit := &twittertypes.Tweet{}
       scrubgeo := &twittertypes.Tweet{}
       statuswithheld := &twittertypes.Tweet{}
       userwithheld := &twittertypes.Tweet{}
       disconnect := &twittertypes.Tweet{}
       stallwarning := &twittertypes.Tweet{}
       toomanyfollows := &twittertypes.Tweet{}
       envelope := &twittertypes.Tweet{}
    */

    err1 := json.Unmarshal(line, tweet)
    err2 := json.Unmarshal(line, friendlist)
    err3 := json.Unmarshal(line, twitterevent)
    if err1 != nil {
        //fmt.Printf("Error unmarshalling into tweet: %v\n", err1)
    }
    if err2 != nil {
        //fmt.Printf("Error unmarshalling into friendlist: %v\n", err2)
    }
    if err3 != nil {
        //fmt.Printf("Error unmarshalling into twitterevent: %v\n", err3)
    }

    if tweet.Text != "" {
        //fmt.Printf("Returning Tweet\n")
        return tweet
    }
    if len(friendlist.Friends) != 0 {
        fmt.Printf("Returning Friendlist\n")
        return friendlist
    }
    if twitterevent.Event != "" {
        fmt.Printf("Returning Event\n")
        return twitterevent
    }
    fmt.Printf("====== No twitter type unmarhsalling successful.\n")
    return nil
}

func LoadJsonFile(filename string, holder interface{}) {
    b, err := ioutil.ReadFile(filename)
    if err != nil {
        fmt.Printf("Error reading file: %s\n", err)
        return
    }
    err = json.Unmarshal(b, holder)
    if err != nil {
        fmt.Printf("Error unmarshalling config file: %s\n", err)
        return
    }
    return
}
