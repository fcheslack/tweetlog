package main

import (
    "code.google.com/p/go.net/websocket"
    "database/sql"
    "encoding/json"
    "flag"
    "fmt"
    "github.com/fcheslack/webtypes/twitter"
    "github.com/fcheslack/tweetlog/analytics"
    "github.com/fcheslack/tweetlog/tweetstore"
    _ "github.com/mattn/go-sqlite3"
    "log"
    "net/http"
    "time"
)

var (
    port     *string = flag.String("port", "10001", "http service port")
    dbname   *string = flag.String("dbname", "../tweets.db", "SQLite3 DB")
    dataPath *string = flag.String("dataPath", "./data", "Path to folder for persistent storage")

    tweetStore  = tweetstore.SqliteTweetStore{}
    tweetServer *TweetServer
)

type Message struct {
    Type string      `json:"Type,omitempty"`
    Body interface{} `json:"Body,omitempty"`
}

type TweetServer struct {
    ServeMux   *http.ServeMux
    Address    string
    TweetCast  chan Message
    Tweethub   hub
    TweetStore *tweetstore.SqliteTweetStore
    curTweetId int64
    Analytics  *analytics.Analytics
}

func (ts *TweetServer) Init() {
    ts.Tweethub = hub{
        broadcast:   make(chan Message),
        register:    make(chan *connection),
        unregister:  make(chan *connection),
        connections: make(map[*connection]bool),
    }

    ts.TweetCast = make(chan Message)
    ts.ServeMux = http.NewServeMux()

    ts.ServeMux.HandleFunc("/recent", recentHandler)

    ts.ServeMux.HandleFunc("/links", linksHandler)

    ts.ServeMux.HandleFunc("/stats", statsHandler)

    ts.ServeMux.Handle("/ws", websocket.Handler(wsHandler))

    ts.Analytics = &analytics.Analytics{}
    ts.Analytics.Tweetstore = &tweetStore
}

func (ts *TweetServer) Start() {
    ts.curTweetId = ts.TweetStore.LatestTweetId()

    go ts.Tweethub.run()
    go ts.TimedStream()

    if err := http.ListenAndServe(ts.Address, ts.ServeMux); err != nil {
        log.Fatal("ListenAndServe:", err)
    }

}

func (ts *TweetServer) BroadcastTweet(tweet *twittertypes.Tweet) {
    /*j, err := json.Marshal(tweet)
      if err != nil {
          fmt.Printf("Error marshalling tweet: %s\n", err)
          return
      }*/
    m := Message{Type: "tweet", Body: tweet}
    ts.Tweethub.broadcast <- m
}

/*
func (ts *TweetServer) wsHandler(ws *websocket.Conn) {
    c := &connection{send: make(chan Message, 256), ws: ws}
    ts.Tweethub.register <- c
    defer func() { ts.Tweethub.unregister <- c }()
    go c.writer()
    //c.reader()
}
*/
func wsHandler(ws *websocket.Conn) {
    c := &connection{send: make(chan Message, 256), ws: ws}
    tweetServer.Tweethub.register <- c
    defer func() { tweetServer.Tweethub.unregister <- c }()
    c.writer()
    //c.reader()
}

func (ts *TweetServer) TimedStream() {
    for {
        fmt.Printf("TimedStream - Checking for new tweets\n")
        <-time.After(5 * time.Second)
        tweets := ts.TweetStore.TweetsAfterId(ts.curTweetId)
        fmt.Printf("%d tweets after tweetid %d\n", len(tweets), ts.curTweetId)
        for _, tweet := range tweets {
            m := Message{Type: "tweet", Body: tweet}
            ts.Tweethub.broadcast <- m
            if int64(*tweet.Id) > ts.curTweetId {
                ts.curTweetId = int64(*tweet.Id)
            }
        }
        m := Message{Type: "message", Body: ""}
        ts.Tweethub.broadcast <- m
    }
}

func recentHandler(rw http.ResponseWriter, req *http.Request) {
    rw.Header().Set("Access-Control-Allow-Origin", "*")
    if req.Method == "GET" {
        recentTweets := tweetStore.RecentTweets(200)
        j, err := json.Marshal(recentTweets)
        if err != nil {
            log.Printf("Error marshalling recent tweets: %s\n", err)
        }
        rw.Write(j)
    } else if req.Method == "OPTIONS" {
        rw.Header().Set("Access-Control-Allow-Methods", "GET")
    }
}

func linksHandler(rw http.ResponseWriter, req *http.Request) {
    rw.Header().Set("Access-Control-Allow-Origin", "*")
    if req.Method == "GET" {
        startTime := time.Now().Add(-24 * time.Hour)
        endTime := time.Now()
        tweeturls := tweetStore.IntervalUrls(startTime, endTime)
        fmt.Printf("%d urls within time limit\n", len(tweeturls))

        j, err := json.Marshal(tweeturls)
        if err != nil {
            log.Printf("Error marshalling recent tweets: %s\n", err)
        }
        rw.Write(j)
    } else if req.Method == "OPTIONS" {
        rw.Header().Set("Access-Control-Allow-Methods", "GET")
    }
}

func statsHandler(rw http.ResponseWriter, req *http.Request) {
    rw.Header().Set("Access-Control-Allow-Origin", "*")
    if req.Method == "GET" {
        startTime := time.Now().Add(-24 * time.Hour)
        endTime := time.Now()
        tweets := tweetStore.IntervalTweets(startTime, endTime)
        fmt.Printf("%d tweets within time limit\n", len(tweets))

        tweetServer.Analytics.Tweets = tweets
        urls, urlcounts := tweetServer.Analytics.UrlsByFrequency()
        screennames, screennameCounts := tweetServer.Analytics.UsersByPosts()
        hashtags, hashtagCounts := tweetServer.Analytics.HashtagsByFrequency()

        for url, count := range urlcounts {
            fmt.Printf("%d  - %s\n", count, url)
        }
        for screenname, count := range screennameCounts {
            fmt.Printf("%d  - %s\n", count, screenname)
        }
        for hashtag, count := range hashtagCounts {
            fmt.Printf("%d  - %s\n", count, hashtag)
        }
        _ = urls
        _ = screennames
        _ = hashtags

        j, err := json.Marshal(urls)
        if err != nil {
            log.Printf("Error marshalling recent tweets: %s\n", err)
        }
        rw.Write(j)
    } else if req.Method == "OPTIONS" {
        rw.Header().Set("Access-Control-Allow-Methods", "GET")
    }
}

func main() {
    flag.Parse()

    db, err := sql.Open("sqlite3", *dbname)
    if err != nil {
        fmt.Println("Error opening sqlite3: %s\n", err)
        return
    }
    defer db.Close()
    tweetStore.Initialize(db)

    tweetServer = &TweetServer{}
    tweetServer.Init()
    tweetServer.Address = ":" + *port
    tweetServer.TweetStore = &tweetStore
    tweetServer.Start()
}

type connection struct {
    // The websocket connection.
    ws  *websocket.Conn

    // Buffered channel of outbound messages.
    send chan Message
}

/*
func (c *connection) reader() {
    for {
        var message Message
        err := websocket.JSON.Receive(c.ws, &message)
        if err != nil {
            break
        }
        tweetServer.Tweethub.broadcast <- message
    }
    c.ws.Close()
}
*/
func (c *connection) writer() {
    for message := range c.send {
        err := websocket.JSON.Send(c.ws, message)
        if err != nil {
            break
        }
    }
    c.ws.Close()
}

type hub struct {
    // Registered connections.
    connections map[*connection]bool

    // Inbound messages from the connections.
    broadcast chan Message

    // Register requests from the connections.
    register chan *connection

    // Unregister requests from connections.
    unregister chan *connection
}

func (h *hub) run() {
    for {
        select {
        case c := <-h.register:
            h.connections[c] = true
        case c := <-h.unregister:
            delete(h.connections, c)
            close(c.send)
        case m := <-h.broadcast:
            for c := range h.connections {
                select {
                case c.send <- m:
                default:
                    delete(h.connections, c)
                    close(c.send)
                    go c.ws.Close()
                }
            }
        }
    }
}
