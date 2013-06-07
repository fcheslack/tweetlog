package analytics

import (
    "database/sql"
    //    "flag"
    //    "fmt"
    "github.com/fcheslack/webtypes/twitter"
    "github.com/fcheslack/tweetlog/tweetstore"
    _ "github.com/mattn/go-sqlite3"
    "io/ioutil"
    "sort"
    "strings"
    "time"
)

var _ = ioutil.ReadAll //DEBUG
//var _ = flag.Parse           //DEBUG
var _ = strings.Join         //DEBUG
var _ = twittertypes.Tweet{} //DEBUG

var ts = tweetstore.SqliteTweetStore{}

type sortedMap struct {
    m   map[string]int
    s   []string
}

func (sm *sortedMap) Len() int {
    return len(sm.m)
}

func (sm *sortedMap) Less(i, j int) bool {
    return sm.m[sm.s[i]] > sm.m[sm.s[j]]
}

func (sm *sortedMap) Swap(i, j int) {
    sm.s[i], sm.s[j] = sm.s[j], sm.s[i]
}

func sortedKeys(m map[string]int) []string {
    sm := new(sortedMap)
    sm.m = m
    sm.s = make([]string, len(m))
    i := 0
    for key, _ := range m {
        sm.s[i] = key
        i++
    }
    sort.Sort(sm)
    return sm.s
}

type Analytics struct {
    DB         *sql.DB
    Tweetstore *tweetstore.SqliteTweetStore
    Tweets     []*twittertypes.Tweet
}

func (a *Analytics) Init(db *sql.DB) {
    //    a.DB = db
    //    a.Tweetstore = &tweetstore.SqliteTweetStore{}
    //    a.Tweetstore.Init(db)
}

func (a *Analytics) PrevDayUrls() []*twittertypes.TwitterUrl {
    startTime := time.Now().Add(-24 * time.Hour)
    endTime := time.Now()
    tweeturls := ts.IntervalUrls(startTime, endTime)
    return tweeturls
}

func (a *Analytics) TweetFrequencies() []int {
    iDuration := time.Hour * 4
    tweetcounts := ts.IntervalTweetCount(iDuration, 10)
    return tweetcounts
}

func (a *Analytics) UrlsByFrequency() ([]string, map[string]int) {
    urls := make(map[string]int)
    for _, t := range a.Tweets {
        for _, u := range t.Entities.Urls {
            urls[string(u.Expanded_url)] = urls[string(u.Expanded_url)] + 1
        }
    }
    sortedUrls := sortedKeys(urls)

    return sortedUrls, urls
}

func (a *Analytics) UsersByPosts() ([]string, map[string]int) {
    screennames := make(map[string]int)
    for _, t := range a.Tweets {
        screennames[t.User.Screen_name] = screennames[t.User.Screen_name] + 1
    }
    sortedScreennames := sortedKeys(screennames)

    return sortedScreennames, screennames
}

func (a *Analytics) HashtagsByFrequency() ([]string, map[string]int) {
    tags := make(map[string]int)
    for _, t := range a.Tweets {
        for _, ht := range t.Entities.Hashtags {
            tags[ht.Text] = tags[ht.Text] + 1
        }
    }
    sortedHashtags := sortedKeys(tags)

    return sortedHashtags, tags
}

/*
var (
    dbname   *string = flag.String("dbname", "tweets.db", "SQLite3 DB")
    trackarg *string = flag.String("track", "", "Search Terms")
    tweetid  *int64  = flag.Int64("tweetid", 320553419830095875, "Tweet ID to use for analysis output")
)
*/
/*
func main() {
    flag.Parse()
    //track := strings.Split(*trackarg, ",")

    command := flag.Arg(0)

    db, err := sql.Open("sqlite3", *dbname)
    if err != nil {
        fmt.Println("Error opening sqlite3: %s\n", err)
        return
    }
    defer db.Close()
    ts.Initialize(db)

    rows := ts.Query("SELECT tweetid, screen_name, text FROM tweets LIMIT 10")
    for rows.Next() {
        var tweetid int64
        var screenname string
        var text string
        rows.Scan(&tweetid, &screenname, &text)
        fmt.Printf("%d %s %s\n", tweetid, screenname, text)
    }
    return

    switch {
    case command == "tweetfrequencies":
        iDuration := time.Hour * 4
        tweetcounts := ts.IntervalTweetCount(iDuration, 10)
        fmt.Printf("%+v\n", tweetcounts)
    case command == "tweetsafter":
        tweetsAfter := ts.TweetsAfterId(*tweetid)
        for _, t := range tweetsAfter {
            fmt.Printf("%d : %s : %s\n", *t.Id, t.User.Screen_name, t.Text)
        }
        fmt.Printf("\n%d Tweets after tweetid: %d\n", len(tweetsAfter), *tweetid)
    case command == "tweeturls":
        startTime := time.Now().Add(-24 * time.Hour)
        endTime := time.Now()
        tweeturls := ts.IntervalUrls(startTime, endTime)
        fmt.Printf("%d urls within time limit\n", len(tweeturls))
        for _, u := range tweeturls {
            fmt.Printf("%s = %s = %s\n", u.Url, u.Expanded_url, u.Display_url)
        }
        fmt.Printf("%d urls within time limit\n", len(tweeturls))

    }
}
*/
