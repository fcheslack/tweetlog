package tweetstore

import (
    "database/sql"
    "encoding/json"
    "fmt"
    "github.com/fcheslack/webtypes/twitter"
    _ "github.com/mattn/go-sqlite3"
    "time"
)

type TweetStore interface {
    BeginTransaction()
    CommitTransaction()
    RollbackTransaction()
    SaveTweet(*twittertypes.Tweet) error
    /*
       SaveMedia(*twittertypes.Tweet, *twittertypes.Media) err
       SaveMention(*twittertypes.Tweet, *twittertypes.Mention) err
       SaveUrl(*twittertypes.Tweet, *twittertypes.TwitterUrl) err
       SaveHashtag(*twittertypes.Tweet, *twittertypes.Hashtag) err
       SaveEntities(*twittertypes.Tweet) err
       SaveEvent(*twittertypes.Event) err
       LoadTweet(int64) (*twittertypes.Tweet, err)
       //LoadEvent()
       LoadRecent() ([]*twittertypes.Tweet, err)
       LoadSince(int64) ([]*twittertypes.Tweet, err)
       LoadOlder(int64) ([]*twittertypes.Tweet, err)
       Search(string) ([]*twittertypes.Tweet, err)
       SaveNormalized(*twittertypes.Tweet) err
    */
}

type SqliteTweetStore struct {
    DB        *sql.DB
    CurrentTx *sql.Tx //pointer to current in progress transaction, may be nil
}

func (sts *SqliteTweetStore) BeginTransaction() error {
    if sts.CurrentTx == nil {
        currentTX, err := sts.DB.Begin()
        if err != nil {
            fmt.Printf("Error beginning SQLite transaction\n%s\n", err)
        } else {
            sts.CurrentTx = currentTX
        }
    }
    return nil
}

func (sts *SqliteTweetStore) CommitTransaction() error {
    if sts.CurrentTx != nil {
        err := sts.CurrentTx.Commit()
        if err != nil {
            fmt.Printf("Error committing SQLite transaction\n%s\n", err)
        } else {
            sts.CurrentTx = nil
        }
    }
    return nil
}

func (sts *SqliteTweetStore) RollbackTransaction() error {
    if sts.CurrentTx != nil {
        err := sts.CurrentTx.Rollback()
        if err != nil {
            fmt.Printf("Error Rolling back SQLite transaction\n%s\n", err)
        } else {
            sts.CurrentTx = nil
        }
    }
    return nil
}

func (sts *SqliteTweetStore) GetOrStartTransaction() (*sql.Tx, bool) {
    ownTransaction := false
    if sts.CurrentTx == nil {
        ownTransaction = true
        err := sts.BeginTransaction()
        if err != nil {
            //TODO: badness
        }
    }

    return sts.CurrentTx, ownTransaction
}

func (sts *SqliteTweetStore) Initialize(db interface{}) (bool, error) {
    sts.DB = db.(*sql.DB)
    sqls := []string{
        "CREATE TABLE IF NOT EXISTS tweets (tweetid INTEGER PRIMARY KEY, screen_name, time, text, fulltweet);",
        "CREATE TABLE IF NOT EXISTS normtweets (tweetid INTEGER PRIMARY KEY, screen_name, created_at, text, in_reply_to_user_id, in_reply_to_screen_name, source, in_reply_to_status_id, fulltweet);",
        "CREATE TABLE IF NOT EXISTS tweettimestamps (tweetid INTEGER PRIMARY KEY, timestamp);",
        "CREATE INDEX IF NOT EXISTS tweettimeind ON tweettimestamps (timestamp);",
        "CREATE TABLE IF NOT EXISTS streamevents (eventid INTEGER PRIMARY KEY ASC, eventtype, object);",
        "CREATE TABLE IF NOT EXISTS media (mediaid, tweetid, expanded_url, type, object, UNIQUE (mediaid, tweetid));",
        "CREATE TABLE IF NOT EXISTS user_mentions (userid, tweetid, screen_name, name, object, UNIQUE(userid, tweetid));",
        "CREATE TABLE IF NOT EXISTS urls (expanded_url, tweetid, url, object, UNIQUE (expanded_url, tweetid));",
        "CREATE TABLE IF NOT EXISTS hashtags (text, tweetid, object, UNIQUE (text, tweetid));",
        //"DROP TABLE IF EXISTS tweetsearch;",
        //"CREATE VIRTUAL TABLE tweetsearch USING fts3(tweetid, tweettext); INSERT INTO tweetsearch (tweetid, tweettext) SELECT tweetid, text FROM tweets;",
    }

    for _, sql := range sqls {
        _, err := sts.DB.Exec(sql)
        if err != nil {
            fmt.Printf("%q: %s\n", err, sql)
            return false, err
        }
    }
    return true, nil
}

func (sts *SqliteTweetStore) SaveTweet(tweet *twittertypes.Tweet) error {
    tx, ownTx := sts.GetOrStartTransaction()
    storetweetq := "INSERT OR REPLACE INTO tweets (tweetid, screen_name, time, text, fulltweet) VALUES (?, ?, ?, ?, ?);"
    storetimestampq := "INSERT OR REPLACE INTO tweettimestamps (tweetid, timestamp) VALUES (?, ?);"
    storesearchableq := "INSERT INTO tweetsearch (tweetid, tweettext) VALUES (?, ?);"

    created_at, err := time.Parse(time.RubyDate, tweet.Created_at)
    if err != nil {
        fmt.Printf("Error parsing created_at time:%s\n", err)
    }
    raw := tweet.RawBytes
    if raw == nil {
        raw, err = json.Marshal(tweet)
        if err != nil {
            fmt.Printf("No tweet.RawBytes and error marshalling tweet: %s\n", err)
        }
    }
    res, err := tx.Exec(storetweetq, tweet.Id, tweet.User.Screen_name, created_at, tweet.Text, raw)
    if err != nil {
        fmt.Printf("Error inserting tweet: %s\n", err)
        //reterr = err
    } else {
        r, _ := res.RowsAffected()
        if r == 0 {
            fmt.Printf("0 rows affected by insert - something is probably wrong")
        }
        //fmt.Printf("Insert tweet rows affected: %d\n", r)
    }

    _, err = tx.Exec(storetimestampq, tweet.Id, created_at.Unix())
    if err != nil {
        //reterr = err
    }

    _ = storesearchableq
    //res, err = sts.DB.Exec(storesearchableq, tweet.Id, tweet.Text)
    if err != nil {
        fmt.Printf("Error storing searchable: %s\n", err)
        //reterr = err
    }
    //r, _ := res.RowsAffected()
    //fmt.Printf("%d rows affected by store searchable\n", r)

    //_ = reterr
    sts.SaveEntities(tweet)

    if ownTx {
        err = sts.CommitTransaction()
    }
    return nil // reterr
}

func (sts *SqliteTweetStore) SaveTweets(tweets []*twittertypes.Tweet) error {
    _, ownTx := sts.GetOrStartTransaction()
    for _, t := range tweets {
        err := sts.SaveTweet(t)
        if err != nil {
            fmt.Printf("Error saving tweet from batch: %s\n", err)
        }
    }
    if ownTx {
        err := sts.CommitTransaction()
        if err != nil {
            fmt.Printf("Error commiting saveTweets transaction: %s\n", err)
        }
    }
    return nil // reterr
}

func (sts *SqliteTweetStore) SaveEntities(tweet *twittertypes.Tweet) error {
    tx, ownTx := sts.GetOrStartTransaction()

    insertmediaq := "INSERT OR REPLACE INTO media VALUES (?, ?,?,?,?);"
    insertusermentionq := "INSERT OR REPLACE INTO user_mentions VALUES (?, ?,?,?,?);"
    inserturlq := "INSERT OR REPLACE INTO urls VALUES (?, ?,?,?);"
    inserthashq := "INSERT OR REPLACE INTO hashtags VALUES (?, ?,?);"

    for _, media := range tweet.Entities.Media {
        j, _ := json.Marshal(media)
        _, err := tx.Exec(insertmediaq, media.Id, tweet.Id, media.Expanded_url, media.Type, j)
        if err != nil {
            fmt.Printf("Error inserting media: %s\n", err)
        }
    }

    for _, mention := range tweet.Entities.User_mentions {
        j, _ := json.Marshal(mention)
        n := string(mention.Name)
        _, err := tx.Exec(insertusermentionq, mention.Id, tweet.Id, mention.Screen_name, n, j)
        if err != nil {
            fmt.Printf("Error inserting mention: %s\n", err)
        }
    }

    for _, turl := range tweet.Entities.Urls {
        j, _ := json.Marshal(turl)
        _, err := tx.Exec(inserturlq, string(turl.Expanded_url), tweet.Id, turl.Url, j)
        if err != nil {
            fmt.Printf("Error inserting url: %s\n", err)
        }
    }

    for _, ht := range tweet.Entities.Hashtags {
        j, _ := json.Marshal(ht)
        _, err := tx.Exec(inserthashq, ht.Text, tweet.Id, j)
        if err != nil {
            fmt.Printf("Error inserting hashtag: %s\n", err)
        }
    }

    if ownTx {
        err := sts.CommitTransaction()
        if err != nil {
            fmt.Printf("Error commiting saveEntities TX: %s\n", err)
        }
    }

    return nil
}

func (sts *SqliteTweetStore) SaveEvent(event *twittertypes.Event) error {
    storeeventq := "INSERT INTO streamevents (eventtype, object) VALUES (?, ?);"
    _ = storeeventq
    return nil
}

func (sts *SqliteTweetStore) LatestTweetId() int64 {
    lastIdq := "SELECT tweetid FROM tweets ORDER BY tweetid DESC LIMIT 1;"
    row := sts.DB.QueryRow(lastIdq)
    var lastId int64
    err := row.Scan(&lastId)
    if err != nil {
        fmt.Println("Error getting last tweet id: %s\n", err)
        return 0
    }
    return lastId
}

func (sts *SqliteTweetStore) RecentTweets(count int) []*twittertypes.Tweet {
    var tweets = make([]*twittertypes.Tweet, 0, count)
    recenttweetsq := "SELECT tweetid, fulltweet FROM tweets ORDER BY tweetid DESC LIMIT ?;"
    rows, err := sts.DB.Query(recenttweetsq, count)
    if err != nil {
        fmt.Printf("Error getting tweets after id: %s\n", err)
        return nil
    }
    for rows.Next() {
        var tweetid int64
        var tweetstring []byte
        err = rows.Scan(&tweetid, &tweetstring)
        if err != nil {
            fmt.Printf("Error scanning tweetafterid row: %s\n", err)
            continue
        }
        var tweet = &twittertypes.Tweet{}
        err = json.Unmarshal(tweetstring, tweet)
        if err != nil {
            fmt.Printf("Error unmarshalling tweetafterid row: %s\n", err)
            fmt.Printf("Problematic tweet: %d \n%s\n", tweetid, string(tweetstring))
            var z twittertypes.Int64Nullable
            z = 0
            tweet.Id = &z
            tweet.User = &twittertypes.User{Screen_name: "Fail"}
        } else {
            //fmt.Printf("Got tweet\n")
        }
        tweets = append(tweets, tweet)
    }
    err = rows.Err() // get any error encountered during iterationerr := row.Scan(&lastId)
    return tweets
}

func (sts *SqliteTweetStore) TweetsAfterId(tweetid int64) []*twittertypes.Tweet {
    var tweets = make([]*twittertypes.Tweet, 0, 200)
    tweetsafterq := "SELECT tweetid, fulltweet FROM tweets WHERE tweetid > ? ORDER BY tweetid DESC;"
    rows, err := sts.DB.Query(tweetsafterq, tweetid)
    if err != nil {
        fmt.Printf("Error getting tweets after id: %s\n", err)
        return nil
    }
    for rows.Next() {
        var tweetid int64
        var tweetstring []byte
        err = rows.Scan(&tweetid, &tweetstring)
        if err != nil {
            fmt.Printf("Error scanning tweetafterid row: %s\n", err)
            continue
        }
        var tweet = &twittertypes.Tweet{}
        err = json.Unmarshal(tweetstring, tweet)
        if err != nil {
            fmt.Printf("Error unmarshalling tweetafterid row: %s\n", err)
            fmt.Printf("Problematic tweet: %d \n%s\n", tweetid, string(tweetstring))
            var z twittertypes.Int64Nullable
            z = 0
            tweet.Id = &z
            tweet.User = &twittertypes.User{Screen_name: "Fail"}
        } else {
            fmt.Printf("Got tweet\n")
        }
        tweets = append(tweets, tweet)
    }
    err = rows.Err() // get any error encountered during iterationerr := row.Scan(&lastId)
    return tweets
}

//Get all the urls (according to twitter, so this excludes explicit media) posted between startTime and endTime
func (sts *SqliteTweetStore) IntervalUrls(startTime time.Time, endTime time.Time) []*twittertypes.TwitterUrl {
    var urls = make([]*twittertypes.TwitterUrl, 0, 200)
    urlquery := "SELECT tweets.tweetid, urls.object FROM tweets JOIN urls ON tweets.tweetid = urls.tweetid WHERE tweets.time > ? AND tweets.time < ? ORDER BY tweets.tweetid DESC;"
    rows, err := sts.DB.Query(urlquery, startTime, endTime)
    if err != nil {
        fmt.Printf("Error getting interval urls: %s\n", err)
        return nil
    }
    for rows.Next() {
        var tweetid int64
        var tweeturlstring []byte
        err = rows.Scan(&tweetid, &tweeturlstring)
        if err != nil {
            fmt.Printf("Error scanning tweeturl row: %s\n", err)
            continue
        }
        var tweeturl = &twittertypes.TwitterUrl{}
        err = json.Unmarshal(tweeturlstring, tweeturl)
        if err != nil {
            fmt.Printf("Error unmarshalling tweeturl row: %s\n", err)
            fmt.Printf("Problematic tweet: %d \n%s\n", tweetid, string(tweeturlstring))
        } else {
            fmt.Printf("Got tweeturl\n")
        }
        urls = append(urls, tweeturl)
    }
    err = rows.Err() // get any error encountered during iterationerr := row.Scan(&lastId)
    return urls
}

//Get all the tweets posted between startTime and endTime
func (sts *SqliteTweetStore) IntervalTweets(startTime time.Time, endTime time.Time) []*twittertypes.Tweet {
    var tweets = make([]*twittertypes.Tweet, 0, 200)
    urlquery := "SELECT tweetid, fulltweet FROM tweets WHERE tweets.time > ? AND tweets.time < ? ORDER BY tweets.tweetid DESC;"
    rows, err := sts.DB.Query(urlquery, startTime, endTime)
    if err != nil {
        fmt.Printf("Error getting interval urls: %s\n", err)
        return nil
    }
    for rows.Next() {
        var tweetid int64
        var tweetstring []byte
        err = rows.Scan(&tweetid, &tweetstring)
        if err != nil {
            fmt.Printf("Error scanning tweetafterid row: %s\n", err)
            continue
        }
        var tweet = &twittertypes.Tweet{}
        err = json.Unmarshal(tweetstring, tweet)
        if err != nil {
            fmt.Printf("Error unmarshalling intervalTweetRow row: %s\n", err)
            fmt.Printf("Problematic tweet: %d \n%s\n", tweetid, string(tweetstring))
        }
        tweets = append(tweets, tweet)
    }
    err = rows.Err() // get any error encountered during iterationerr := row.Scan(&lastId)
    return tweets
}

func (sts *SqliteTweetStore) IntervalTweetCount(intervalDuration time.Duration, numIntervals int) []int {
    rowCounts := make([]int, numIntervals)
    lastHourq := "SELECT COUNT(tweetid) FROM tweets WHERE time > ? AND time < ?;"
    for i := 0; i < numIntervals; i++ {
        after := time.Now().Add(-1 * time.Duration(i) * intervalDuration)
        before := time.Now().Add(-1 * time.Duration(i+1) * intervalDuration)
        row := sts.DB.QueryRow(lastHourq, before, after)
        err := row.Scan(&rowCounts[i])
        if err != nil {
            fmt.Printf("Error scanning tweet count row: %s\n", err)
        }
    }
    return rowCounts
}

func (sts *SqliteTweetStore) Query(query string, args ...interface{}) *sql.Rows {
    rows, err := sts.DB.Query(query, args...)
    if err != nil {
        fmt.Printf("Error performing query %s : %s\n\n", query, err)
        return nil
    }

    return rows
}
