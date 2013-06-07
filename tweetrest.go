package main

import (
    "bufio"
    "bytes"
    "encoding/json"
    "fmt"
    "github.com/fcheslack/webtypes/twitter"
    "github.com/kurrik/oauth1a"
    "io/ioutil"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

type TwitterClient struct {
    HttpClient    *http.Client
    Service       *oauth1a.Service
    UserConfig    *oauth1a.UserConfig
    StreamBackoff time.Duration
    RestBackoff   time.Duration
}

func (trc *TwitterClient) FillSearch(search string, sinceId int64) (results []*twittertypes.Tweet) {
    endPoint := "https://api.twitter.com/1.1/search/tweets.json"
    q := search
    v := url.Values{}
    v.Set("count", "100")
    v.Set("result_type", "recent")
    v.Set("q", q)

    if sinceId != 0 {
        v.Set("since_id", strconv.FormatInt(sinceId, 10))
    }
    /*
       for key, val := range params {
           v.Set(key, val)
       }
    */
    reqUrl, _ := url.Parse(endPoint)
    reqUrl.RawQuery = v.Encode()

    for i := 0; i < 5; i++ {
        fmt.Printf("Requesting: %s\n", reqUrl.String())
        httpRequest, _ := http.NewRequest("GET", reqUrl.String(), nil)
        fmt.Printf("http Requesting: %s\n", httpRequest.URL.String())
        trc.Service.Sign(httpRequest, trc.UserConfig)

        resp, err := trc.HttpClient.Do(httpRequest)
        if err != nil {
            fmt.Printf("Error making request: %v\n", err)
            os.Exit(1)
        }
        body, err := ioutil.ReadAll(resp.Body)
        resp.Body.Close()
        //check if response is ok
        if resp.StatusCode != 200 {
            //something went wrong, possibly rate-limit
            fmt.Printf("Non-200 response from search rest call: %d : %s\n", resp.StatusCode, body)
        }

        fmt.Printf("Got Rest Search Response\n")
        searchresult := &twittertypes.SearchResult{}
        _ = json.Unmarshal(body, searchresult)
        for i, _ := range searchresult.Statuses {
            //fmt.Printf("%s: %s\n", t.User.Screen_name, t.Text)
            results = append(results, &searchresult.Statuses[i])
        }

        fmt.Printf("%+v\n\n", searchresult.Search_metadata)
        //if the search metadata has next_results go get them
        nextResults := searchresult.Search_metadata.Next_results
        if nextResults == "" {
            fmt.Printf("Empty nextResults - stopping fill\n")
            break
        } else {
            fmt.Printf("Fetch More Results: %s\n", nextResults)
            nv, err := url.ParseQuery(nextResults[1:])
            if err != nil {
                fmt.Printf("Error parsing nextResults query")
                break
            }
            reqUrl.RawQuery = nv.Encode()
        }
    }

    return
}

func (trc *TwitterClient) FillUserTimeline(screen_name string, sinceId int64) (results []*twittertypes.Tweet) {
    endPoint := "https://api.twitter.com/1.1/statuses/user_timeline.json"
    v := url.Values{}
    v.Set("count", "200")
    v.Set("screen_name", screen_name)
    v.Set("include_rts", "1")

    if sinceId != 0 {
        v.Set("since_id", strconv.FormatInt(sinceId, 10))
    }
    /*
       for key, val := range params {
           v.Set(key, val)
       }
    */
    reqUrl, _ := url.Parse(endPoint)
    reqUrl.RawQuery = v.Encode()

    for i := 0; i < 5; i++ {
        fmt.Printf("Requesting: %s\n", reqUrl.String())
        httpRequest, _ := http.NewRequest("GET", reqUrl.String(), nil)
        fmt.Printf("http Requesting: %s\n", httpRequest.URL.String())
        trc.Service.Sign(httpRequest, trc.UserConfig)

        resp, err := trc.HttpClient.Do(httpRequest)
        if err != nil {
            fmt.Printf("Error making request: %v\n", err)
            os.Exit(1)
        }
        body, err := ioutil.ReadAll(resp.Body)
        resp.Body.Close()
        //check if response is ok
        if resp.StatusCode != 200 {
            //something went wrong, possibly rate-limit
            fmt.Printf("Non-200 response from usertimeline rest call: %d : %s\n", resp.StatusCode, body)
        }

        fmt.Printf("Got Rest Timeline Response\n")
        timelineResults := make([]*twittertypes.Tweet, 0, 200)
        _ = json.Unmarshal(body, &timelineResults)
        for i, _ := range timelineResults {
            //fmt.Printf("%s: %s\n", t.User.Screen_name, t.Text)
            results = append(results, timelineResults[i])
        }

        fmt.Printf("Got %d tweets from timeline for %s\n\n", len(timelineResults), screen_name)

        if len(timelineResults) > 0 {
            oldestTweetId := timelineResults[len(timelineResults)-1].Id
            nextMaxId := int64(*oldestTweetId) - 1
            v.Set("max_id", strconv.FormatInt(nextMaxId, 10))
            reqUrl.RawQuery = v.Encode()
        } else {
            return
        }
    }

    return
}

func (trc *TwitterClient) FillHomeTimeline(sinceId int64) (results []*twittertypes.Tweet) {
    endPoint := "https://api.twitter.com/1.1/statuses/home_timeline.json"
    v := url.Values{}
    v.Set("count", "200")
    v.Set("include_entities", "1")

    if sinceId != 0 {
        v.Set("since_id", strconv.FormatInt(sinceId, 10))
    }
    /*
       for key, val := range params {
           v.Set(key, val)
       }
    */

    reqUrl, _ := url.Parse(endPoint)
    reqUrl.RawQuery = v.Encode()

    for i := 0; i < 5; i++ {
        fmt.Printf("Requesting: %s\n", reqUrl.String())
        httpRequest, _ := http.NewRequest("GET", reqUrl.String(), nil)
        fmt.Printf("http Requesting: %s\n", httpRequest.URL.String())
        trc.Service.Sign(httpRequest, trc.UserConfig)

        resp, err := trc.HttpClient.Do(httpRequest)
        if err != nil {
            fmt.Printf("Error making request: %v\n", err)
            os.Exit(1)
        }
        body, err := ioutil.ReadAll(resp.Body)
        resp.Body.Close()
        //check if response is ok
        if resp.StatusCode != 200 {
            //something went wrong, possibly rate-limit
            fmt.Printf("Non-200 response from hometimeline rest call: %d : %s\n", resp.StatusCode, body)
        }
        fmt.Printf("Got Rest Home Timeline Response\n")
        timelineResults := make([]*twittertypes.Tweet, 0, 200)
        err = json.Unmarshal(body, &timelineResults)
        if err != nil {
            fmt.Printf("Error unmarshalling home timeline result: %s\n", err)
            fmt.Printf(string(body))
            os.Exit(1)
        }
        for i, _ := range timelineResults {
            //fmt.Printf("%s: %s\n", t.User.Screen_name, t.Text)
            results = append(results, timelineResults[i])
        }

        fmt.Printf("Got %d tweets from home timeline\n\n", len(timelineResults))

        if len(timelineResults) > 0 {
            oldestTweet := timelineResults[len(timelineResults)-1]
            oldestTweetId := oldestTweet.Id
            nextMaxId := int64(*oldestTweetId) - 1
            v.Set("max_id", strconv.FormatInt(nextMaxId, 10))
            reqUrl.RawQuery = v.Encode()
        } else {
            return
        }
    }

    return
}

func (trc *TwitterClient) VerifyCredentials() bool {
    endPoint := "https://api.twitter.com/1.1/account/verify_credentials.json"
    httpRequest, _ := http.NewRequest("GET", endPoint, nil)
    trc.Service.Sign(httpRequest, trc.UserConfig)
    resp, err := trc.HttpClient.Do(httpRequest)
    if err != nil {
        fmt.Printf("Error making request: %v\n", err)
        os.Exit(1)
    }
    defer resp.Body.Close()
    body, err := ioutil.ReadAll(resp.Body)
    resp.Body.Close()
    _ = body
    if resp.StatusCode == 200 {
        return true
    }
    return false
}

func (trc *TwitterClient) StartUserStream(track []string) (*http.Response, error) {
    //user stream
    endPoint := "https://userstream.twitter.com/1.1/user.json"
    v := url.Values{}
    v.Set("with", "followings")
    v.Set("track", strings.Join(track, ","))
    //v.Set("replies", "all")

    reqUrl, _ := url.Parse(endPoint)
    reqUrl.RawQuery = v.Encode()

    httpRequest, _ := http.NewRequest("GET", reqUrl.String(), nil)
    trc.Service.Sign(httpRequest, trc.UserConfig)
    return trc.HttpClient.Do(httpRequest)
}

func (trc *TwitterClient) MaintainUserStream(track []string, linechan chan []byte) {
    for {
        resp, err := trc.StartUserStream(track)
        if err != nil {

        }
        switch {
        case resp.StatusCode == 200:
            trc.StreamBackoff = 0
            ReadHttpStream(resp, linechan)
        case resp.StatusCode == 420 || resp.StatusCode == 503:
            if trc.StreamBackoff == 0 {
                trc.StreamBackoff = 5
            } else {
                trc.StreamBackoff = trc.StreamBackoff * 2
            }
            time.Sleep(trc.StreamBackoff * time.Second)
        default:
            //something is wrong that won't be fixed by retrying, so bail out
            fmt.Printf("Error response: %s\n", resp.Status)
            close(linechan) //close the channel so receiver knows we can't continue
            return
        }
    }
}

func ReadHttpStream(resp *http.Response, linechan chan []byte) {
    defer resp.Body.Close()
    var reader *bufio.Reader
    reader = bufio.NewReader(resp.Body)

    for {
        line, err := reader.ReadBytes('\n')
        if err != nil {
            fmt.Printf("Error reading line: %v\n", err)
            break
        }
        line = bytes.TrimSpace(line)
        if len(line) == 0 {
            now := time.Now()
            fmt.Printf("%s Read line with length of 0\n", now.Format("03:04:05"))
            continue
        }
        linechan <- line
    }
}
