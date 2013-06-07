package main

import (
    "fmt"
    "github.com/kurrik/oauth1a"
    "net/http"
)

var userConfig *oauth1a.UserConfig
var service *oauth1a.Service
var httpClient *http.Client

func main() {
    //set up oauth for signing requests
    service = &oauth1a.Service{
        RequestURL:   "https://api.twitter.com/oauth/request_token",
        AuthorizeURL: "https://api.twitter.com/oauth/authorize",
        AccessURL:    "https://api.twitter.com/oauth/access_token",
        ClientConfig: &oauth1a.ClientConfig{
            ConsumerKey:    "consumerKey",
            ConsumerSecret: "consumerSecret",
            CallbackURL:    "http://localhost.faolancp.com:10000/oacallback",
        },
        Signer: new(oauth1a.HmacSha1Signer),
    }

    httpClient = new(http.Client)
    userConfig = &oauth1a.UserConfig{}
    userConfig.GetRequestToken(service, httpClient)
    url, _ := userConfig.GetAuthorizeURL(service)
    // Redirect the user to <url> and parse out token and verifier from the response.
    fmt.Printf("Go to %s\n", url)

    http.HandleFunc("/oacallback", oacallbackHandler)
    http.ListenAndServe(":10000", nil)
}

func oacallbackHandler(rw http.ResponseWriter, req *http.Request) {
    output := ""

    token, verifier, err := userConfig.ParseAuthorize(req, service)
    if err != nil {
        output += fmt.Sprintf("Error parsing authorization")
    }
    output += fmt.Sprintf("Token: %s \nVerifier: %s \n", token, verifier)

    err = userConfig.GetAccessToken(token, verifier, service, httpClient)
    if err != nil {
        output += fmt.Sprintf("Error getting access token: %s\n", err)
    }

    output += fmt.Sprintf("Access Token: %+v\n", userConfig)
    fmt.Print(output)
    rw.Write([]byte(output))
}
