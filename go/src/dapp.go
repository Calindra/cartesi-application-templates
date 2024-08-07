package main

import (
  "encoding/json"
  "io/ioutil"
  "strconv"
  "fmt"
  "log"
  "os"
  "net/http"
  "dapp/rollups"
  "bytes"
)

var (
  infolog  = log.New(os.Stderr, "[ info ]  ", log.Lshortfile)
  errlog   = log.New(os.Stderr, "[ error ] ", log.Lshortfile)
)

func HandleAdvance(data *rollups.AdvanceResponse) error {
  dataMarshal, err := json.Marshal(data)
  if err != nil {
    return fmt.Errorf("HandleAdvance: failed marshaling json: %w", err)
  }
  infolog.Println("Received advance request data", string(dataMarshal))
  
  lambada_server_url, _ := os.LookupEnv("LAMBADA_HTTP_SERVER_URL")
  open_state_url := fmt.Sprintf("%s/open_state", lambada_server_url)
  response, err := http.Get(open_state_url)

  if err != nil {
    log.Fatalf("Failed to open state: %v", err)
  }
  defer response.Body.Close()
  fmt.Println("State opened successfully.")

  set_state_url := fmt.Sprintf("%s/set_state/output", lambada_server_url)
  response, err = http.Post(set_state_url, "application/octet-stream", bytes.NewBuffer([]byte("hello world")))

  if err != nil {
    log.Fatalf("Failed to set state: %v", err)
  }
  defer response.Body.Close()
  fmt.Println("State set successfully.")

  commit_state_url := fmt.Sprintf("%s/commit_state", lambada_server_url)
  response, err = http.Get(commit_state_url)

  if err != nil {
    log.Fatalf("Failed to commit state: %v", err)
  }
  defer response.Body.Close()

  return nil
}


func HandleInspect(data *rollups.InspectResponse) error {
  dataMarshal, err := json.Marshal(data)
  if err != nil {
    return fmt.Errorf("HandleInspect: failed marshaling json: %w", err)
  }
  infolog.Println("Received inspect request data", string(dataMarshal))
  return nil
}

func Handler(response *rollups.FinishResponse) error {
  var err error

  switch response.Type {
  case "advance_state":
    data := new(rollups.AdvanceResponse)
    if err = json.Unmarshal(response.Data, data); err != nil {
      return fmt.Errorf("Handler: Error unmarshaling advance:", err)
    }
    err = HandleAdvance(data)
  case "inspect_state":
    data := new(rollups.InspectResponse)
    if err = json.Unmarshal(response.Data, data); err != nil {
      return fmt.Errorf("Handler: Error unmarshaling inspect:", err)
    }
    err = HandleInspect(data)
  }
  return err
}

func main() {
  finish := rollups.FinishRequest{"accept"}

  for true {
    infolog.Println("Sending finish")
    res, err := rollups.SendFinish(&finish)
    if err != nil {
      errlog.Panicln("Error: error making http request: ", err)
    }
    infolog.Println("Received finish status ", strconv.Itoa(res.StatusCode))
    
    if (res.StatusCode == 202){
      infolog.Println("No pending rollup request, trying again")
    } else {

      resBody, err := ioutil.ReadAll(res.Body)
      if err != nil {
        errlog.Panicln("Error: could not read response body: ", err)
      }
      
      var response rollups.FinishResponse
      err = json.Unmarshal(resBody, &response)
      if err != nil {
        errlog.Panicln("Error: unmarshaling body:", err)
      }

      finish.Status = "accept"
      err = Handler(&response)
      if err != nil {
        errlog.Println(err)
        finish.Status = "reject"
      }
    }
  }
}