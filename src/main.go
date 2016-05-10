package main;

import (
    "log"
    "data_file"
    "fmt"
    "io/ioutil"
    "strings"
    "os"
    "time"
)

//var session *mgo.Session;

//func InitDB()  {
//    var err error
//    session, err = mgo.Dial("112.74.90.113:22522")
//    if err != nil {
//        panic(err)
//    }
//    session.SetMode(mgo.Monotonic, true)
//    log.Println("connect to db succ")
//}
//
//
//func GetDBSession() *mgo.Session {
//    return session
//}

func SaveData(data []map[string]string)  {
    for i, fields := range data {
        //mac := fields["PLACE_NAME"]
        //time := fields["CAPTURE_TIME"]
        //log.Println(mac,time)
        fmt.Println("No.",i, " ")
        for k,v := range fields {
            fmt.Print(k, ":", v, ", ")
        }
        fmt.Println("")

    }
}

func ProcDir(dirPath string)  {
    files := make([]string, 0)
    dir, err := ioutil.ReadDir(dirPath)
    if err != nil {
        return
    }
    PthSep := string(os.PathSeparator)
    for _, f := range dir {
        if f.IsDir() {
            continue
        }
        if strings.HasPrefix(f.Name(), "~") {
            continue
        }
        files = append(files, dirPath + PthSep + f.Name())
    }
    for _, filePath := range files {
        zipFile := data_file.DataFile{}
        err := zipFile.Load(filePath)
        if err != nil {
            log.Println(err)
            os.Remove(filePath)
            continue
        }
        log.Println(zipFile.Fields)
        SaveData(zipFile.Fields)
        os.Remove(filePath)
    }
}

func main() {
    dirPath := "e:\\1"
    for {
        ProcDir(dirPath)
        time.Sleep(time.Second)
    }

    //InitDB()
    //fileList, err := unzip("")
    //if err != nil {
    //    log.Println("unzip err,", err)
    //    return
    //}
    //for _, xmlContent := range fileList{
    //    xmlInfo := XmlMeta{}
    //    err = xml.Unmarshal(xmlContent, &xmlInfo)
    //    if err != nil {
    //        log.Println("parse xml err,", err)
    //        continue
    //    }
    //    log.Println(xmlInfo)
    //    xmlInfo.save()
    //}

}
