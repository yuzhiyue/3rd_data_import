package main;

import (
    "log"
    "data_file"
    "io/ioutil"
    "strings"
    "os"
    "time"
    "gopkg.in/mgo.v2"
)

var session *mgo.Session;

func InitDB()  {
    var err error
    session, err = mgo.Dial("112.74.90.113:22522")
    if err != nil {
        panic(err)
    }
    session.SetMode(mgo.Monotonic, true)
    log.Println("connect to db succ")
}


func GetDBSession() *mgo.Session {
    return session
}

func PrintData(data []map[string]string)  {
    for i, fields := range data {
        log.Println(i, fields)
    }
}

func UpdateApData(data []map[string]string)  {
    for i, fields := range data {
        mac := fields["AP_MAC"]
        lng := fields["LONGITUDE"]
        lat := fields["LATITUDE"]
        log.Println(i,": mac", mac, "lng", lng, "lat", lat)
    }
}

func SaveDeviceInfo(data []map[string]string)  {
    for i, fields := range data {
        mac := fields["MAC"]
        ap_mac := fields["ACCESS_AP_MAC"]
        authType := fields["AUTH_TYPE"]
        authAccount := fields["AUTH_ACCOUNT"]
        time := fields["START_TIME"]
        log.Println(i,": mac", mac, "ap_mac", ap_mac, "auth_type", authType, "account", authAccount, "time", time)
    }
}

func SaveTraceInfo(data []map[string]string)  {
    for i, fields := range data {
        mac := fields["MAC"]
        ap_mac := fields["ACCESS_AP_MAC"];
        lng := fields["COLLECTION_EQUIPMENT_LONGITUDE"]
        lat := fields["COLLECTION_EQUIPMENT_LATITUDE"]
        time := fields["CAPTURE_TIME"]
        log.Println(i,": mac", mac, "ap_mac", ap_mac, "lng", lng, "lat", lat, "time", time)
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
        if strings.Contains(zipFile.Meta.FileName, "WA_BASIC_FJ_0003") {
            UpdateApData(zipFile.Fields)
        } else if strings.Contains(zipFile.Meta.FileName, "WA_SOURCE_FJ_1001") {
            SaveTraceInfo(zipFile.Fields)
        } else if strings.Contains(zipFile.Meta.FileName, "WA_SOURCE_FJ_0001") {
            SaveDeviceInfo(zipFile.Fields)
        } else {
            PrintData(zipFile.Fields)
        }

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
