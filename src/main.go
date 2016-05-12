package main;

import (
    "log"
    "data_file"
    "io/ioutil"
    "strings"
    "os"
    "time"
    "gopkg.in/mgo.v2"
    "gopkg.in/mgo.v2/bson"
    "strconv"
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

func filterMac(mac string) string {
    return strings.ToLower(strings.Replace(strings.Replace(mac, "-", "", -1), ":", "", -1))
}

func UpdateApData(data []map[string]string)  {
    c := GetDBSession().DB("detector").C("detector_info")
    for i, fields := range data {
        mac := fields["AP_MAC"]
        mac = filterMac(mac)
        lng, err1 := strconv.ParseFloat(fields["LONGITUDE"], 64)
        lat, err2 := strconv.ParseFloat(fields["LATITUDE"], 64)
        if err1 != nil || err2 != nil {
            continue
        }
        log.Println(i,": mac", mac, "lng", lng, "lat", lat)
        //continue
        c.UpsertId(mac, bson.M{"_id":mac, "longitude":lng, "latitude": lat, "last_active_time": uint32(time.Now().Unix()),"company":"02"})
    }
}

func SaveDeviceInfo(data []map[string]string)  {
    c := GetDBSession().DB("person_info").C("mac")
    for i, fields := range data {
        mac := fields["MAC"]
        mac = filterMac(mac)
        ap_mac := fields["ACCESS_AP_MAC"]
        ap_mac = filterMac(ap_mac)
        authType := fields["AUTH_TYPE"]
        authAccount := fields["AUTH_ACCOUNT"]
        time, err3 := strconv.Atoi(fields["START_TIME"])
        if err3 != nil {
            continue
        }
        log.Println(i,": mac", mac, "ap_mac", ap_mac, "auth_type", authType, "account", authAccount, "time", time)
        //continue
        if authType == "1020004" {
            c.Upsert(bson.M{"mac":mac, "phone":authAccount}, bson.M{"mac":mac, "phone":authAccount, "time":uint32(time)})
        }
    }
}

func SaveTraceInfo(data []map[string]string)  {
    c := GetDBSession().DB("person_info").C("mac")
    for i, fields := range data {
        mac := fields["MAC"]
        mac = filterMac(mac)
        ap_mac := fields["ACCESS_AP_MAC"];
        ap_mac = filterMac(ap_mac)
        lng, err1 := strconv.ParseFloat(fields["COLLECTION_EQUIPMENT_LONGITUDE"], 64)
        lat, err2 := strconv.ParseFloat(fields["COLLECTION_EQUIPMENT_LATITUDE"], 64)
        time, err3 := strconv.Atoi(fields["CAPTURE_TIME"])
        if err1 != nil || err2 != nil || err3 != nil {
            continue
        }

        log.Println(i,": mac", mac, "ap_mac", ap_mac, "lng", lng, "lat", lat, "time", time)
        //continue
        c.Insert(bson.M{"ap_mac":ap_mac, "device_mac":mac, "longitude":lng, "latitude": lat, "time":uint32(time)})
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
    InitDB()
    dirPath := "d:\\1"
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
