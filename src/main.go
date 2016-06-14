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

func UpdateApData(orgcode string, data []map[string]string)  {
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
        if saveToDB {
            c.UpsertId(mac, bson.M{"_id":mac, "longitude":lng, "latitude": lat, "last_active_time": uint32(time.Now().Unix()), "company":"02", "org_code":orgcode})
        }
    }
}

func SaveDeviceInfo(orgcode string, data []map[string]string)  {
    c := GetDBSession().DB("person_info").C("mac")
    for i, fields := range data {
        mac := fields["MAC"]
        mac = filterMac(mac)
        ap_mac := fields["AP_MAC"]
        ap_mac = filterMac(ap_mac)
        authType := fields["AUTH_TYPE"]
        authAccount := fields["AUTH_ACCOUNT"]
        time, err3 := strconv.Atoi(fields["START_TIME"])
        if err3 != nil {
            continue
        }
        log.Println(i,": mac", mac, "ap_mac", ap_mac, "auth_type", authType, "account", authAccount, "time", time)

        if saveToDB {
            if authType == "1020004" {
                c.Upsert(bson.M{"mac":mac, "phone":authAccount}, bson.M{"mac":mac, "phone":authAccount, "org_code":orgcode, "time":uint32(time)})
            }
        }
    }
}

func SaveTraceInfo(orgcode string, data []map[string]string)  {
    log.Println("SaveTraceInfo")
    c := GetDBSession().DB("detector").C("detector_report")
    for i, fields := range data {
        log.Println(fields)
        mac := fields["MAC"]
        mac = filterMac(mac)
        ap_mac := fields["AP_MAC"];
        ap_mac = filterMac(ap_mac)
        lng, err1 := strconv.ParseFloat(fields["COLLECTION_EQUIPMENT_LONGITUDE"], 64)
        lat, err2 := strconv.ParseFloat(fields["COLLECTION_EQUIPMENT_LATITUDE"], 64)
        time, err3 := strconv.Atoi(fields["CAPTURE_TIME"])
        if err1 != nil || err2 != nil || err3 != nil {
            continue
        }

        log.Println(i,": mac", mac, "ap_mac", ap_mac, "lng", lng, "lat", lat, "time", time)

        if saveToDB {
            c.Insert(bson.M{"ap_mac":ap_mac, "device_mac":mac, "longitude":lng, "latitude": lat, "org_code":orgcode, "time":uint32(time)})
        }
    }
}

func SaveLog(orgcode string, data []map[string]string)  {
    log.Println("SaveLog")
    c := GetDBSession().DB("person_info").C("log")
    for i, fields := range data {
        log.Println(i,fields)
        if saveToDB {
            c.Insert(fields)
        }
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
        if !strings.HasSuffix(f.Name(), ".zip") {
            continue
        }
        if time.Now().Unix() - f.ModTime().Unix() < 60{
            continue
        }
        files = append(files, f.Name())
    }
    for _, fileName := range files {
        log.Println("start proc", fileName)
        filePath := dirPath + PthSep + fileName
        zipFile := data_file.DataFile{}
        err := zipFile.Load(filePath)
        if err != nil {
            log.Println(err)
            os.Remove(filePath)
            continue
        }
        fileNameSplited := strings.Split(fileName, "-")
        if len(fileNameSplited) < 2 {
            log.Println("fileNameSplited len < 2", fileName)
            os.Remove(filePath)
            continue
        }
        orgCode := fileNameSplited[1]
        log.Println("parse", zipFile.Meta.FileName, orgCode)
        if strings.Contains(zipFile.Meta.FileName, "WA_BASIC_FJ_0003") {
            UpdateApData(orgCode, zipFile.Fields)
        } else if strings.Contains(zipFile.Meta.FileName, "WA_SOURCE_FJ_1001") {
            SaveTraceInfo(orgCode, zipFile.Fields)
        } else if strings.Contains(zipFile.Meta.FileName, "WA_SOURCE_FJ_0001") {
            SaveDeviceInfo(orgCode, zipFile.Fields)
        }else if strings.Contains(zipFile.Meta.FileName, "WA_SOURCE_FJ_0002") {
            SaveLog(orgCode, zipFile.Fields)
        } else {
            PrintData(zipFile.Fields)
        }

        os.Remove(filePath)
    }
}

var saveToDB = true
var dirPath = ""
var loopCount = 1
var openLogFile = true
func main() {
    log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
    if openLogFile {
        logFile, logErr := os.OpenFile("/home/detector/3rd_data_import/3rd_data_import.log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
        if logErr != nil {
            log.Println("Fail to find", "/home/detector/3rd_data_import/3rd_data_import.log", "Server start Failed")
            os.Exit(1)
        }
        log.SetOutput(logFile)
    }

    if saveToDB {
        InitDB()
    }
    if dirPath == "" {
        if len(os.Args) == 2 {
            dirPath = os.Args[1]
        } else {
            return;
        }
    }

    log.Println("read dir", dirPath)

    for i := 0; i < loopCount; i++{
            ProcDir(dirPath)
            time.Sleep(time.Second)
    }
}
