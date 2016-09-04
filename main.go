package main;

import (
    "log"
    "io/ioutil"
    "strings"
    "os"
    "time"
    "gopkg.in/mgo.v2/bson"
    "strconv"
    "net"
    "3rd_data_import/data_file"
    "unicode"
    "3rd_data_import/db"
    "3rd_data_import/export"
)

func inet_ntoa(ipnr int64) string {
    var bytes [4]byte
    bytes[0] = byte(ipnr & 0xFF)
    bytes[1] = byte((ipnr >> 8) & 0xFF)
    bytes[2] = byte((ipnr >> 16) & 0xFF)
    bytes[3] = byte((ipnr >> 24) & 0xFF)

    return net.IPv4(bytes[3],bytes[2],bytes[1],bytes[0]).String()
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
    c := db.GetDBSession().DB("detector").C("detector_info")
    for i, fields := range data {
        mac := fields["AP_MAC"]
        mac = filterMac(mac)
        lng, err1 := strconv.ParseFloat(fields["LONGITUDE"], 64)
        lat, err2 := strconv.ParseFloat(fields["LATITUDE"], 64)
        if err1 != nil || err2 != nil {
            continue
        }
        lng, lat = data_file.Bd09towgs84(lng, lat)
        log.Println(i,": mac", mac, "lng", lng, "lat", lat)
        if saveToDB {
            c.UpsertId(mac, bson.M{"_id":mac, "longitude":lng, "latitude": lat, "last_active_time": uint32(time.Now().Unix()), "company":"02", "org_code":orgcode})
        }
    }
}

func isPhoneNo(value string) bool {
    if len(value) == 11 && strings.HasPrefix(value, "1") {
        for _, v := range []rune(value) {
            if !unicode.IsDigit(v) {
                return false
            }
        }
        return true
    } else {
        return false
    }
}

func SaveDeviceInfo(orgcode string, data []map[string]string)  {
    c := db.GetDBSession().DB("person_info").C("mac")
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
            } else if authType == "1029999" {
                if isPhoneNo(authAccount) {
                    c.Upsert(bson.M{"mac":mac, "phone":authAccount}, bson.M{"mac":mac, "phone":authAccount, "org_code":orgcode, "time":uint32(time)})
                }
            }
        }
    }
}

func SaveTraceInfo(orgcode string, data []map[string]string)  {
    log.Println("SaveTraceInfo")
    c := db.GetDBSession().DB("detector").C("detector_report")
    for i, fields := range data {
        //log.Println(fields)
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
        lng, lat = data_file.Bd09towgs84(lng, lat)
        log.Println(i,": mac", mac, "ap_mac", ap_mac, "lng", lng, "lat", lat, "time", time)

        if saveToDB {
            c.Insert(bson.M{"ap_mac":ap_mac, "device_mac":mac, "longitude":lng, "latitude": lat, "org_code":orgcode, "time":uint32(time)})
        }
    }
}

func SaveBehaviorLog(orgcode string, data []map[string]string)  {
    log.Println("SaveLog")
    c := db.GetDBSession().DB("person_info").C("behavior_log")
    for i, fields := range data {
        log.Println(i,fields)
        mac := fields["MAC"]
        mac = filterMac(mac)
        IpInt, err4 := strconv.ParseInt(fields["DST_IP"], 10, 64)
        Ip := inet_ntoa(IpInt)
        port := fields["DST_PORT"]
        lng, err1 := strconv.ParseFloat(fields["LONGITUDE"], 64)
        lat, err2 := strconv.ParseFloat(fields["LATITUDE"], 64)
        time, err3 := strconv.Atoi(fields["CAPTURE_TIME"])
        if err1 != nil || err2 != nil || err3 != nil || err4 != nil{
            log.Println("error:", err1, err2, err3, err4)
            continue
        }
        lng, lat = data_file.Bd09towgs84(lng, lat)
        if saveToDB {
            //c.Insert(fields)
            c.Insert(bson.M{"mac": mac, "dst_ip":Ip, "dst_port":port, "longitude":lng, "latitude": lat, "org_code":orgcode, "time":uint32(time)})
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
        for _, bcpFile := range zipFile.BCPFiles {
            log.Println("parse", bcpFile.Meta.FileName, orgCode)
            PrintData(bcpFile.Fields)
            PrintData(bcpFile.KeyFields)
            if strings.Contains(bcpFile.Meta.FileName, "WA_BASIC_FJ_0003") {
                UpdateApData(orgCode, bcpFile.Fields)
            } else if strings.Contains(bcpFile.Meta.FileName, "WA_SOURCE_FJ_1001") {
                SaveTraceInfo(orgCode, bcpFile.Fields)
            } else if strings.Contains(bcpFile.Meta.FileName, "WA_SOURCE_FJ_0001") {
                SaveDeviceInfo(orgCode, bcpFile.Fields)
            }else if strings.Contains(bcpFile.Meta.FileName, "WA_SOURCE_FJ_0002") {
                SaveBehaviorLog(orgCode, bcpFile.Fields)
            } else {

            }
        }
        //os.Remove(filePath)
    }
}

func GetNumber(m bson.M, key string) float64 {
    v := m[key]
    if v == nil {
        return 0
    }
    switch v.(type) {
    case float64:
        return v.(float64)
    case float32:
        return float64(v.(float32))
    case int:
        return float64(v.(int))
    }
    return 0
}

func ConvertGeo()  {
    c := db.GetDBSession().DB("detector").C("detector_report")
    c2 := db.GetDBSession().Copy().DB("detector").C("detector_report");
    query := c.Find(bson.M{"org_code":"555400905"})
    iter := query.Iter()
    e := bson.M{}
    for iter.Next(&e) {
        id,_ := e["_id"]
        idStr := id.(bson.ObjectId)
        log.Println(idStr)
        lng := GetNumber(e, "longitude")
        lat := GetNumber(e, "latitude")
        lng, lat = data_file.Bd09towgs84(lng, lat)
        c2.UpdateId(e["_id"], bson.M{"$set":bson.M{"longitude":lng, "latitude":lat}})
    }
}

var saveToDB = true
var dirPath = ""
var loopCount = 1
var openLogFile = true
func main() {
    log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
    if openLogFile {
        logFile, logErr := os.OpenFile("./3rd_data_import.log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
        if logErr != nil {
            log.Println("Fail to find", "./3rd_data_import.log", "Server start Failed")
            os.Exit(1)
        }
        log.SetOutput(logFile)
    }

    if saveToDB {
        db.InitDB()
        //export.ExportDetectorInfo()
        //export.ExportTrace()
        //return
    }

    if len(os.Args) == 3 {
        path := os.Args[2]
        export.OutPath = path
        if os.Args[1] == "export_service" {
            return
        } else if os.Args[1] == "export_detector" {
            export.ExportDetectorInfo()
            return
        } else if os.Args[1] == "export_trace" {
            export.ExportTrace()
            return
        }
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
