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
    "3rd_data_import/data_import"
    "sync"
    "fmt"
    "io"
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


type RawData struct  {
    OrgCode string `bson:"org_code"`
    Type string `bson:"type"`
    Fields [] data_file.Field `bson:"fields"`
}

func SaveRawData(data * data_file.BCPFile)  {
    session := db.GetDBSession()
    defer db.ReleaseDBSession(session)
    c := session.DB("3rd_data").C("raw_data")
    bulk := c.Bulk()
    orgCode := data.Meta.OrgCode
    dataType := data.Meta.DataType
    for _, fields := range data.RawData {
        RawData := RawData{}
        RawData.OrgCode = orgCode
        RawData.Type = dataType
        RawData.Fields = fields
        bulk.Insert(RawData)
    }
    bulk.Run()
}

func UpdateApData(orgcode string, data []map[string]string)  {
    session := db.GetDBSession()
    defer db.ReleaseDBSession(session)
    c := session.DB("detector").C("detector_info")
    for i, fields := range data {
        mac := fields["AP_MAC"]
        mac = filterMac(mac)
        netbar_wacode := fields["NETBAR_WACODE"]
        lng, err1 := strconv.ParseFloat(fields["LONGITUDE"], 64)
        lat, err2 := strconv.ParseFloat(fields["LATITUDE"], 64)
        if orgcode == "779852855" && err2 != nil {
            lat, err2 = strconv.ParseFloat(fields["LAITTUDE"], 64)
        }
        if err1 != nil || err2 != nil {
            continue
        }
        lng, lat = data_file.Bd09towgs84(lng, lat)
        log.Println(i,": mac", mac, "lng", lng, "lat", lat)
        if saveToDB {
            c.UpsertId(mac, bson.M{"_id":mac, "longitude":lng, "latitude": lat, "last_active_time": uint32(time.Now().Unix()), "company":"02", "org_code":orgcode, "netbar_wacode":netbar_wacode})
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
    session := db.GetDBSession()
    defer db.ReleaseDBSession(session)
    c := session.DB("person_info").C("mac")
    bulk := c.Bulk()
    var waitgroup sync.WaitGroup
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
            f1 := data_import.Feature{}
            f2 := data_import.Feature{}
            f1.Type = "1020002"
            f1.Value = mac
            f1.OrgCode = orgcode
            f1.Time = uint32(time)
            if authType == "1020004" {
                bulk.Upsert(bson.M{"mac":mac, "phone":authAccount}, bson.M{"mac":mac, "phone":authAccount, "org_code":orgcode, "time":uint32(time)})
            } else if authType == "1029999" {
                if isPhoneNo(authAccount) {
                    bulk.Upsert(bson.M{"mac":mac, "phone":authAccount}, bson.M{"mac":mac, "phone":authAccount, "org_code":orgcode, "time":uint32(time)})
                }
            }

            if authType == "1029999" {
                if isPhoneNo(authAccount) {
                    f2.Type = "1020004"
                    f2.Value = authAccount
                    f2.OrgCode = orgcode
                    f2.Time = uint32(time)
                } else {
                    continue
                }
            } else {
                f2.Type = authType
                if f2.Type == "1020002" {
                    f2.Value = filterMac(authAccount)
                } else {
                    f2.Value = authAccount
                }

                f2.OrgCode = orgcode
                f2.Time = uint32(time)
            }
            waitgroup.Add(1)
            go data_import.SaveFeature(&waitgroup, f1, f2)
        }
    }
    bulk.Run()
    waitgroup.Wait()
}


func SaveTraceInfo(orgcode string, data []map[string]string)  {
    log.Println("SaveTraceInfo")
    session := db.GetDBSession()
    defer db.ReleaseDBSession(session)
    c := session.DB("detector").C("detector_report")
    bulk := c.Bulk()
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
            bulk.Insert(bson.M{"ap_mac":ap_mac, "device_mac":mac, "longitude":lng, "latitude": lat, "org_code":orgcode, "time":uint32(time)})
        }
    }
    bulk.Run()
}

func SaveBehaviorLog(orgcode string, data []map[string]string)  {
    log.Println("SaveLog")
    session := db.GetDBSession()
    defer db.ReleaseDBSession(session)
    c := session.DB("person_info").C("behavior_log")
    bulk := c.Bulk()
    for i, fields := range data {
        log.Println(i,fields)
        mac := fields["MAC"]
        mac = filterMac(mac)
        IpInt, err4 := strconv.ParseInt(fields["DST_IP"], 10, 64)
        Ip := inet_ntoa(IpInt)
        port := fields["DST_PORT"]
        lng, err1 := strconv.ParseFloat(fields["LONGITUDE"], 64)
        lat, err2 := strconv.ParseFloat(fields["LATITUDE"], 64)
        if orgcode == "779852855" && err2 != nil {
            lat, err2 = strconv.ParseFloat(fields["LAITTUDE"], 64)
        }
        time, err3 := strconv.Atoi(fields["CAPTURE_TIME"])
        if err1 != nil || err2 != nil || err3 != nil || err4 != nil{
            log.Println("error:", err1, err2, err3, err4)
            continue
        }
        lng, lat = data_file.Bd09towgs84(lng, lat)
        if saveToDB {
            //c.Insert(fields)
            bulk.Insert(bson.M{"mac": mac, "dst_ip":Ip, "dst_port":port, "longitude":lng, "latitude": lat, "org_code":orgcode, "time":uint32(time)})
        }
    }
    bulk.Run()
}

func SaveVirtualID(orgcode string, data []map[string]string) {
    log.Println("SaveVirtualID")
    var waitgroup sync.WaitGroup
    for _, fields := range data {
        authType := fields["B040021"]
        authAccount := fields["B040022"]
        virtualID := fields["B040003"]
        virtualType := fields["B040001"]
        mac := fields["C040002"]
        mac = filterMac(mac)
        time, err := strconv.Atoi(fields["H010015"])
        if err != nil {
            continue
        }
        fAuth := data_import.Feature{}
        fVirtual := data_import.Feature{}
        fMac := data_import.Feature{}

        if authType == "1029999" {
            if isPhoneNo(authAccount) {
                fAuth.Type = "1020004"
            } else {
                continue
            }
        } else if authType == "1020002" {
            fAuth.Type = "1020002"
            fAuth.Value = filterMac(authAccount)
        } else {
            fAuth.Type = authType
            fAuth.Value = authAccount
            fAuth.OrgCode = orgcode
            fAuth.Time = uint32(time)
        }

        fVirtual.Type = virtualType
        fVirtual.OrgCode = orgcode
        fVirtual.Time = uint32(time)
        fVirtual.Value = virtualID

        fMac.Value = mac
        fMac.OrgCode = orgcode
        fMac.Time = uint32(time)
        fMac.Type = "1020002"
        if authType == "1020002" {
            waitgroup.Add(1)
            go data_import.SaveFeature(&waitgroup, fMac, fVirtual)
        } else {
            waitgroup.Add(1)
            go data_import.SaveFeature(&waitgroup, fMac, fVirtual)
            waitgroup.Add(1)
            go data_import.SaveFeature(&waitgroup, fMac, fAuth)
            waitgroup.Add(1)
            go data_import.SaveFeature(&waitgroup, fAuth, fVirtual)
        }
    }
    waitgroup.Wait()
}

func CopyFile(src,dst string)(w int64,err error){
    srcFile,err := os.Open(src)
    if err!=nil{
        fmt.Println(err.Error())
        return
    }
    defer srcFile.Close()

    dstFile,err := os.Create(dst)

    if err!=nil{
        fmt.Println(err.Error())
        return
    }

    defer dstFile.Close()

    return io.Copy(dstFile,srcFile)
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
            log.Println("parse", fileName, bcpFile.Meta.FileName, orgCode)
            //PrintData(bcpFile.Fields)
            // PrintData(bcpFile.KeyFields)
            SaveRawData(&bcpFile)
            if orgCode == "555400905" || orgCode == "779852855"{
                ProcContent(orgCode, &bcpFile)
            }

        }

        os.MkdirAll("/home/detector/file_bak/" + orgCode, 0755)
        CopyFile(filePath, "/home/detector/file_bak/" + orgCode + "/" + fileName)
        os.Remove(filePath)
    }
}

func ProcContent(orgCode string, bcpFile * data_file.BCPFile)  {
    if strings.Contains(bcpFile.Meta.FileName, "WA_BASIC_FJ_0003") {
        UpdateApData(orgCode, bcpFile.Fields)
    } else if strings.Contains(bcpFile.Meta.FileName, "WA_SOURCE_FJ_1001") {
        SaveTraceInfo(orgCode, bcpFile.Fields)
    } else if strings.Contains(bcpFile.Meta.FileName, "WA_SOURCE_FJ_0001") {
        SaveDeviceInfo(orgCode, bcpFile.Fields)
    }else if strings.Contains(bcpFile.Meta.FileName, "WA_SOURCE_FJ_0002") {
        SaveBehaviorLog(orgCode, bcpFile.Fields)
    } else if strings.Contains(bcpFile.Meta.FileName, "WA_SOURCE_FJ_0003") {
        if orgCode != "555400905" {
            return
        }
        SaveVirtualID(orgCode, bcpFile.KeyFields)
    } else if strings.Contains(bcpFile.Meta.FileName, "WA_BASIC_FJ_0001"){
        if orgCode != "555400905" {
            return
        }
        data_import.SaveServiceInfo(bcpFile)
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
    db.InitDB()
    //stats.StatsImport("779852855")
    //stats.StatsImport("555400905")
    //return
    if saveToDB {
        //db.InitDB()
        //export.ExportDetectorInfo()
        //export.ExportTrace()
        //return
    }

    if len(os.Args) == 3 {
        path := os.Args[2]
        export.OutPath = path
        if os.Args[1] == "export_service" {
            export.ExportServiceFromDB(false)
            return
        } else if os.Args[1] == "export_detector" {
            export.ExportDetectorInfo()
            return
        } else if os.Args[1] == "export_trace" {
            export.ExportTrace()
            return
        } else if os.Args[1] == "export_service_status" {
            export.ExportServiceStatus()
            export.ExportServiceFromDB(true)
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
