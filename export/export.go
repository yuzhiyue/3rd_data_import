package export

import (
    "3rd_data_import/db"
    "gopkg.in/mgo.v2/bson"
    "encoding/json"
    "log"
    "strconv"
    "strings"
    "time"
    "math/rand"
    "os"
    "fmt"
)

type DetectorDBInfo struct {
    Mac string `bson:"_id"`
    Longitude float64 `bson:"longitude"`
    Latitude float64 `bson:"latitude"`
    Last_active_time uint32 `bson:"last_active_time"`
}

type TraceDBInfo struct {
    ApMac string `bson:"ap_mac"`
    DeviceMac string `bson:"device_mac"`
    Longitude float64 `bson:"longitude"`
    Latitude float64 `bson:"latitude"`
    Time uint32 `bson:"time"`
}

type DetectorInfo struct {
    EQUIPMENT_NUM string
    EQUIPMENT_NAME string
    MAC string
    SECURITY_FACTORY_ORGCODE string
    SERVICE_CODE  string
    PROVINCE_CODE  string
    CITY_CODE  string
    AREA_CODE  string
    EQUIPMENT_TYPE string
    LONGITUDE  string
    LATITUDE  string
    CREATE_TIME string
}

var OrgCode string = "589504630"
var OutPath = "d:/out"

func FormatMac(mac string ) string {
    return strings.ToUpper(mac[0:2] + "-" + mac[2:4] + "-" + mac[4:6] + "-" + mac[6:8] + "-" + mac[8:10] + "-" + mac[10:12])
}

func ExportDetectorInfo() {
    session := db.GetDBSession()
    detectorArr := make([]DetectorDBInfo, 0)
    err := session.DB("detector").C("detector_info").Find(bson.M{"company":"01"}).All(&detectorArr)
    if err != nil {
        log.Println(err)
        return
    }
    outArr := make([]DetectorInfo, 0)
    for _, e := range detectorArr {
        if len(e.Mac) < 12 {
            continue
        }

        Mac := strings.ToUpper(e.Mac[len(e.Mac) - 12:])
        var detector DetectorInfo
        detector.MAC = FormatMac(Mac)
        detector.EQUIPMENT_NUM = OrgCode + Mac
        detector.EQUIPMENT_NAME = e.Mac
        detector.SECURITY_FACTORY_ORGCODE = OrgCode
        detector.SERVICE_CODE = "00000000000000"
        detector.PROVINCE_CODE = "44"
        detector.CITY_CODE = "441400"
        detector.AREA_CODE = "441421"
        detector.EQUIPMENT_TYPE = "00"
        detector.LATITUDE = strconv.FormatFloat(e.Latitude, 'f', 6, 64)
        detector.LONGITUDE = strconv.FormatFloat(e.Longitude, 'f', 6, 64)
        detector.CREATE_TIME = "2016-07-01 12:32:00"
        outArr = append(outArr, detector)
    }

    jsonString, err := json.Marshal(outArr)
    if err != nil {
        return
    }
    log.Print(string(jsonString))
    SaveFile(string(jsonString), "010")
}

func ExportTrace() {
    session := db.GetDBSession()
    traceArr := make([]TraceDBInfo, 0)
    err := session.DB("detector").C("detector_report").Find(bson.M{"company":"01"}).Sort("-time").Limit(1000).All(&traceArr)
    if err != nil {
        log.Println(err)
        return
    }
    for _, e := range traceArr {

    }
}

func SaveFile(content string, typeCode string) {
    fileName := OutPath + "/" + time.Now().Format("20060102150405") + "_" + strconv.Itoa(rand.Intn(800) + 100) + "_440200100001_" + OrgCode + "_" + typeCode +".log"
    fout, err := os.Create(fileName)
    defer fout.Close()
    if err != nil {
        fmt.Println(fileName, err)
        return
    }

    fout.WriteString(content)
}