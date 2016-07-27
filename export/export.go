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
    "github.com/dutchcoders/goftp"
    "io/ioutil"
    "crypto/tls"
)

type ServiceInfo struct {
    SERVICE_CODE string
    SERVICE_NAME string
    ADDRESS string
    BUSINESS_NATURE string
    STATUS int
    SERVICE_TYPE int
    PROVINCE_CODE string
    CITY_CODE string
    AREA_CODE string
    XPOINT string
    YPOINT string
    CREATE_TIME string
    CAP_TYPE string
    PERSON_NAME string
    PERSON_TEL string
}

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
    LAST_CONNECT_TIME string
    WDA_VERSION string
    FIRMWARE_VERSION string
    COLLECTION_RADIU int
    UPLOAD_TIME_INTERVAL int
    CREATER string
}

type TraceInfo struct {
    MAC string
    TYPE uint32
    START_TIME uint32
    BSSID string
    XPOINT string
    YPOINT string
    DEVMAC string
    DEVICENUM string
    SERVICECODE string
}

var OrgCode string = "589504630"
var OutPath = "out"
var ServiceCode = "441402" + "39" + "000001"
func FormatMac(mac string ) string {
    return strings.ToUpper(mac[0:2] + "-" + mac[2:4] + "-" + mac[4:6] + "-" + mac[6:8] + "-" + mac[8:10] + "-" + mac[10:12])
}

func ExportService() {
    outArr := make([]ServiceInfo, 0)
    var serviceInfo ServiceInfo
    serviceInfo.SERVICE_CODE = ServiceCode
    serviceInfo.SERVICE_NAME = "梅州市WIFI采集"
    serviceInfo.ADDRESS = "广东省梅州市梅江区江南街道梅江三路138号"
    serviceInfo.PERSON_NAME = "黄工"
    serviceInfo.PERSON_TEL = "15870002521"
    serviceInfo.BUSINESS_NATURE = "3"
    serviceInfo.STATUS = 1
    serviceInfo.SERVICE_TYPE = 9
    serviceInfo.PROVINCE_CODE = "440000"
    serviceInfo.CITY_CODE = "441400"
    serviceInfo.AREA_CODE = "441402"
    serviceInfo.XPOINT = "116.117999"
    serviceInfo.YPOINT = "24.292084"
    serviceInfo.CREATE_TIME = "2016-07-02 00:00:00"
    serviceInfo.CAP_TYPE = "1"
    outArr = append(outArr, serviceInfo)
    jsonString, err := json.Marshal(outArr)
    if err != nil {
        return
    }
    log.Print(string(jsonString))
    SaveFile(string(jsonString), "008")
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
        detector.EQUIPMENT_NAME = "广晟通信_梅州_" + Mac[6:]
        detector.SECURITY_FACTORY_ORGCODE = OrgCode
        detector.SERVICE_CODE = ServiceCode
        detector.PROVINCE_CODE = "440000"
        detector.CITY_CODE = "441400"
        detector.AREA_CODE = "441402"
        detector.EQUIPMENT_TYPE = "10"
        detector.LATITUDE = strconv.FormatFloat(e.Latitude, 'f', 6, 64)
        detector.LONGITUDE = strconv.FormatFloat(e.Longitude, 'f', 6, 64)
        detector.CREATE_TIME = "2016-07-03 12:32:00"
        detector.LAST_CONNECT_TIME = time.Now().Format("2006-01-02 15:04:05")
        detector.WDA_VERSION = "1.10"
        detector.FIRMWARE_VERSION = "1.0"
        detector.COLLECTION_RADIU = 150
        detector.UPLOAD_TIME_INTERVAL = 60
        detector.CREATER = "黄工"
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
    err := session.DB("detector").C("detector_report").Find(bson.M{"org_code":bson.M{"$ne":"555400905"}}).Sort("-_id").Limit(1000).All(&traceArr)
    if err != nil {
        log.Println(err)
        return
    }

    outArr := make([]TraceInfo, 0)
    for _, e := range traceArr {
        ApMac := strings.ToUpper(e.ApMac[len(e.ApMac) - 12:])
        var trace TraceInfo
        trace.MAC = FormatMac(e.DeviceMac)
        trace.TYPE = 2
        trace.START_TIME = e.Time
        trace.BSSID = FormatMac(ApMac)
        trace.XPOINT = strconv.FormatFloat(e.Longitude, 'f', 6, 64)
        trace.YPOINT = strconv.FormatFloat(e.Latitude, 'f', 6, 64)
        trace.DEVMAC = FormatMac(ApMac)
        trace.DEVICENUM = OrgCode + ApMac
        trace.SERVICECODE = ServiceCode
        outArr = append(outArr, trace)
    }

    jsonString, err := json.Marshal(outArr)
    if err != nil {
        return
    }
    log.Print(string(jsonString))
    SaveFile(string(jsonString), "001")
}

func SaveFile(content string, typeCode string) {
    PthSep := string(os.PathSeparator)
    os.Mkdir(OutPath, 0777)
    fileName := OutPath + PthSep + time.Now().Format("20060102150405") + strconv.Itoa(rand.Intn(800) + 100) + "_139_441400_" + OrgCode + "_" + typeCode +".log"
    fout, err := os.Create(fileName)
    defer fout.Close()
    if err != nil {
        fmt.Println(fileName, err)
        return
    }

    fout.WriteString(content)
}

func UploadFile(filename string) {
    var ftp *goftp.FTP
    var err error
    // For debug messages: goftp.ConnectDbg("ftp.server.com:21")
    if ftp, err = goftp.Connect(""); err != nil {
        panic(err)
    }

    defer ftp.Close()

    config := tls.Config{
        InsecureSkipVerify: true,
        ClientAuth:         tls.RequestClientCert,
    }
    if err = ftp.AuthTLS(&config); err != nil {
        panic(err)
    }
    if err = ftp.Login("", ""); err != nil {
        panic(err)
    }
    if err = ftp.Cwd("/"); err != nil {
        panic(err)
    }
    var curpath string
    if curpath, err = ftp.Pwd(); err != nil {
        panic(err)
    }
    fmt.Printf("Current path: %s\n", curpath)
    err = ftp.Upload(filename);
    if err != nil {
        panic(err)
    }
    //os.Remove(filename)
}

func UploadFiles()  {
    files := make([]string, 0)
    dir, err := ioutil.ReadDir(OutPath)
    if err != nil {
        return
    }
    PthSep := string(os.PathSeparator)
    for _, f := range dir {
        if f.IsDir() {
            continue
        }
        if !strings.HasSuffix(f.Name(), ".log") {
            continue
        }
        if time.Now().Unix() - f.ModTime().Unix() < 60{
            continue
        }
        files = append(files, f.Name())
        log.Println("append", f.Name())
    }
    for _, fileName := range files {
        filePath := OutPath + PthSep + fileName
        log.Println("start proc", filePath)
        UploadFile(filePath)
    }
}