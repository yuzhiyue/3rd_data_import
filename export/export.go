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
    "io/ioutil"
    "net/http"
    "3rd_data_import/protocol"
    "3rd_data_import/data_file"
)

type ServiceInfo struct {
    SERVICE_CODE string
    SERVICE_NAME string
    ADDRESS string
    ZIP string
    BUSINESS_NATURE string
    PRINCIPAL string
    PRINCIPAL_TEL string
    INFOR_MAN string
    INFOR_MAN_TEL string
    INFOR_MAN_EMAIL string
    PRODUCER_CODE string
    STATUS int
    ENDING_NUMBER int
    SERVER_NUMBER int
    EXIT_IP string
    AUTH_ACCOUNT string
    NET_TYPE string
    PRACTITIONER_NUMBER int
    NET_MONITOR_DEPARTMENT string
    NET_MONITOR_MAN string
    NET_MONITOR_MAN_TEL string
    REMARK string
    SERVICE_TYPE int
    PROVINCE_CODE string
    CITY_CODE string
    AREA_CODE string
    CITY_TYPE string
    POLICE_CODE string
    MAIL_ACCOUNT string
    MOBILE_ACCOUNT string
    XPOINT string
    YPOINT string
    GIS_XPOINT string
    GIS_YPOINT  string
    TERMINAL_FACTORY_ORGCODE  string
    ORG_CODE string
    IP_TYPE string
    BAND_WIDTH int
    NET_LAN int
    NET_LAN_TERMINAL int
    IS_SAFE string
    WIFI_TERMINAL int
    PRINCIPAL_CERT_TYPE string
    PRINCIPAL_CERT_CODE string
    PERSON_NAME string
    PERSON_TEL string
    PERSON_QQ string
    INFOR_MAN_QQ string
    START_TIME string
    END_TIME string
    CREATE_TIME string
    CAP_TYPE string
}

type ServiceStatus struct {
    SERVICE_CODE string
    SERVICE_ONLINE_STATUS int
    DATA_ONLINE_STATUS int
    EQUIPMENT_RUNNING_STATUS int
    ACTIVE_PC int
    REPORT_PC int
    ONLINE_PERSON int
    VITRUAL_NUM int
    EXIT_IP string
    UPDATE_TIME string
}

type DetectorDBInfo struct {
    Mac string `bson:"_id"`
    No int `bson:"no"`
    Longitude float64 `bson:"longitude"`
    Latitude float64 `bson:"latitude"`
    Last_active_time uint32 `bson:"last_active_time"`
    OrgCode string `bson:"org_code"`
    NetbarWacode string `bson:"netbar_wacode"`
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
    IP string
    SECURITY_FACTORY_ORGCODE string
    VENDOR_NAME string
    VENDOR_NUM string
    SERVICE_CODE  string
    PROVINCE_CODE  string
    CITY_CODE  string
    AREA_CODE  string
    INSTALL_DATE string
    INSTALL_POINT string
    EQUIPMENT_TYPE string
    LONGITUDE string
    LATITUDE string
    SUBWAY_STATION string
    SUBWAY_LINE_INFO string
    SUBWAY_VEHICLE_INFO string
    SUBWAY_COMPARTMENT_NUM string
    CAR_CODE string
    UPLOAD_TIME_INTERVAL int
    COLLECTION_RADIUS int
    CREATE_TIME string
    CREATER string
    LAST_CONNECT_TIME string
    REMARK string
    WDA_VERSION string
    FIRMWARE_VERSION string
}

type TraceInfo struct {
    MAC string
    TYPE uint32
    START_TIME uint32
    END_TIME uint32
    POWER string
    BSSID string
    ESSID string
    HISTORY_ESSID string
    MODEL string
    OS_VERSION string
    IMEI string
    IMSI string
    STATION string
    XPOINT string
    YPOINT string
    PHONE string
    DEVMAC string
    DEVICENUM string
    SERVICECODE string
    PROTOCOL_TYPE string
    ACCOUNT string
    FLAG string
    URL string
    COMPANY_ID string
    AP_CHANNEL string
    AP_ENCRYTYPE string
    CONSULT_XPOINT string
    CONSULT_YPOINT string
}

var OrgCode string = "589504630"
var OutPath = "out"
var ServiceCode = "441402" + "39" + "000001"
var ServiceCodePrefix = "441402" + "39"
func FormatMac(mac string ) string {
    return strings.ToUpper(mac[0:2] + "-" + mac[2:4] + "-" + mac[4:6] + "-" + mac[6:8] + "-" + mac[8:10] + "-" + mac[10:12])
}

func GeoConvert(lng float64, lat float64) (float64,float64) {
    url := fmt.Sprintf("http://restapi.amap.com/v3/assistant/coordinate/convert?key=4e7f4dba3fdfe5fbc2ff361da70f2c2a&locations=%f,%f&coordsys=gps", lng, lat)
    resp, err := http.Get(url)
    if err != nil {
        // handle error
    }

    defer resp.Body.Close()
    body, err := ioutil.ReadAll(resp.Body)
    if err == nil {
        var res map[string] string
        err = json.Unmarshal(body, &res)
        if err == nil {
            loc, ok := res["locations"]
            if ok {
                geo := strings.Split(loc, ",")
                lng_o, _ := strconv.ParseFloat(geo[0], 64)
                lat_o, _ := strconv.ParseFloat(geo[1], 64)
                return lng_o, lat_o
            }
        }
    }
    return 0, 0
}

func GeoCode(lng float64, lat float64) (string,string) {
    url := fmt.Sprintf("http://restapi.amap.com/v3/geocode/regeo?key=4e7f4dba3fdfe5fbc2ff361da70f2c2a&location=%f,%f&extensions=base&batch=false", lng, lat)
    resp, err := http.Get(url)
    if err != nil {
        return "广东省梅州市梅江区江南街道梅江三路138号", "441402"
    }

    defer resp.Body.Close()
    body, err := ioutil.ReadAll(resp.Body)
    if err == nil {
        var res map[string] interface{}
        err = json.Unmarshal(body, &res)
        if err == nil {
            fmt.Println(string(body))
            regCode, ok := res["regeocode"]
            if ok {
                regCodeObj := regCode.(map[string] interface{})
                fmtAddress, ok1 := regCodeObj["formatted_address"]
                addressComponent, ok2 := regCodeObj["addressComponent"]
                if ok1 && ok2 {
                    addressComponentObj := addressComponent.(map[string] interface{})
                    adCode, ok3 := addressComponentObj["adcode"]
                    if ok3 {
                        return fmtAddress.(string), adCode.(string)
                    }
                }
            }
        }
    }
    return "广东省梅州市梅江区江南街道梅江三路138号", "441402"
}

func GetGDGeoCode(lng float64, lat float64) (string, string) {
    lng2, lat2 := GeoConvert(lng, lat)
    if lng2 !=0 && lat2 != 0 {
        return GeoCode(lng2, lat2)
    }
    return "广东省梅州市梅江区江南街道梅江三路138号", "441402"
}

func ExportService(no int, lng float64, lat float64) ServiceInfo {
    var serviceInfo ServiceInfo
    serviceInfo.SERVICE_CODE = ServiceCodePrefix + fmt.Sprintf("%06d", no)
    serviceInfo.SERVICE_NAME = "梅州市WIFI采集_" + strconv.Itoa(no)

    serviceInfo.PERSON_NAME = "黄工"
    serviceInfo.PERSON_TEL = "15870002521"
    serviceInfo.BUSINESS_NATURE = "3"
    serviceInfo.STATUS = 1
    serviceInfo.SERVICE_TYPE = 9
    serviceInfo.PROVINCE_CODE = "440000"
    serviceInfo.CITY_CODE = "441400"
    serviceInfo.ADDRESS, serviceInfo.AREA_CODE = GetGDGeoCode(lng, lat)

    serviceInfo.XPOINT = strconv.FormatFloat(lng, 'f', 6, 64)
    serviceInfo.YPOINT = strconv.FormatFloat(lat, 'f', 6, 64)
    serviceInfo.CREATE_TIME = "2016-07-02 00:00:00"
    serviceInfo.CAP_TYPE = "1"
    return serviceInfo
}

func ExportServiceFromDB(onlyStatus bool) {
    session := db.GetDBSession()
    defer db.ReleaseDBSession(session)
    serviceArr := make([]protocol.ServiceInfo, 0)
    outServiceStatusArr := make([]ServiceStatus, 0)
    err := session.DB("platform").C("service").Find(bson.M{}).All(&serviceArr)
    if err != nil {
        log.Println(err)
        return
    }
    for i := range serviceArr {
        service := &serviceArr[i]
        no,_ := strconv.Atoi(service.NO)
        service.SERVICE_CODE = service.NETBAR_WACODE[:8] + fmt.Sprintf("%06d", no)

        serviceStatus := ServiceStatus{}
        serviceStatus.SERVICE_CODE = service.SERVICE_CODE
        serviceStatus.SERVICE_ONLINE_STATUS = 1
        serviceStatus.DATA_ONLINE_STATUS = 1
        serviceStatus.EQUIPMENT_RUNNING_STATUS = 1
        serviceStatus.ACTIVE_PC = 0
        serviceStatus.REPORT_PC = 0
        serviceStatus.ONLINE_PERSON = 0
        serviceStatus.VITRUAL_NUM = 0
        serviceStatus.EXIT_IP = "0.0.0.0"
        serviceStatus.UPDATE_TIME = time.Now().Format("2006-01-02 15:04:05")
        outServiceStatusArr = append(outServiceStatusArr, serviceStatus)
    }

    if !onlyStatus {
        jsonString, err := json.Marshal(serviceArr)
        if err != nil {
            return
        }
        log.Print(string(jsonString))
        SaveFile(string(jsonString), "008")
    }

    jsonString, err := json.Marshal(outServiceStatusArr)
    if err != nil {
        return
    }
    log.Print(string(jsonString))
    SaveFile(string(jsonString), "009")
}

func ExportServiceStatus()  {
    session := db.GetDBSession()
    defer db.ReleaseDBSession(session)
    detectorArr := make([]DetectorDBInfo, 0)
    err := session.DB("detector").C("detector_info").Find(bson.M{"company":"01"}).All(&detectorArr)
    if err != nil {
        log.Println(err)
        return
    }

    outServiceStatusArr := make([]ServiceStatus, 0)
    for _, e := range detectorArr {
        if len(e.Mac) < 12 {
            continue
        }

        serviceStatus := ServiceStatus{}
        serviceStatus.SERVICE_CODE = ServiceCodePrefix + fmt.Sprintf("%06d", e.No)
        serviceStatus.SERVICE_ONLINE_STATUS = 1
        if e.Latitude != 0 {
            serviceStatus.DATA_ONLINE_STATUS = 1
            serviceStatus.EQUIPMENT_RUNNING_STATUS = 1
        } else {
            serviceStatus.DATA_ONLINE_STATUS = 2
            serviceStatus.EQUIPMENT_RUNNING_STATUS = 2
        }
        serviceStatus.ACTIVE_PC = 0
        serviceStatus.REPORT_PC = 0
        serviceStatus.ONLINE_PERSON = 0
        serviceStatus.VITRUAL_NUM = 0
        serviceStatus.EXIT_IP = "0.0.0.0"
        serviceStatus.UPDATE_TIME = time.Now().Format("2006-01-02 15:04:05")
        outServiceStatusArr = append(outServiceStatusArr, serviceStatus)
    }
    jsonString, err := json.Marshal(outServiceStatusArr)
    if err != nil {
        return
    }
    log.Print(string(jsonString))
    SaveFile(string(jsonString), "009")
}

func  ExportDetectorInfo() {
    session := db.GetDBSession()
    defer db.ReleaseDBSession(session)
    detectorArr := make([]DetectorDBInfo, 0)
    err := session.DB("detector").C("detector_info").Find(bson.M{"company":"01"}).All(&detectorArr)
    if err != nil {
        log.Println(err)
        return
    }
    outArr := make([]DetectorInfo, 0)
    outServiceArr := make([]ServiceInfo, 0)
    for _, e := range detectorArr {
        if len(e.Mac) < 12 {
            continue
        }
        service := ExportService(e.No, e.Longitude, e.Latitude)
        outServiceArr = append(outServiceArr, service)
        Mac := strings.ToUpper(e.Mac[len(e.Mac) - 12:])
        var detector DetectorInfo
        detector.MAC = FormatMac(Mac)
        detector.EQUIPMENT_NUM = OrgCode + Mac
        detector.EQUIPMENT_NAME = "广晟通信_梅州_" + Mac[6:]
        detector.SECURITY_FACTORY_ORGCODE = OrgCode
        detector.SERVICE_CODE = service.SERVICE_CODE
        detector.PROVINCE_CODE = "440000"
        detector.CITY_CODE = "441400"
        detector.AREA_CODE = service.AREA_CODE
        detector.EQUIPMENT_TYPE = "10"
        detector.LATITUDE = strconv.FormatFloat(e.Latitude, 'f', 6, 64)
        detector.LONGITUDE = strconv.FormatFloat(e.Longitude, 'f', 6, 64)
        detector.CREATE_TIME = "2016-07-03 12:32:00"
        detector.LAST_CONNECT_TIME = time.Now().Format("2006-01-02 15:04:05")
        detector.WDA_VERSION = "1.10"
        detector.FIRMWARE_VERSION = "1.0"
        detector.COLLECTION_RADIUS = 150
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

    jsonString, err = json.Marshal(outServiceArr)
    if err != nil {
        return
    }
    log.Print(string(jsonString))
    SaveFile(string(jsonString), "008")
}

func  ExportAPInfo() {
    session := db.GetDBSession()
    defer db.ReleaseDBSession(session)
    detectorArr := make([]DetectorDBInfo, 0)
    err := session.DB("detector").C("detector_info").Find(bson.M{"company":"02"}).All(&detectorArr)
    if err != nil {
        log.Println(err)
        return
    }

    outArr := make([]DetectorInfo, 0)
    for _, e := range detectorArr {
        if len(e.Mac) < 12 {
            continue
        }
        service := protocol.ServiceInfo{}
        err := session.DB("platform").C("service").Find(bson.M{"id":e.OrgCode+"_"+e.NetbarWacode}).One(&service)
        if err != nil {
            continue
        }
        no,_ := strconv.Atoi(service.NO)
        service.SERVICE_CODE = service.NETBAR_WACODE[:8] + fmt.Sprintf("%06d", no);
        Mac := strings.ToUpper(e.Mac[len(e.Mac) - 12:])
        var detector DetectorInfo
        detector.MAC = FormatMac(Mac)
        detector.EQUIPMENT_NUM = OrgCode + Mac
        detector.EQUIPMENT_NAME = service.SERVICE_NAME + Mac[6:]
        detector.SECURITY_FACTORY_ORGCODE = OrgCode
        detector.SERVICE_CODE = service.SERVICE_CODE
        detector.PROVINCE_CODE = service.PROVINCE_CODE
        detector.CITY_CODE = service.CITY_CODE
        detector.AREA_CODE = service.AREA_CODE
        detector.EQUIPMENT_TYPE = "00"
        detector.LATITUDE = strconv.FormatFloat(e.Latitude, 'f', 6, 64)
        detector.LONGITUDE = strconv.FormatFloat(e.Longitude, 'f', 6, 64)
        detector.CREATE_TIME = "2016-07-03 12:32:00"
        detector.LAST_CONNECT_TIME = time.Now().Format("2006-01-02 15:04:05")
        detector.WDA_VERSION = "1.10"
        detector.FIRMWARE_VERSION = "1.0"
        detector.COLLECTION_RADIUS = 150
        detector.UPLOAD_TIME_INTERVAL = 60
        detector.CREATER = service.PERSON_NAME
        outArr = append(outArr, detector)
    }

    jsonString, err := json.Marshal(outArr)
    if err != nil {
        return
    }
    log.Print(string(jsonString))
    SaveFile(string(jsonString), "010")
}


func ExportAPTrace() {
    session := db.GetDBSession()
    defer db.ReleaseDBSession(session)
    detectorArr := make([]DetectorDBInfo, 0)
    err := session.DB("detector").C("detector_info").Find(bson.M{"company":"02"}).All(&detectorArr)
    if err != nil {
        log.Println(err)
        return
    }

    outArr := make([]TraceInfo, 0)
    for _, e := range detectorArr {
        if len(e.Mac) < 12 {
            continue
        }
        service := protocol.ServiceInfo{}
        err := session.DB("platform").C("service").Find(bson.M{"id":e.OrgCode+"_"+e.NetbarWacode}).One(&service)
        if err != nil {
            continue
        }

        no,_ := strconv.Atoi(service.NO)
        service.SERVICE_CODE = service.NETBAR_WACODE[:8] + fmt.Sprintf("%06d", no)
        traceArr := make([]TraceDBInfo, 0)
        err = session.DB("detector").C("detector_report").Find(bson.M{"ap_mac":e.Mac}).Sort("-time").Limit(1).All(&traceArr)
        if err != nil {
            log.Println(err)
            return
        }

        for _, e := range traceArr {
            detectorDBInfo := DetectorDBInfo{}
            session.DB("detector").C("detector_info").FindId(e.ApMac).One(&detectorDBInfo)
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
            trace.SERVICECODE = service.SERVICE_CODE
            outArr = append(outArr, trace)
        }
    }

    jsonString, err := json.Marshal(outArr)
    if err != nil {
        return
    }
    log.Print(string(jsonString))
    SaveFile(string(jsonString), "001")
}

func ExportTrace() {
    session := db.GetDBSession()
    defer db.ReleaseDBSession(session)
    traceArr := make([]TraceDBInfo, 0)
    err := session.DB("detector").C("detector_report").Find(bson.M{"org_code":bson.M{"$ne":"555400905"}}).Sort("-_id").Limit(1000).All(&traceArr)
    if err != nil {
        log.Println(err)
        return
    }

    outArr := make([]TraceInfo, 0)
    for _, e := range traceArr {
        detectorDBInfo := DetectorDBInfo{}
        session.DB("detector").C("detector_info").FindId(e.ApMac).One(&detectorDBInfo)
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
        trace.SERVICECODE = ServiceCodePrefix + fmt.Sprintf("%06d", detectorDBInfo.No)
        outArr = append(outArr, trace)
    }

    jsonString, err := json.Marshal(outArr)
    if err != nil {
        return
    }
    log.Print(string(jsonString))
    SaveFile(string(jsonString), "001")
}

type RawData struct  {
    OrgCode string `bson:"org_code"`
    Type string `bson:"type"`
    Fields [] data_file.Field `bson:"fields"`
}

func filterMac(mac string) string {
    return strings.ToLower(strings.Replace(strings.Replace(mac, "-", "", -1), ":", "", -1))
}

func ExportDeviceInfo() {
    rawData := make([]RawData, 0)
    session := db.GetDBSession()
    defer db.ReleaseDBSession(session)
    err := session.DB("3rd_data").C("raw_data").Find(bson.M{"org_code":"555400905","type":"WA_SOURCE_FJ_0001"}).Sort("-_id").Limit(10).All(&rawData)
    if err != nil {
        log.Println(err)
        return
    }

    outArr := make([]protocol.DeviceInfo, 0)
    for _, data := range rawData {
        fields := make(map[string]string)
        for _, e := range data.Fields{
            fields[e.Key] = e.Value
        }
        wc := fields["G020004"]
        service := protocol.ServiceInfo{}
        err := session.DB("platform").C("service").Find(bson.M{"id":data.OrgCode+"_"+wc}).One(&service)
        if err != nil {
            continue
        }

        no,_ := strconv.Atoi(service.NO)
        service.SERVICE_CODE = service.NETBAR_WACODE[:8] + fmt.Sprintf("%06d", no)

        deviceInfo := protocol.DeviceInfo{}
        deviceInfo.SERVICE_CODE = service.SERVICE_CODE
        tmp,_ := strconv.ParseUint(fields["H010015"],10,32)
        deviceInfo.ONLINE_TIME = uint32(tmp)
        deviceInfo.NET_ENDING_NAME = ""
        tmp,_ = strconv.ParseUint(fields["F020001"],10,32)
        deviceInfo.NET_ENDING_IP = uint32(tmp)
        deviceInfo.NET_ENDING_MAC = fields["C040002"]
        deviceInfo.SESSION_ID = fields["H010013"]
        deviceInfo.AP_MAC = fields["F030011"]
        deviceInfo.AP_NUM = data.OrgCode +  filterMac(deviceInfo.AP_MAC)
        deviceInfo.COMPANY_ID = data.OrgCode
        outArr = append(outArr, deviceInfo)
    }

    jsonString, err := json.Marshal(outArr)
    if err != nil {
        return
    }
    log.Print(string(jsonString))
    SaveFile(string(jsonString), "005")
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
    }
}