package export

import (
    "3rd_data_import/db"
    "gopkg.in/mgo.v2/bson"
    "encoding/json"
    "log"
    "strconv"
    "strings"
)

type DetectorDBInfo struct {
    Mac string `bson:"_id"`
    Longitude float64 `bson:"longitude"`
    Latitude float64 `bson:"latitude"`
    Last_active_time uint32 `bson:"last_active_time"`
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
    INSTALL_DATE  string
    EQUIPMENT_TYPE string
    LONGITUDE  string
    LATITUDE  string
    UPLOAD_TIME_INTERVAL uint32
    COLLECTION_RADIUS uint32
    CREATE_TIME string
    LAST_CONNECT_TIME string
}

type DetectorInfoArr struct  {
    LIST []DetectorInfo
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
        var detector DetectorInfo
        detector.MAC = strings.ToUpper(e.Mac[:12])
        detector.MAC = detector.MAC[0:2] + "-" + detector.MAC[2:4] + "-" + detector.MAC[4:6] + "-" + detector.MAC[6:8] + "-" + detector.MAC[8:10] + "-" + detector.MAC[10:12]
        detector.EQUIPMENT_NUM = "000000000" + strings.ToUpper(e.Mac[:12])
        detector.EQUIPMENT_NAME = e.Mac
        detector.SECURITY_FACTORY_ORGCODE = "000000000"
        detector.SERVICE_CODE = "00000000000000"
        detector.PROVINCE_CODE = "000000"
        detector.CITY_CODE = "000000"
        detector.AREA_CODE = "000000"
        detector.EQUIPMENT_TYPE = "00"
        detector.LATITUDE = strconv.FormatFloat(e.Latitude, 'f', 6, 64)
        detector.LONGITUDE = strconv.FormatFloat(e.Longitude, 'f', 6, 64)
        detector.CREATE_TIME = "2016-07-01 12:32:00"
        outArr = append(outArr, detector)
    }
    arr := DetectorInfoArr{}
    arr.LIST = outArr
    jsonString, err := json.Marshal(outArr)
    if err != nil {
        return
    }
    log.Print(string(jsonString))
}