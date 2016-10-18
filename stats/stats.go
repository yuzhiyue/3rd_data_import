package stats

import (
    "3rd_data_import/db"
    "gopkg.in/mgo.v2/bson"
    "fmt"
)

func StatsDetectorNum(orgCode string) {
    session := db.GetDBSession()
    defer  db.ReleaseDBSession(session)
    count, _ := session.DB("detector").C("detector_info").Find(bson.M{"org_code":orgCode}).Count()
    fmt.Println("detector_num:", count)
}

func StatsFeatureNum(orgCode string) {
    session := db.GetDBSession()
    defer  db.ReleaseDBSession(session)
    count, _ := session.DB("feature").C("feature").Find(bson.M{"org_code":orgCode}).Count()
    fmt.Println("feature_num:", count)
}

func StatsDeviceFeatureNum(orgCode string) {
    session := db.GetDBSession()
    defer  db.ReleaseDBSession(session)
    count, _ := session.DB("person_info").C("mac").Find(bson.M{"org_code":orgCode}).Count()
    fmt.Println("device_feature_num:", count)
}

func StatsDeviceLogNum(orgCode string) {
    session := db.GetDBSession()
    defer  db.ReleaseDBSession(session)
    count, _ := session.DB("person_info").C("behavior_log").Find(bson.M{"org_code":orgCode}).Count()
    fmt.Println("log_num:", count)
}

func StatsImport(orgCode string)  {
    StatsDetectorNum(orgCode)
    StatsFeatureNum(orgCode)
    StatsDeviceFeatureNum(orgCode)
    StatsDeviceLogNum(orgCode)
}


