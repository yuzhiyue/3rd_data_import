package main;

import (
    "log"
    "gopkg.in/mgo.v2"
    "data_file"
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





func main() {
    zipFile := data_file.DataFile{}
    err := zipFile.Load("d:\\123-743218887-320500-320000-1435735991-00001.zip")
    if err != nil {
        log.Fatal(err)
    }

    log.Println(zipFile.Fields)

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
