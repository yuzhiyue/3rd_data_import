package main;

import (
    "archive/zip"
    "io/ioutil"
    "encoding/xml"
    "log"
    "gopkg.in/mgo.v2"
    "gopkg.in/mgo.v2/bson"
    "time"
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

type XmlInfo struct {
    mac string `xml:"mac"`
    phone string `xml:"phone"`
    time string `xml:"time"`
}

func (self *XmlInfo)save()  {
    macTable := GetDBSession().DB("person_info").C("mac")
    timestamp, _ := time.Parse("2006-01-02 15:04:05", self.time)
    macTable.Upsert(bson.M{"mac": self.mac, "phone": self.phone}, bson.M{"mac": self.mac, "phone": self.phone, "time":uint32(timestamp.Unix())})
}

func unzip(path string) ([][]byte, error) {
    unzip_file, err := zip.OpenReader(path)
    defer unzip_file.Close()
    if err != nil {
        return err;
    }
    ret_list := make([][]byte, 0)
    for _, f := range unzip_file.File {
        if f.FileInfo().IsDir() {
            continue
        }
        file, err := f.Open()
        defer file.Close()
        if err != nil {
            continue
        }
        buff, err := ioutil.ReadAll(file)
        if err != nil {
            continue
        }
        ret_list = append(ret_list, buff)
    }
    return nil
}

func main() {
    InitDB()
    fileList, err := unzip("")
    if err != nil {
        log.Println("unzip err,", err)
        return
    }
    for _, xmlContent := range fileList{
        xmlInfo := XmlInfo{}
        err = xml.Unmarshal(xmlContent, &xmlInfo)
        if err != nil {
            log.Println("parse xml err,", err)
            continue
        }
        log.Println(xmlInfo)
        xmlInfo.save()
    }

}
