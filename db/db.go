package db

import (
    "gopkg.in/mgo.v2"
    "log"
    "runtime"
    "sync"
    "gopkg.in/mgo.v2/bson"
)

var session *mgo.Session;
var sessionNum int
var sessionNumLocker *sync.Mutex = new(sync.Mutex)

func InitDB()  {
    var err error
    session, err = mgo.Dial("218.15.154.6:22522")
    if err != nil {
        panic(err)
    }
    session.SetMode(mgo.Monotonic, true)
    log.Println("connect to db succ")
    session.DB("feature").C("feature_set").EnsureIndexKey("feature.value", "feature.type")
    session.DB("feature").C("feature").EnsureIndexKey("value", "type")
    session.DB("3rd_data").C("raw_data").EnsureIndexKey("org_code", "type")
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

func GetDBSession() *mgo.Session {
    for ;; {
        if sessionNum < 900 {
            break
        } else {
            runtime.Gosched()
        }
    }
    sessionNumLocker.Lock()
    sessionNum++
    newSession := session.Clone()
    sessionNumLocker.Unlock()
    log.Println("GetDBSession: current session num ", sessionNum)
    return newSession
}

func ReleaseDBSession( session * mgo.Session)  {
    sessionNumLocker.Lock()
    session.Close();
    sessionNum--
    sessionNumLocker.Unlock()
    log.Println("ReleaseDBSession: current session num ", sessionNum)
}


