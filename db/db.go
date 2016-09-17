package db

import (
    "gopkg.in/mgo.v2"
    "log"
    "runtime"
    "sync"
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
    return newSession
}

func ReleaseDBSession( session * mgo.Session)  {
    sessionNumLocker.Lock()
    session.Close();
    sessionNum--
    sessionNumLocker.Unlock()
}


