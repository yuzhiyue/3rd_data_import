package db

import (
    "gopkg.in/mgo.v2"
    "log"
)

var session *mgo.Session;

func InitDB()  {
    var err error
    session, err = mgo.Dial("218.15.154.6:22522")
    if err != nil {
        panic(err)
    }
    session.SetMode(mgo.Monotonic, true)
    log.Println("connect to db succ")
}


func GetDBSession() *mgo.Session {
    return session
}
