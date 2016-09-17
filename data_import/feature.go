package data_import

import (
    "3rd_data_import/db"
    "gopkg.in/mgo.v2/bson"
    "sync"
)

type Feature struct {
    Type string
    Value string
    OrgCode string
    Time uint32
}

func SaveFeature(waitgroup *sync.WaitGroup, f1 Feature, f2 Feature) {
    session := db.GetDBSession()
    defer db.ReleaseDBSession(session)
    c := session.DB("feature").C("feature_set")
    f1Obj := bson.M{}
    f2Obj := bson.M{}
    err1 := c.Find(bson.M{"feature.value": f1.Value, "feature.type": f1.Type}).One(f1Obj)
    err2 := c.Find(bson.M{"feature.value": f2.Value, "feature.type": f2.Type}).One(f2Obj)
    if err1 == nil && err2 == nil {
        //merge
        feature2, ok := f2Obj["feature"]
        if ok {
            c.UpdateId(f1Obj["_id"].(string), bson.M{"$pushAll":bson.M{"feature":feature2.([]interface {})}})
            c.RemoveId(f1Obj["_id"].(string))
        }
    } else if (err1 == nil && err2 != nil) {
        //push f2 in f1
        c.UpdateId(f1Obj["_id"].(string), bson.M{"$push":bson.M{"feature":bson.M{"type":f2.Type, "value":f2.Value, "org_code": f2.OrgCode, "time": f2.Time}}})
    } else if (err1 != nil && err2 == nil) {
        //push f1 in f2
        c.UpdateId(f2Obj["_id"].(string), bson.M{"$push":bson.M{"feature":bson.M{"type":f1.Type, "value":f1.Value, "org_code": f1.OrgCode, "time": f1.Time}}})
    } else {
        //insert new
        featureArr := []bson.M{bson.M{"type":f1.Type, "value":f1.Value, "org_code": f1.OrgCode, "time": f1.Time},
            bson.M{"type":f2.Type, "value":f2.Value, "org_code": f2.OrgCode, "time": f2.Time}}
        c.Insert(bson.M{"_id":bson.NewObjectId().Hex(), "feature":featureArr})
    }
    waitgroup.Done()
}