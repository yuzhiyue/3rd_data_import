package data_import

import (
    "3rd_data_import/db"
    "gopkg.in/mgo.v2/bson"
    "sync"
    "errors"
    "log"
    "strings"
)

type FeatureRelate struct {
    Target string `json:"target" bson:"target"`
    Count int `json:"count" bson:"count"`
}

type Feature struct {
    ID string `json:"id" bson:"_id"`
    Type string `json:"type" bson:"type"`
    Value string `json:"value" bson:"value"`
    OrgCode string `json:"org_code" bson:"org_code"`
    Time uint32 `json:"time" bson:"time"`
    Relate []FeatureRelate `json:"relate" bson:"relate"`
}

var SaveFeatureLocker *sync.Mutex = new(sync.Mutex)
func SaveFeature(waitgroup *sync.WaitGroup, f1 Feature, f2 Feature) error{
    if f1.Type == "" || f2.Type == "" {
        log.Println("SaveFeature:type is empty, orgCode:" + f1.OrgCode)
        waitgroup.Done()
        return errors.New("type is empty")
    }

    if strings.Contains(f1.Type, " ") {
        log.Println("SaveFeature:type contains space, orgCode:" + f1.OrgCode + ", type:" + f1.Type)
        f1.Type = strings.Replace(f1.Type, " ", "", -1)
    }

    if strings.Contains(f2.Type, " ") {
        log.Println("SaveFeature:type contains space, orgCode:" + f2.OrgCode + ", type:" + f2.Type)
        f2.Type = strings.Replace(f2.Type, " ", "", -1)
    }

    session := db.GetDBSession()
    defer db.ReleaseDBSession(session)
    c := session.DB("feature").C("feature_set")
    f1Obj := bson.M{}
    f2Obj := bson.M{}
    SaveFeatureLocker.Lock()
    err1 := c.Find(bson.M{"feature.value": f1.Value, "feature.type": f1.Type}).One(f1Obj)
    err2 := c.Find(bson.M{"feature.value": f2.Value, "feature.type": f2.Type}).One(f2Obj)
    if err1 == nil && err2 == nil {
        if f1Obj["_id"].(string) != f2Obj["_id"].(string) {
            //merge
            feature2, ok := f2Obj["feature"]
            if ok {
                c.UpdateId(f1Obj["_id"].(string), bson.M{"$pushAll":bson.M{"feature":feature2.([]interface {})}})
                c.RemoveId(f2Obj["_id"].(string))
            }
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

    SaveFeatureV2(f1, f2)
    SaveFeatureLocker.Unlock()
    waitgroup.Done()
    return nil
}

func SaveFeatureV2(f1 Feature, f2 Feature) error {
    if f1.Type == f2.Type && f1.Value == f2.Value {
        return errors.New("f1 == f2")
    }

    session := db.GetDBSession()
    defer db.ReleaseDBSession(session)

    c := session.DB("feature").C("feature")
    oldFeature1 := Feature{}
    oldFeature2 := Feature{}
    err1 := c.Find(bson.M{"value": f1.Value, "type": f1.Type}).One(&oldFeature1)
    err2 := c.Find(bson.M{"value": f2.Value, "type": f2.Type}).One(&oldFeature2)
    if err1 != nil {
        f1.ID = bson.NewObjectId().Hex()
    } else {
        f1.ID = oldFeature1.ID
        f1.Relate = oldFeature1.Relate
    }

    if err2 != nil {
        f2.ID = bson.NewObjectId().Hex()
    } else {
        f2.ID = oldFeature2.ID
        f2.Relate = oldFeature2.Relate
    }

    hasRelate := false
    for i := range f1.Relate {
        r := & f1.Relate[i]
        if r.Target == f2.ID {
            hasRelate = true
            r.Count++
        }
    }
    if !hasRelate {
        r := FeatureRelate{f2.ID, 1}
        f1.Relate = append(f1.Relate, r)
    }

    hasRelate = false
    for i := range f2.Relate {
        r := & f2.Relate[i]
        if r.Target == f1.ID {
            hasRelate = true
            r.Count++
        }
    }
    if !hasRelate {
        r := FeatureRelate{f1.ID, 1}
        f2.Relate = append(f2.Relate, r)
    }

    c.UpsertId(f1.ID, f1)
    c.UpsertId(f2.ID, f2)
    return nil
}