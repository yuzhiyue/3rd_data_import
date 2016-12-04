package data_import

import (
    "3rd_data_import/data_file"
    "3rd_data_import/db"
    "3rd_data_import/protocol"
    "gopkg.in/mgo.v2/bson"
    "gopkg.in/mgo.v2"
    "strconv"
)

func CreateServiceNo() (int, error) {
    session := db.GetDBSession()
    defer db.ReleaseDBSession(session)
    change := mgo.Change{
        Update: bson.M{"$inc": bson.M{"value": 1}},
        ReturnNew: true,
        Upsert: true,
    }
    doc := bson.M{}
    _, err := session.DB("platfrom").C("ids").Find(bson.M{"_id": "detector_no"}).Apply(change, &doc)
    if err == nil {
        no := int(db.GetNumber(doc, "value"))
        return no, nil
    }
    return -1, err
}

func SaveServiceInfo(data * data_file.BCPFile)  {
    session := db.GetDBSession()
    defer db.ReleaseDBSession(session)
    orgCode := data.Meta.OrgCode
    for _, fields := range data.KeyFields {
        serviceInfo := protocol.ServiceInfo{}
        serviceInfo.NETBAR_WACODE = fields["G020004"]
        serviceInfo.ID = orgCode + "_" + serviceInfo.NETBAR_WACODE
        serviceInfo.SERVICE_NAME = fields["G020017"]
        serviceInfo.PRINCIPAL = fields["E020001"]
        serviceInfo.PERSON_NAME = fields["E020001"]
        serviceInfo.PERSON_TEL = fields["B070003"]
        serviceInfo.BUSINESS_NATURE = fields["E010007"]
        serviceInfo.STATUS = 1
        serviceInfo.SERVICE_TYPE = 9
        serviceInfo.PROVINCE_CODE = serviceInfo.NETBAR_WACODE[:2] + "0000"
        serviceInfo.CITY_CODE = serviceInfo.NETBAR_WACODE[:4] + "00"
        serviceInfo.AREA_CODE = serviceInfo.NETBAR_WACODE[:6]
        serviceInfo.ADDRESS = fields["G020017"]
        serviceInfo.ORG_CODE = orgCode
        serviceInfo.XPOINT = fields["F010016"]
        serviceInfo.YPOINT = fields["F010017"]
        serviceInfo.CREATE_TIME = "2016-07-02 00:00:00"
        serviceInfo.CAP_TYPE = "2"

        serviceInfoOld := protocol.ServiceInfo{}
        err := session.DB("platform").C("service").FindId(serviceInfo.ID).One(&serviceInfoOld)
        if err != nil {
            no, err := CreateServiceNo()
            serviceInfo.NO = strconv.Itoa(no)
            if err != nil {
                continue
            }
            session.DB("platform").C("service").Insert(serviceInfo)
        } else {
            serviceInfo.NO = serviceInfoOld.NO
            session.DB("platform").C("service").UpsertId(serviceInfo.ID, serviceInfo)
        }
    }
}
