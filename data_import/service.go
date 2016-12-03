package data_import

import (
    "3rd_data_import/data_file"
    "3rd_data_import/db"
    "3rd_data_import/protocol"
    "gopkg.in/mgo.v2/bson"
    "fmt"
    "strconv"
)

func SaveRawData(data * data_file.BCPFile)  {
    session := db.GetDBSession()
    defer db.ReleaseDBSession(session)
    c := session.DB("3rd_data").C("raw_data")
    bulk := c.Bulk()
    orgCode := data.Meta.OrgCode
    dataType := data.Meta.DataType
    for _, fields := range data.KeyFields {
        serviceInfo := protocol.ServiceInfo{}
        serviceInfo.SERVICE_CODE = fields["G020004"]
        serviceInfo.SERVICE_NAME = fields["G020017"]
        serviceInfo.PERSON_NAME = "黄工"
        serviceInfo.PERSON_TEL = "15870002521"
        serviceInfo.BUSINESS_NATURE = "3"
        serviceInfo.STATUS = 1
        serviceInfo.SERVICE_TYPE = 9
        serviceInfo.PROVINCE_CODE = "440000"
        serviceInfo.CITY_CODE = "441400"
        serviceInfo.ADDRESS, serviceInfo.AREA_CODE = fields["G020017"],_

        serviceInfo.XPOINT = fields["F010016"]
        serviceInfo.YPOINT = fields["F010017"]
        serviceInfo.CREATE_TIME = "2016-07-02 00:00:00"
        serviceInfo.CAP_TYPE = "1"

        bulk.Upsert(bson.M{"_id": serviceInfo.SERVICE_CODE}, serviceInfo)
    }
    bulk.Run()
}
