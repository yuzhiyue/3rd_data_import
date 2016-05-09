package data_file

import (
    "encoding/xml"
    "log"
    "strconv"
    "archive/zip"
    "io/ioutil"
    "strings"
)

type Item struct {
    Key string `xml:"key,attr"`
    Val string `xml:"val,attr"`
    Rmk string `xml:"rmk,attr"`
    Eng string `xml:"eng,attr"`
    Chn string `xml:"chn,chn"`
}
//type Data struct {
//    Name string `xml:"name,attr"`
//    Rmk string `xml:"rmk,attr"`
//    Items []Item `xml:"ITEM"`
//}
type DataSet struct {
    Name string `xml:"name,attr"`
    Rmk string `xml:"rmk,attr"`
    Items []Item `xml:"DATA>ITEM"`
    Datasets []DataSet `xml:"DATA>DATASET"`
}
type FileMetaXml struct {
    Dataset DataSet `xml:"DATASET"`
}

type FileMeta struct {
    ColDelimiter string
    RowDelimiter string
    StartLine int
    Path string
    FileName string
    Fields []string
}

type DataFile struct {
    Meta FileMeta
    Fields []map[string]string
}

func (self *DataFile)Load(path string) error {
    unzip_file, err := zip.OpenReader(path)
    defer unzip_file.Close()
    if err != nil {
        return nil
    }

    var xmlFile *zip.File
    var DataFile *zip.File
    for _, f := range unzip_file.File {
        if f.FileInfo().Name() == "GAB_ZIP_INDEX.xml" {
            xmlFile = f
        } else {
            DataFile = f
        }
    }
    fileXml, err := xmlFile.Open()
    defer fileXml.Close()
    if err != nil {
        return err
    }
    buff, err := ioutil.ReadAll(fileXml)
    if err != nil {
        return err
    }
    err = self.Meta.Decode(buff)
    if err != nil {
        return err
    }

    fileData, err := DataFile.Open()
    defer fileData.Close()
    if err != nil {
        return err
    }
    buffData, err := ioutil.ReadAll(fileData)
    if err != nil {
        return err
    }
    lines := strings.Split(string(buffData), "\n")
    for _, line := range lines {
        fieldStart := -1
        inWord := false
        fieldIdx := 0
        fields := make(map[string] string)
        for i, c := range line {
            if int(c) == int('\t'){
                if inWord {
                    val := line[fieldStart: i]
                    name := self.Meta.Fields[fieldIdx]
                    fields[name] = val
                }
                inWord = false
                fieldIdx++
            } else {
                if !inWord {
                    inWord = true;
                    fieldStart = i
                }
            }
        }
        self.Fields = append(self.Fields, fields)
    }
    return nil
}

func (self * FileMeta)Decode(xmlContent []byte) error {
    xmlInfo := FileMetaXml{}
    err := xml.Unmarshal(xmlContent, &xmlInfo)
    if err != nil {
        log.Println("parse xml err,", err)
        return err
    }

    for _,item := range xmlInfo.Dataset.Datasets[0].Items {
        switch item.Key {
        case "I010032": {
            self.ColDelimiter = item.Val
            break;
        }
        case "I010033": {
            self.RowDelimiter = item.Val;
            break;
        }
        case "I010038": {
            self.StartLine,_ = strconv.Atoi(item.Val);
            break;
        }
        }
    }

    for _, item := range xmlInfo.Dataset.Datasets[0].Datasets[0].Items {
        switch item.Key {
        case "H040003": {
            self.Path = item.Val
            break;
        }
        case "H010020": {
            self.FileName = item.Val;
            break;
        }
        }
    }

    for _, item := range xmlInfo.Dataset.Datasets[0].Datasets[1].Items {
        self.Fields = append(self.Fields, item.Eng)
    }
    return nil
}