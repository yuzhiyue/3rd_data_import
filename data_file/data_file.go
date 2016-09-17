package data_file

import (
    "encoding/xml"
    "log"
    "archive/zip"
    "io/ioutil"
    "strings"
    "strconv"
    "errors"
)

type Item struct {
    Key string `xml:"key,attr"`
    Val string `xml:"val,attr"`
    Rmk string `xml:"rmk,attr"`
    Eng string `xml:"eng,attr"`
    Chn string `xml:"chn,chn"`
}
type Data struct {
    Items []Item `xml:"ITEM"`
    Datasets []DataSet `xml:"DATASET"`
}

type DataSet struct {
    Name string `xml:"name,attr"`
    Rmk string `xml:"rmk,attr"`
    Data []Data `xml:"DATA"`
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
    Fields []Item
}

type BCPFile struct {
    Meta FileMeta
    Fields []map[string]string
    KeyFields []map[string]string
}

type DataFile struct {
    BCPFiles []BCPFile
}

func (self *DataFile)Load(path string) error {
    unzip_file, err := zip.OpenReader(path)
    if err != nil {
        log.Println("openzip err:", err)
        return nil
    }
    defer unzip_file.Close()
    var xmlFile *zip.File
    fileMap := make(map[string]*zip.File)
    for _, f := range unzip_file.File {
        if f.FileInfo().Name() == "GAB_ZIP_INDEX.xml" {
            xmlFile = f
        } else {
            fileMap[f.FileInfo().Name()] = f
        }
    }
    if xmlFile == nil {
        return errors.New("xmlFile is nil")
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
    err = self.Decode(buff)
    if err != nil {
        return err
    }

    for i := 0; i < len(self.BCPFiles); i++ {
        bcpFile := &self.BCPFiles[i]
        DataFile, ok := fileMap[bcpFile.Meta.FileName]
        if !ok {
            continue
        }
        fileData, err := DataFile.Open()
        defer fileData.Close()
        if err != nil {
            continue
        }
        buffData, err := ioutil.ReadAll(fileData)
        if err != nil {
            continue
        }
        lines := strings.Split(string(buffData), "\n")
        for _, line := range lines {
            fieldStart := -1
            inWord := false
            fieldIdx := 0
            fields := make(map[string] string)
            keyFields := make(map[string] string)
            for i, c := range line {
                if int(c) == int('\t') || i == len(line) - 1{
                    if inWord {
                        val := line[fieldStart: i]
                        name := bcpFile.Meta.Fields[fieldIdx].Eng
                        key := bcpFile.Meta.Fields[fieldIdx].Key
                        fields[name] = val
                        keyFields[key] = val
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
            if len(fields) != 0 {
                bcpFile.Fields = append(bcpFile.Fields, fields)
            }
            if len(keyFields) != 0 {
                bcpFile.KeyFields = append(bcpFile.KeyFields, keyFields)
            }
        }
    }

    return nil
}

func (self * DataFile)Decode(xmlContent []byte) error {
    xmlInfo := FileMetaXml{}
    err := xml.Unmarshal(xmlContent, &xmlInfo)
    if err != nil {
        log.Println("parse xml err,", err)
        return err
    }
    log.Println(xmlInfo)
    self.BCPFiles = make([]BCPFile, 0)
    for _, data := range xmlInfo.Dataset.Data[0].Datasets[0].Data {
        var file BCPFile
        for _,item := range data.Items {
            switch item.Key {
            case "I010032": {
                file.Meta.ColDelimiter = item.Val
                break;
            }
            case "I010033": {
                file.Meta.RowDelimiter = item.Val;
                break;
            }
            case "I010038": {
                file.Meta.StartLine,_ = strconv.Atoi(item.Val);
                break;
            }
            }
        }

        for _, dataset := range data.Datasets {
            if dataset.Name == "WA_COMMON_010014" {
                for _, item := range dataset.Data[0].Items {
                    switch item.Key {
                    case "H040003": {
                        file.Meta.Path = item.Val
                        break;
                    }
                    case "H010020": {
                        file.Meta.FileName = item.Val;
                        break;
                    }
                    }
                }
            }

            if dataset.Name == "WA_COMMON_010015" {
                for _, item := range dataset.Data[0].Items {
                    file.Meta.Fields = append(file.Meta.Fields, item)
                }
            }
        }
        self.BCPFiles = append(self.BCPFiles, file)
    }

    return nil
}