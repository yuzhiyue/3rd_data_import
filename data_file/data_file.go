package data_file

import (
    "encoding/xml"
    "log"
    "archive/zip"
    "io/ioutil"
    "strings"
    "errors"
    "strconv"
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
    Fields []string
}

type DataFile struct {
    Meta FileMeta
    Fields []map[string]string
}

func (self *DataFile)Load(path string) error {
    unzip_file, err := zip.OpenReader(path)
    if err != nil {
        log.Println("openzip err:", err)
        return nil
    }
    defer unzip_file.Close()
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
    if self.Meta.FileName != DataFile.FileHeader.Name {
        return errors.New("file name mismatch")
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
            if int(c) == int('\t') || i == len(line) - 1{
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
        if len(fields) != 0 {
            self.Fields = append(self.Fields, fields)
        }
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
    log.Println(xmlInfo)
    for _, data := range xmlInfo.Dataset.Data {
        for _,item := range data.Items {
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

        for _, dataset := range data.Datasets {
            if dataset.Name == "WA_COMMON_010014" {
                for _, item := range dataset.Data[0].Items {
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
            }

            if dataset.Name == "WA_COMMON_010015" {
                for _, item := range dataset.Data[0].Items {
                    self.Fields = append(self.Fields, item.Eng)
                }
            }
        }
    }

    return nil
}