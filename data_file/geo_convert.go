package data_file
import "math"

var x_pi = 3.14159265358979324 * 3000.0 / 180.0
var pi = 3.1415926535897932384626
var a = 6378245.0
var ee = 0.00669342162296594323

func transformlat(lng float64, lat float64) float64 {
    ret := -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + 0.1 * lng * lat + 0.2 * math.Sqrt(math.Abs(lng))
    ret += (20.0 * math.Sin(6.0 * lng * pi) + 20.0 *
    math.Sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.Sin(lat * pi) + 40.0 *
    math.Sin(lat / 3.0 * pi)) * 2.0 / 3.0
    ret += (160.0 * math.Sin(lat / 12.0 * pi) + 320 *
    math.Sin(lat * pi / 30.0)) * 2.0 / 3.0
    return ret
}



func transformlng(lng float64, lat float64) float64 {
    ret := 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + 0.1 * lng * lat + 0.1 * math.Sqrt(math.Abs(lng))
    ret += (20.0 * math.Sin(6.0 * lng * pi) + 20.0 *
    math.Sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.Sin(lng * pi) + 40.0 *
    math.Sin(lng / 3.0 * pi)) * 2.0 / 3.0
    ret += (150.0 * math.Sin(lng / 12.0 * pi) + 300.0 *
    math.Sin(lng / 30.0 * pi)) * 2.0 / 3.0
    return ret
}

func bd09togcj02(bd_lon float64, bd_lat float64) (float64, float64) {
    x := bd_lon - 0.0065
    y := bd_lat - 0.006
    z := math.Sqrt(x * x + y * y) - 0.00002 * math.Sin(y * x_pi)
    theta := math.Atan2(y, x) - 0.000003 * math.Cos(x * x_pi)
    gg_lng := z * math.Cos(theta)
    gg_lat := z * math.Sin(theta)
    return gg_lng, gg_lat
}

func gcj02towgs84(lng float64, lat float64) (float64,float64) {
    dlat := transformlat(lng - 105.0, lat - 35.0)
    dlng := transformlng(lng - 105.0, lat - 35.0)
    radlat := lat / 180.0 * pi
    magic := math.Sin(radlat)
    magic = 1 - ee * magic * magic
    sqrtmagic := math.Sqrt(magic)
    dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * pi)
    dlng = (dlng * 180.0) / (a / sqrtmagic * math.Cos(radlat) * pi)
    mglat := lat + dlat
    mglng := lng + dlng
    return lng * 2 - mglng, lat * 2 - mglat
}

func Bd09towgs84(bd_lng float64, bd_lat float64) (float64, float64) {
    gcj_lng, gcj_lat := bd09togcj02(bd_lng, bd_lat)
    return gcj02towgs84(gcj_lng, gcj_lat)
}