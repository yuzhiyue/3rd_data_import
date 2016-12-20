package protocol


type DeviceInfo struct {
    SERVICE_CODE string
    USER_NAME string
    CERTIFICATE_TYPE string
    CERTIFICATE_CODE string
    ONLINE_TIME uint32
    OFFLINE_TIME uint32
    NET_ENDING_NAME string
    NET_ENDING_IP uint32
    NET_ENDING_MAC string
    ORG_NAME string
    COUNTRY string
    NOTE string
    SESSION_ID string
    MOBILE_PHONE string
    SRC_V4_IP uint32
    SRC_V6_IP  string
    SRC_V4START_PORT uint32
    SRC_V4END_PORT uint32
    SRC_V6START_PORT uint32
    SRC_V6END_PORT uint32
    AP_NUM string
    AP_MAC string
    AP_XPOINT string
    AP_YPOINT string
    POWER string
    XPOINT string
    YPOINT string
    AUTH_TYPE string
    AUTH_CODE string
    COMPANY_ID string
    APP_COMPANY_NAME string
    APP_SOFTWARE_NAME string
    APP_VERSION string
    APPID string
    IMSI string
    IMEI_ESN_MEID string
    OS_NAME string
    BRAND string
    MODEL string
}