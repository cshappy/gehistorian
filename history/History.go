package history

import (
	"database/sql"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-ole/go-ole"
	_ "github.com/mattn/go-adodb"
)

func Historytojson(data map[string]string) (interface{}, error) {
	var finalresult = map[string]interface{}{}
	var result = []interface{}{}
	var tagname string
	var timestart string
	var timeend string
	var ip string
	var target string
	var timestartlength string
	var timeendlength string
	var intervalmilliseconds string
	var calculationMode string
	var numberofsamples string
	var samplingmode string
	var timezone string

	tagname, _ = data["tagname"]
	timestart, _ = data["timestart"]
	timestartlength, _ = data["timestart"]
	timeend, _ = data["timeend"]
	timeendlength, _ = data["timeend"]
	ip, _ = data["ip"]
	target, _ = data["target"]
	intervalmilliseconds, _ = data["intervalMilliseconds"]
	calculationMode, _ = data["calculationMode"]
	numberofsamples, _ = data["numberofsamples"]
	samplingmode, _ = data["samplingmode"]
	timezone, _ = data["timezone"]

	if tagname == "" {
		var apidata = make(map[string]interface{})
		apidata["ErrorCode"] = 10
		apidata["ErrorMessage"] = "Invalid tagName"
		result = append(result, []interface{}{apidata}...)
		return result, nil
	}

	db, err := sql.Open("adodb", "Provider=ihOLEDB.iHistorian.1;User ID=;Password=;Data Source="+ip+";")
	if err != nil {
		var apidata = make(map[string]interface{})
		apidata["ErrorCode"] = 10
		apidata["ErrorMessage"] = "Invalid sql"
		result = append(result, []interface{}{apidata}...)
		return result, nil
	}
	timestart, err = getTime(timestart)
	if err != nil {
		var apidata = make(map[string]interface{})
		apidata["status"] = 405
		apidata["error"] = "error"
		apidata["message"] = "invlaid Starttime"
		result = append(result, []interface{}{apidata}...)
		return result, nil
	}
	timeend, err = getTime(timeend)
	if err != nil {
		var apidata = make(map[string]interface{})
		apidata["status"] = 405
		apidata["error"] = "error"
		apidata["message"] = "invlaid Endtime"
		result = append(result, []interface{}{apidata}...)
		return result, nil
	}
	cutname := strings.Split(tagname, ";")
	var tag string
	for i := 1; i < len(cutname); i++ {
		tag = tag + " or tagname=" + cutname[i]
	}
	var sqlstring string
	if len(timestartlength) == 0 || (len(timeendlength) == 0 && len(timestartlength) == 0) {
		sqlstring = "SELECT tagname,quality,value,timestamp FROM ihRawData where tagname=" + cutname[0] + tag
	} else if len(timeendlength) == 0 {
		sqlstring = "SELECT tagname,quality,value,timestamp FROM ihRawData where tagname=" + cutname[0] + tag + " and timestamp>='" + timestart
	} else {
		sqlstring = "SELECT tagname,quality,value,timestamp FROM ihRawData where tagname=" + cutname[0] + tag + " and timestamp>='" + timestart + "' and timestamp<='" + timeend + "'"
	}
	if len(intervalmilliseconds) != 0 {
		sqlstring = sqlstring + " and intervalmilliseconds=" + intervalmilliseconds
	}

	if len(calculationMode) != 0 {
		sqlstring = sqlstring + " and calculationMode=" + calculationMode
	}

	if len(numberofsamples) != 0 {
		sqlstring = sqlstring + " and numberofsamples=" + numberofsamples
	}

	if len(samplingmode) != 0 {
		sqlstring = sqlstring + " and samplingmode=" + samplingmode
	}

	if len(timezone) != 0 {
		sqlstring = sqlstring + " and timezone=" + timezone
	}
	rows, err := db.Query(sqlstring)
	if err != nil {
		panic(err)
	}
	if target == "" {
		var tagnamestring string
		var samples = []interface{}{}
		var apidata = make(map[string]interface{})
		var samplesdata = make(map[string]interface{})
		for rows.Next() {
			var tagname *ole.VARIANT
			var quality *ole.VARIANT
			var value *ole.VARIANT
			var timestamp string
			rows.Scan(&tagname, &quality, &value, &timestamp)
			if tagnamestring == tagname.ToString() {
				apidata["TagName"] = tagname.ToString()
				apidata["ErrorCode"] = 0
				samplesdata["Value"] = value.Value()
				samplesdata["Quality"] = quality.Value()
				samplesdata["Timestamp"] = timestamp
				samples = append(samples, []interface{}{samplesdata}...)
				samplesdata = make(map[string]interface{})
			} else {
				apidata["Samples"] = samples
				result = append(result, []interface{}{apidata}...)
				samples = []interface{}{}
				apidata = make(map[string]interface{})
				tagnamestring = tagname.ToString()
				apidata["TagName"] = tagname.ToString()
				apidata["ErrorCode"] = 0
				samplesdata["Value"] = value.Value()
				samplesdata["Quality"] = quality.Value()
				samplesdata["Timestamp"] = timestamp
				samples = append(samples, []interface{}{samplesdata}...)
				samplesdata = make(map[string]interface{})
			}
		}
		apidata["Samples"] = samples
		if len(samples) == 0 {
			apidata["ErrorCode"] = -14
		}
		result = append(result, []interface{}{apidata}...)
		// return result[1:], nil
		finalresult["Data"] = result[1:]
		finalresult["ErrorCode"] = 0
		finalresult["ErrorMessage"] = nil
		return finalresult, nil
	} else if target == "echart" {
		var echartdata = make(map[string]interface{})
		var name string
		var eachartvalue = []interface{}{}
		var data = make(map[string]interface{})
		var resultdata = []interface{}{}
		for rows.Next() {
			var tagname *ole.VARIANT
			var quality *ole.VARIANT
			var value *ole.VARIANT
			var timestamp string
			echartdata = make(map[string]interface{})

			rows.Scan(&tagname, &quality, &value, &timestamp)
			timestamp = timestamp[0:10] + " " + timestamp[11:23]
			if name == tagname.ToString() {
				echartdata["name"] = tagname.ToString()
				totimestamp, _ := time.ParseInLocation("2006-01-02 15:04:05.000", timestamp, time.Local)
				eachartvalue = append(eachartvalue, strconv.FormatInt(totimestamp.UnixNano()/1e6, 10))
				tostring := typetostring(value, reflect.TypeOf(value.Value()).String())
				eachartvalue = append(eachartvalue, tostring)
				data["value"] = eachartvalue
				resultdata = append(resultdata, data)
				eachartvalue = []interface{}{}
				data = make(map[string]interface{})
			} else {
				echartdata["name"] = name
				name = tagname.ToString()
				echartdata["data"] = resultdata
				result = append(result, echartdata)
				resultdata = []interface{}{}
				echartdata = make(map[string]interface{})
				eachartvalue = []interface{}{}
				data = make(map[string]interface{})
				totimestamp, _ := time.ParseInLocation("2006-01-02 15:04:05.000", timestamp, time.Local)
				eachartvalue = append(eachartvalue, strconv.FormatInt(totimestamp.UnixNano()/1e6, 10))
				tostring := typetostring(value, reflect.TypeOf(value.Value()).String())
				eachartvalue = append(eachartvalue, tostring)
				data["value"] = eachartvalue
				resultdata = append(resultdata, data)
				eachartvalue = []interface{}{}
			}
			echartdata["data"] = resultdata
		}
		result = append(result, echartdata)
		// return result[1:], nil
		finalresult["Data"] = result[1:]
		finalresult["ErrorCode"] = 0
		finalresult["ErrorMessage"] = nil
		return finalresult, nil
	}
	return result, nil

}
func removezero(num string) string {
	for i := 0; i < 4; i++ {
		if num[len(num)-1:len(num)] == "0" {
			num = num[:len(num)-1]
		}
	}
	if num[len(num)-1:len(num)] == "." {
		num = num[:len(num)-1]
	}
	return num

}
func getTime(timestring string) (string, error) {
	var timenum string
	if len(timestring) == 0 {
		t := time.Now()
		timenow := t.Format("20060102150405")
		year := timenow[0:4]
		month := timenow[4:6]
		day := timenow[6:8]
		hour := timenow[8:10]
		min := timenow[10:12]
		second := timenow[12:14]
		timenum = month + "/" + day + "/" + year + " " + hour + ":" + min + ":" + second
	} else {
		var timesnum, err = strconv.ParseInt(timestring[:len(timestring)-3], 10, 64)
		if err == nil {
			tm := time.Unix(timesnum, 0)
			timenum = tm.Format("01/02/2006 15:05:05")
		} else {
			timenum = timestring
		}
	}
	return timenum, nil
}

func typetostring(value *ole.VARIANT, valuetype string) string {
	var stringnum string
	if valuetype == "int32" {
		stringnum = Int32toString(value.Value().(int32))
	} else if valuetype == "int64" {
		stringnum = strconv.FormatInt(value.Value().(int64), 10)
	} else if valuetype == "float64" {
		stringnum = strconv.FormatFloat(value.Value().(float64), 'f', 4, 64)
		stringnum = removezero(stringnum)
	} else if valuetype == "float32" {
		stringnum = strconv.FormatFloat(float64(value.Value().(float32)), 'f', 4, 64)
		stringnum = removezero(stringnum)
	} else if valuetype == "string" {
		stringnum = value.Value().(string)
	} else if valuetype == "bool" {
		stringnum = strconv.FormatBool(value.Value().(bool))
	} else if valuetype == "int" {
		stringnum = strconv.Itoa(value.Value().(int))
	} else if valuetype == "byte" {
		stringnum = string(value.Value().(byte))
	} else if valuetype == "uint" {
		stringnum = strconv.Itoa(int(value.Value().(uint)))
	} else if valuetype == "uint8" {
		stringnum = strconv.Itoa(int(value.Value().(uint8)))
	} else if valuetype == "uint16" {
		stringnum = strconv.Itoa(int(value.Value().(uint16)))
	} else if valuetype == "uint32" {
		stringnum = strconv.Itoa(int(value.Value().(uint32)))
	} else if valuetype == "uint64" {
		stringnum = strconv.Itoa(int(value.Value().(uint64)))
	}
	return stringnum
}

func Int32toString(n int32) string {
	buf := [11]byte{}
	pos := len(buf)
	i := int64(n)
	signed := i < 0
	if signed {
		i = -i
	}
	for {
		pos--
		buf[pos], i = '0'+byte(i%10), i/10
		if i == 0 {
			if signed {
				pos--
				buf[pos] = '-'
			}
			return string(buf[pos:])
		}
	}
}
