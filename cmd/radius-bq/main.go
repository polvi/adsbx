package main

import (
	"cloud.google.com/go/bigquery"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"google.golang.org/api/option"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

type RapidAPIRadiusResponse struct {
	ACs []AC `json:"ac"`
}

type AC struct {
	//"postime":"1611680687237"
	PosTime string `json:"postime"`
	//"icao":"4B18FE"
	Icao string `json:"icao"`
	//"reg":"HB-JMC"
	Reg string `json:"reg"`
	//"type":"A343"
	Type string `json:"type"`
	//"wtc":"3"
	Wtc string `json:"wtc"`
	//"spd":"371.4"
	Spd string `json:"spd"`
	//"altt":"0"
	Altt string `json:"altt"`
	//"alt":"34000"
	Alt string `json:"alt"`
	//"galt":"34010"
	Galt string `json:"galt"`
	//"talt":"34016"
	Talt string `json:"talt"`
	//"lat":"51.341718"
	Lat string `json:"lat"`
	//"lon":"0.807343"
	Lon string `json:"lon"`
	//"vsit":"1"
	Vsit string `json:"vsit"`
	//"vsi":"32"
	Vsi string `json:"vsi"`
	//"trkh":"0"
	Trkh string `json:"trkh"`
	//"ttrk":""
	Ttrk string `json:"ttrk"`
	//"trak":"320.4"
	Trak string `json:"trak"`
	//"sqk":"3057"
	Sqk string `json:"sqk"`
	//"call":"SWR8T"
	Call string `json:"call"`
	//"gnd":"0"
	Gnd string `json:"gnd"`
	//"trt":"5"
	Trt string `json:"trt"`
	//"pos":"1"
	Pos string `json:"pos"`
	//"mlat":"0"
	Mlat string `json:"mlat"`
	//"tisb":"0"
	Tisb string `json:"tisb"`
	//"sat":"0"
	Sat string `json:"sat"`
	//"opicao":"SWR"
	Opicao string `json:"opicao"`
	//"cou":"Switzerland"
	Cou string `json:"cou"`
	//"mil":"0"
	Mil string `json:"mil"`
	//"interested":"0"
	Interested string `json:"interested"`
	//"from":"ZRH Zürich Switzerland"
	From string `json:"from"`
	//"to":"ORD Chicago OHare United States"
	To string `json:"to"`
	//"dst":"15.22"
	Dst string `json:"dst"`
}

// Save implements the ValueSaver interface.
// This example disables best-effort de-duplication, which allows for higher throughput.
func (i *AC) Save() (map[string]bigquery.Value, string, error) {
	postime, _ := strconv.ParseInt(i.PosTime[0:10], 10, 64)
	wtc, _ := strconv.ParseInt(i.Wtc, 10, 64)
	spd, _ := strconv.ParseFloat(i.Spd, 64)
	altt, _ := strconv.ParseInt(i.Altt, 10, 64)
	alt, _ := strconv.ParseInt(i.Alt, 10, 64)
	galt, _ := strconv.ParseInt(i.Galt, 10, 64)
	talt, _ := strconv.ParseInt(i.Talt, 10, 64)
	trak, _ := strconv.ParseFloat(i.Trak, 64)
	lat, _ := strconv.ParseFloat(i.Lat, 64)
	lon, _ := strconv.ParseFloat(i.Lon, 64)
	vsit, _ := strconv.ParseInt(i.Vsit, 10, 64)
	trkh, _ := strconv.ParseInt(i.Trkh, 10, 64)
	ttrk, _ := strconv.ParseInt(i.Ttrk, 10, 64)
	sqk, _ := strconv.ParseInt(i.Sqk, 10, 64)
	gnd, _ := strconv.ParseInt(i.Gnd, 10, 64)
	trt, _ := strconv.ParseInt(i.Trt, 10, 64)
	pos, _ := strconv.ParseInt(i.Pos, 10, 64)
	mlat, _ := strconv.ParseInt(i.Mlat, 10, 64)
	tisb, _ := strconv.ParseInt(i.Tisb, 10, 64)
	sat, _ := strconv.ParseInt(i.Sat, 10, 64)
	mil, _ := strconv.ParseInt(i.Mil, 10, 64)
	interested, _ := strconv.ParseInt(i.Interested, 10, 64)
	dst, _ := strconv.ParseFloat(i.Dst, 64)

	return map[string]bigquery.Value{
		"postime":    postime,
		"icao":       i.Icao,
		"reg":        i.Reg,
		"type":       i.Type,
		"wtc":        wtc,
		"spd":        spd,
		"altt":       altt,
		"alt":        alt,
		"galt":       galt,
		"talt":       talt,
		"trak":       trak,
		"lat":        lat,
		"lon":        lon,
		"vsit":       vsit,
		"trkh":       trkh,
		"ttrk":       ttrk,
		"sqk":        sqk,
		"call":       i.Call,
		"gnd":        gnd,
		"trt":        trt,
		"pos":        pos,
		"mlat":       mlat,
		"tisb":       tisb,
		"sat":        sat,
		"opicao":     i.Opicao,
		"cou":        i.Cou,
		"mil":        mil,
		"interested": interested,
		"from":       i.From,
		"to":         i.To,
		"dst":        dst,
		"geo":        fmt.Sprintf("POINT(%f %f)", lon, lat),
	}, bigquery.NoDedupeID, nil
}

func main() {

	project := flag.String("project", "adsb-storage", "gcp project")
	dataset := flag.String("dataset", "adsb", "bigquery dataset")
	table := flag.String("table", "adsbx", "bigquery table")
	keyfile := flag.String("keyfile", "", "service account keyfile")
	rapidAPIKey := flag.String("rapidapi-key", "", "rapidapi key")
	latFlag := flag.Float64("lat", 44.204900, "lat")
	lonFlag := flag.Float64("lon", -121.279170, "lon")
	radius := flag.Int64("radius", 250, "query radius")
	sleep := flag.String("sleep", "1m", "duration to sleep between requests, such as 30s, 1m, 2h45m, etc")

	flag.Parse()

	dur, err := time.ParseDuration(*sleep)
	if err != nil {
		fmt.Println("error parsing -sleep", err)
		flag.PrintDefaults()
		os.Exit(2)
	}

	if *rapidAPIKey == "" || *keyfile == "" {
		fmt.Println("you must specify -rapidapi-key and -keyfile")
		flag.PrintDefaults()
		os.Exit(2)
	}

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, *project, option.WithCredentialsFile(*keyfile))
	if err != nil {
		log.Fatalln("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	inserter := client.Dataset(*dataset).Table(*table).Inserter()

	url := fmt.Sprintf("https://adsbexchange-com1.p.rapidapi.com/json/lat/%f/lon/%f/dist/%d/", *latFlag, *lonFlag, *radius)

	for {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.Fatalln(err)
		}

		req.Header.Add("x-rapidapi-key", *rapidAPIKey)
		req.Header.Add("x-rapidapi-host", "adsbexchange-com1.p.rapidapi.com")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatalln(err)
		}

		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			log.Println(err)
		}
		var resp RapidAPIRadiusResponse
		err = json.Unmarshal(body, &resp)
		if err != nil {
			log.Println(string(body))
			log.Println(err)
		}
		if resp.ACs == nil {
			log.Fatalln(string(body))
		}

		items := []*AC{}
		for i, _ := range resp.ACs {
			items = append(items, &resp.ACs[i])
		}
		if err := inserter.Put(ctx, items); err != nil {
			log.Println(string(body))
			log.Fatalln(err)
		}
		time.Sleep(dur)
	}
}
