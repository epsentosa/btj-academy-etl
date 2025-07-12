package nyc_trip

import (
	"context"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"processor/lib"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

type nycTripParser struct {
	fileName       string
	remoteFilePath string
	mapId          *lib.MapId
	wg             sync.WaitGroup
	ch             chan parserRow
	droppedRow     atomic.Int64
	processedRow   atomic.Int64
	maxTime        *time.Time
	minTime        *time.Time
	err            error
}

type nycTripDbInsert struct {
	ctx         context.Context
	wg          sync.WaitGroup
	ch          chan dbRow
	dbConn      *pgxpool.Pool
	insertedRow atomic.Int64
	tableName   string
	column      []string
	err         error
}

type parserRow struct {
	line      []string
	headerMap map[string]int
}

type dbRow struct {
	Vendor               string
	PickupTime           time.Time
	DropoffTime          time.Time
	PassengerCount       int64
	TripDistance         float64
	PuLocationRegion     string
	PuLocationZone       string
	DoLocationRegion     string
	DoLocationZone       string
	PaymentType          string
	FareAmount           *float64
	Extra                *float64
	MtaTax               *float64
	TipAmount            *float64
	TollsAmount          *float64
	ImprovementSurcharge *float64
	TotalAmount          *float64
	CongestionSurcharge  *float64
	AirportFee           *float64
}

var vendorOpts = map[int]string{
	1: "Creative Mobile Technologies, LLC",
	2: "Curb Mobility, LLC",
	6: "Myle Technologies Inc",
	7: "Helix",
}

var paymentOpts = map[int]string{
	0: "Flex Fare trip",
	1: "Credit card",
	2: "Cash",
	3: "No charge",
	4: "Dispute",
	5: "Unknown",
	6: "Voided trip",
}

func newNycTripParser(fileName string, remoteFilePath string, mapId *lib.MapId) *nycTripParser {
	return &nycTripParser{
		fileName:       fileName,
		remoteFilePath: remoteFilePath,
		mapId:          mapId,
		ch:             make(chan parserRow, 100),
		droppedRow:     atomic.Int64{},
		processedRow:   atomic.Int64{},
	}
}

func newNycTripDbInsert(ctx context.Context, dbConn *pgxpool.Pool) *nycTripDbInsert {
	return &nycTripDbInsert{
		ctx:         ctx,
		ch:          make(chan dbRow, 100),
		dbConn:      dbConn,
		insertedRow: atomic.Int64{},
		tableName:   "nyc_trip",
		column: []string{
			"vendor",
			"pickup_time",
			"dropoff_time",
			"passenger_count",
			"trip_distance",
			"pu_location_region",
			"pu_location_zone",
			"do_location_regin",
			"do_location_zone",
			"payment_type",
			"fare_amount",
			"extra",
			"mta_tax",
			"tip_amount",
			"tolls_amount",
			"improvement_surcharge",
			"total_amount",
			"congestion_surcharge",
			"airport_fee",
		},
	}
}

func (row *parserRow) get(headerName string) string {
	val := row.line[row.headerMap[headerName]]
	switch headerName {
	case "vendorID":
		id, err := strconv.Atoi(val)
		if err != nil {
			return val
		}
		val = vendorOpts[id]
	case "payment_type":
		id, err := strconv.Atoi(val)
		if err != nil {
			return val
		}
		val = paymentOpts[id]
	}
	return val
}

func (r *dbRow) toSlice() []any {
	val := reflect.ValueOf(*r)

	res := make([]any, val.NumField())
	for i := range val.NumField() {
		if val.Field(i).Kind() == reflect.Ptr {
			if val.Field(i).IsNil() {
				res[i] = nil
			} else {
				res[i] = val.Field(i).Elem().Interface()
			}
		} else {
			res[i] = val.Field(i).Interface()
		}
	}
	return res
}

func parseToFloat64Ptr(s string) *float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return &f
}

// Drain the channel to prevent deadlocks if there was an error on the process
func (p *nycTripParser) drainCh() {
	for range p.ch {
	}
}

func (d *nycTripDbInsert) drainCh() {
	for range d.ch {
	}
}

func (p *nycTripParser) done() error {
	close(p.ch)
	p.wg.Wait()
	return p.err
}

func (d *nycTripDbInsert) done() error {
	close(d.ch)
	d.wg.Wait()
	return d.err
}

func (p *nycTripParser) processWorker(dbInsertChan chan<- dbRow) {
	defer p.wg.Done()
	const timeLayout string = "2006-01-02 15:04:05"
	var passengerCount int64

	mapTaxiZone, err := p.mapId.GetTaxiZoneMap()
	if err != nil {
		p.err = errors.Wrap(err, "failed getting taxi_zone map")
		p.drainCh()
		return
	}
	for row := range p.ch {
		vendor := row.get("VendorID")
		pickupTimeStr := row.get("tpep_pickup_datetime")
		pickupTime, err := time.Parse(timeLayout, pickupTimeStr)
		if err != nil {
			p.droppedRow.Add(1)
			continue
		}

		dropoffTimeStr := row.get("tpep_dropoff_datetime")
		dropoffTime, err := time.Parse(timeLayout, dropoffTimeStr)
		if err != nil {
			p.droppedRow.Add(1)
			continue
		}

		passengerCountStr := row.get("passenger_count")
		passengerCount, err = strconv.ParseInt(passengerCountStr, 10, 64)
		if err != nil {
			p.droppedRow.Add(1)
			continue
		}

		tripDistanceStr := row.get("trip_distance")
		tripDistance, err := strconv.ParseFloat(tripDistanceStr, 64)
		if err != nil {
			p.droppedRow.Add(1)
			continue
		}

		puLocationIdStr := row.get("PULocationID")
		puLocationId, err := strconv.ParseInt(puLocationIdStr, 10, 64)
		if err != nil {
			p.droppedRow.Add(1)
			continue
		}
		puLookup, found := mapTaxiZone.Get(puLocationId)
		if !found {
			p.droppedRow.Add(1)
			continue
		}

		doLocationIdStr := row.get("DOLocationID")
		doLocationId, err := strconv.ParseInt(doLocationIdStr, 10, 64)
		if err != nil {
			p.droppedRow.Add(1)
			continue
		}
		doLookup, found := mapTaxiZone.Get(doLocationId)
		if !found {
			p.droppedRow.Add(1)
			continue
		}

		paymentType := row.get("payment_type")

		fareAmountStr := row.get("fare_amount")
		fareAmount := parseToFloat64Ptr(fareAmountStr)

		extraStr := row.get("extra")
		extra := parseToFloat64Ptr(extraStr)

		mtaTaxStr := row.get("mta_tax")
		mtaTax := parseToFloat64Ptr(mtaTaxStr)

		tipAmountStr := row.get("tip_amount")
		tipAmount := parseToFloat64Ptr(tipAmountStr)

		tollsAmountStr := row.get("tolls_amount")
		tollsAmount := parseToFloat64Ptr(tollsAmountStr)

		improvementSurchargeStr := row.get("improvement_surcharge")
		improvementSurcharge := parseToFloat64Ptr(improvementSurchargeStr)

		totalAmountStr := row.get("total_amount")
		totalAmount := parseToFloat64Ptr(totalAmountStr)

		congestionSurchargeStr := row.get("congestion_surcharge")
		congestionSurcharge := parseToFloat64Ptr(congestionSurchargeStr)

		airportFeeStr := row.get("Airport_fee")
		airportFee := parseToFloat64Ptr(airportFeeStr)

		if p.maxTime == nil || dropoffTime.After(*p.maxTime) {
			p.maxTime = &dropoffTime
		}
		if p.minTime == nil || pickupTime.Before(*p.minTime) {
			p.minTime = &pickupTime
		}
		p.processedRow.Add(1)

		dbInsertChan <- dbRow{
			Vendor:               vendor,
			PickupTime:           pickupTime,
			DropoffTime:          dropoffTime,
			PassengerCount:       passengerCount,
			TripDistance:         tripDistance,
			PuLocationRegion:     *puLookup.Borough,
			PuLocationZone:       *puLookup.Zone,
			DoLocationRegion:     *doLookup.Borough,
			DoLocationZone:       *doLookup.Zone,
			PaymentType:          paymentType,
			FareAmount:           fareAmount,
			Extra:                extra,
			MtaTax:               mtaTax,
			TipAmount:            tipAmount,
			TollsAmount:          tollsAmount,
			ImprovementSurcharge: improvementSurcharge,
			TotalAmount:          totalAmount,
			CongestionSurcharge:  congestionSurcharge,
			AirportFee:           airportFee,
		}
	}
}

func (d *nycTripDbInsert) processWorker() {
	defer d.wg.Done()

	batchSize := 1000
	bufRowInsert := make([][]any, 0, batchSize)
	for {
		select {
		case rowInsert, ok := <-d.ch:
			if !ok {
				if len(bufRowInsert) > 0 {
					tbl := lib.NewTableInsert(d.tableName, d.column, bufRowInsert)
					totalInserted, err := lib.InsertUpdateDuplicateBatch(d.ctx, tbl, d.dbConn)
					if err != nil {
						d.err = errors.Wrap(err, "failed inserting to database")
					}
					d.insertedRow.Add(totalInserted)
				}
				return
			}

			row := rowInsert.toSlice()
			bufRowInsert = append(bufRowInsert, row)
			if len(bufRowInsert) == batchSize {
				tbl := lib.NewTableInsert(d.tableName, d.column, bufRowInsert)
				totalInserted, err := lib.InsertUpdateDuplicateBatch(d.ctx, tbl, d.dbConn)
				if err != nil {
					d.err = errors.Wrap(err, "failed inserting to database")
					d.drainCh()
					return
				}
				d.insertedRow.Add(totalInserted)
				bufRowInsert = bufRowInsert[:0]
			}
		case <-d.ctx.Done():
			return
		}
	}
}
