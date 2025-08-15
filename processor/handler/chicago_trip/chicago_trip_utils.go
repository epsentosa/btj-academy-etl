package chicago_trip

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

type chicagoTripParser struct {
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

type chicagoTripDbInsert struct {
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
	TripStart                time.Time
	TripEnd                  time.Time
	TripMiles                *float64
	Fare                     *float64
	Tips                     *float64
	Tolls                    *float64
	Extras                   *float64
	TripTotal                *float64
	PaymentType              string
	Company                  string
	PickupCentroidLatitude   string
	PickupCentroidLongitude  string
	DropoffCentroidLatitude  string
	DropoffCentroidLongitude string
}

func newChicagoTripParser(fileName string, remoteFilePath string) *chicagoTripParser {
	return &chicagoTripParser{
		fileName:       fileName,
		remoteFilePath: remoteFilePath,
		ch:             make(chan parserRow, 100),
		droppedRow:     atomic.Int64{},
		processedRow:   atomic.Int64{},
	}
}

func newChicagoTripDbInsert(ctx context.Context, dbConn *pgxpool.Pool) *chicagoTripDbInsert {
	return &chicagoTripDbInsert{
		ctx:         ctx,
		ch:          make(chan dbRow, 100),
		dbConn:      dbConn,
		insertedRow: atomic.Int64{},
		tableName:   "chicago_trip",
		column: []string{
			"trip_start",
			"trip_end",
			"trip_miles",
			"fare",
			"tips",
			"tolls",
			"extras",
			"trip_total",
			"payment_type",
			"company",
			"pickup_centroid_latitude",
			"pickup_centroid_longitude",
			"dropoff_centroid_latitude",
			"dropoff_centroid_longitude",
		},
	}
}

func (row *parserRow) get(headerName string) string {
	val := row.line[row.headerMap[headerName]]
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
func (p *chicagoTripParser) drainCh() {
	for range p.ch {
	}
}

func (d *chicagoTripDbInsert) drainCh() {
	for range d.ch {
	}
}

func (p *chicagoTripParser) done() error {
	close(p.ch)
	p.wg.Wait()
	return p.err
}

func (d *chicagoTripDbInsert) done() error {
	close(d.ch)
	d.wg.Wait()
	return d.err
}

func (p *chicagoTripParser) processWorker(dbInsertChan chan<- dbRow) {
	defer p.wg.Done()
	const timeLayout string = "2006-01-02T15:04:05.000"

	for row := range p.ch {
		tripStartStr := row.get("trip_start_timestamp")
		tripStart, err := time.Parse(timeLayout, tripStartStr)
		if err != nil {
			p.droppedRow.Add(1)
			continue
		}
		tripEndStr := row.get("trip_start_timestamp")
		tripEnd, err := time.Parse(timeLayout, tripEndStr)
		if err != nil {
			p.droppedRow.Add(1)
			continue
		}
		tripMilesStr := row.get("trip_miles")
		tripMiles := parseToFloat64Ptr(tripMilesStr)

		fareStr := row.get("fare")
		fare := parseToFloat64Ptr(fareStr)

		tipsStr := row.get("tips")
		tips := parseToFloat64Ptr(tipsStr)

		tollsStr := row.get("tolls")
		tolls := parseToFloat64Ptr(tollsStr)

		extrasStr := row.get("extras")
		extras := parseToFloat64Ptr(extrasStr)

		trip_totalStr := row.get("trip_total")
		tripTotal := parseToFloat64Ptr(trip_totalStr)

		paymentType := row.get("payment_type")
		company := row.get("company")
		pickupCentroidLatitude := row.get("pickup_centroid_latitude")
		pickupCentroidLongitude := row.get("pickup_centroid_longitude")
		dropoffCentroidLatitude := row.get("dropoff_centroid_latitude")
		dropoffCentroidLongitude := row.get("dropoff_centroid_longitude")

		if p.maxTime == nil || tripEnd.After(*p.maxTime) {
			p.maxTime = &tripEnd
		}
		if p.minTime == nil || tripStart.Before(*p.minTime) {
			p.minTime = &tripStart
		}
		p.processedRow.Add(1)

		dbInsertChan <- dbRow{
			TripStart:                tripStart,
			TripEnd:                  tripEnd,
			TripMiles:                tripMiles,
			Fare:                     fare,
			Tips:                     tips,
			Tolls:                    tolls,
			Extras:                   extras,
			TripTotal:                tripTotal,
			PaymentType:              paymentType,
			Company:                  company,
			PickupCentroidLatitude:   pickupCentroidLatitude,
			PickupCentroidLongitude:  pickupCentroidLongitude,
			DropoffCentroidLatitude:  dropoffCentroidLatitude,
			DropoffCentroidLongitude: dropoffCentroidLongitude,
		}
	}
}

func (d *chicagoTripDbInsert) processWorker() {
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
