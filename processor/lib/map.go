package lib

import (
	"context"
	"time"

	"processor/logger"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type MapId struct {
	mapTaxiZone *ristretto.Cache[int64, TaxiZone]
}

type TaxiZone struct {
	Borough      *string
	Zone         *string
	Service_zone *string
}

func (m *MapId) newTaxiZone() error {
	var err error
	m.mapTaxiZone, err = ristretto.NewCache(&ristretto.Config[int64, TaxiZone]{
		NumCounters: 300,             // number of keys to track frequency.
		MaxCost:     1 * 1024 * 1024, // maximum cost of cache (20MB). Please adjust this value based on use case.
		BufferItems: 64,              // number of keys per Get buffer.
	})
	if err != nil {
		return err
	}
	return nil
}

func (m *MapId) putTaxiZone(ctx context.Context, conn *pgxpool.Pool) error {
	rows, err := conn.Query(ctx, `
		SELECT location_id key,
			   borough, zone, service_zone
		FROM taxi_zone_lookup
		WHERE 1=1
		  AND borough != 'N/A'
		  AND zone != 'N/A'
		  AND service_zone != 'N/A'
	`)
	if err != nil {
		return errors.Wrap(err, "failed querying taxi_zone_lookup")
	}
	defer rows.Close()

	for rows.Next() {
		var key int64
		var borough string
		var zone string
		var serviceZone string
		err := rows.Scan(&key, &borough, &zone, &serviceZone)
		if err != nil {
			return errors.Wrap(err, "failed scanning taxi_zone_lookup")
		}
		m.mapTaxiZone.Set(key, TaxiZone{Borough: &borough, Zone: &zone, Service_zone: &serviceZone}, 1)
	}

	return nil
}

func (m *MapId) GetTaxiZoneMap() (*ristretto.Cache[int64, TaxiZone], error) {
	if m.mapTaxiZone == nil {
		return nil, errors.New("mapTaxiZone map is not created")
	}
	return m.mapTaxiZone, nil
}

func NewMap(ctx context.Context, conn *pgxpool.Pool) (*MapId, error) {
	start := time.Now()
	mapId := new(MapId)
	err := mapId.newTaxiZone()
	if err != nil {
		return nil, errors.Wrap(err, "failed creating mapTaxiZone")
	}
	err = mapId.putTaxiZone(ctx, conn)
	if err != nil {
		return nil, errors.Wrap(err, "failed putting mapTaxiZone")
	}
	logger.Info("creating map", zap.Duration("duration", time.Since(start)))
	return mapId, nil
}

func MustAutoRefreshMap(ctx context.Context, conn *pgxpool.Pool, mapId *MapId) {
	for range time.Tick(time.Hour * 6) {
		start := time.Now()
		err := mapId.putTaxiZone(ctx, conn)
		if err != nil {
			logger.Panic("failed creating mapTaxiZone", zap.Error(err))
		}
		logger.Info("refreshed map", zap.Duration("duration", time.Since(start)))
	}
}

func WaitMap(mapId *MapId) {
	mapId.mapTaxiZone.Wait()
}

func CloseMap(mapId *MapId) {
	mapId.mapTaxiZone.Close()
}
