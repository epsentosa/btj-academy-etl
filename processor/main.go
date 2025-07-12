package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"

	"processor/handler"
	"processor/handler/nyc_trip"
	"processor/lib"
	"processor/logger"
	pb "processor/protos"

	"github.com/caarlos0/env/v11"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"google.golang.org/grpc"
)

func runGrpcServer(cfg *config, dbConn *pgxpool.Pool, mapId *lib.MapId) {
	logger.Info("starting grpc")
	lis, err := net.Listen("tcp", "0.0.0.0:3000")
	if err != nil {
		logger.Panic("failed to listen", zap.Error(err))
	}

	inputFileNYCTrip := nyc_trip.NewInputFile(cfg.RedisHost, cfg.RedisDB, dbConn, mapId)
	InputFileTesting := handler.NewInputFileTesting(mapId)

	s := grpc.NewServer()
	pb.RegisterTransformServiceServer(s, &handler.InputFileHandler{
		InputFileNYCTrip: inputFileNYCTrip,
		InputFileTesting: InputFileTesting,
	})
	logger.Info("server listening at", zap.Any("listener", lis.Addr()))
	if err := s.Serve(lis); err != nil {
		logger.Panic("failed to serve grpc", zap.Error(err))
	}
}

func main() {
	var cfg config
	err := env.Parse(&cfg)
	if err != nil {
		logger.Panic("failed configuring instance", zap.Error(err))
	}
	var isDevel bool
	if cfg.Environment == Development {
		isDevel = true
	}
	logger.NewLog(cfg.LogLevel, isDevel)
	defer func() {
		if err := logger.Sync(); err != nil {
			logger.Panic("failed syncing logger", zap.Error(err))
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	signal.Notify(sig, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()

	dbConn, err := lib.NewDBConn(ctx, cfg.DBUrl)
	if err != nil {
		logger.Panic("failed connecting to database", zap.Error(err))
	}
	defer dbConn.Close()

	mapId, err := lib.NewMap(ctx, dbConn)
	if err != nil {
		logger.Panic("failed creating Map", zap.Error(err))
	}
	lib.WaitMap(mapId)
	defer lib.CloseMap(mapId)
	go lib.MustAutoRefreshMap(ctx, dbConn, mapId)

	runGrpcServer(&cfg, dbConn, mapId)
}
