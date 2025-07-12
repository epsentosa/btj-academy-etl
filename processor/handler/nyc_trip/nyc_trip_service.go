package nyc_trip

import (
	"bufio"
	"bytes"
	"context"
	"strconv"
	"strings"
	"time"

	"processor/lib"
	"processor/logger"
	pb "processor/protos"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type InputFile struct {
	pb.UnimplementedTransformServiceServer

	RedisHost string
	RedisDB   string
	DBConn    *pgxpool.Pool
	MapId     *lib.MapId
}

func NewInputFile(redisHost, redisDB string, dbConn *pgxpool.Pool, mapId *lib.MapId) *InputFile {
	return &InputFile{
		RedisHost: redisHost,
		RedisDB:   redisDB,
		DBConn:    dbConn,
		MapId:     mapId,
	}
}

func (in *InputFile) ProcessNYCTrip(ctx context.Context, req *pb.InputFileRequest) (*pb.ProcessFileResponse, error) {
	inputFile := req.GetInputFile()
	logger.Info("received request", zap.String("FileInput", inputFile))

	perfStart := time.Now()
	redisDB, err := strconv.Atoi(in.RedisDB)
	if err != nil {
		logger.Error("failed converting redis db to int", zap.Error(err))
		return nil, status.New(codes.Internal, err.Error()).Err()
	}
	redisClient, err := lib.NewRedisClient(ctx, in.RedisHost, redisDB)
	if err != nil {
		logger.Error("failed creating redis client", zap.Error(err))
		return nil, status.New(codes.Internal, err.Error()).Err()
	}
	inputFileRedis, err := redisClient.GetInputFile(ctx, inputFile)
	if err != nil {
		logger.Error("failed getting input file from redis", zap.Error(err))
		return nil, status.New(codes.Internal, err.Error()).Err()
	}
	defer func() {
		if err := redisClient.DeleteInputFile(ctx, inputFile); err != nil {
			logger.Error("failed deleting input file from redis", zap.Error(err))
		}
	}()

	fileList := [][]byte{}

	if strings.HasSuffix(inputFile, ".tar.gz") {
		files, err := lib.ExtractTarGz(inputFileRedis.File)
		if err != nil {
			logger.Error("failed to extract tar.gz file", zap.Error(err))
			return nil, status.Errorf(codes.Internal, "failed to extract tar.gz file %v", inputFile)
		}
		for _, file := range files {
			fileList = append(fileList, file)
		}
	} else if strings.HasSuffix(inputFile, ".txt") || strings.HasSuffix(inputFile, ".csv") {
		fileList = append(fileList, inputFileRedis.File)
	} else {
		logger.Error("file type not supported", zap.String("file", inputFile))
		return nil, status.Errorf(codes.Internal, "file type not supported %v", inputFile)
	}

	var lineNumber int64
	var totalRow int64

	parser := newNycTripParser(inputFile, req.GetRemoteFilePath(), in.MapId)

	dbInsert := newNycTripDbInsert(ctx, in.DBConn)

	workerCount := 3
	for range workerCount {
		parser.wg.Add(1)
		go parser.processWorker(dbInsert.ch)
	}

	for range workerCount {
		dbInsert.wg.Add(1)
		go dbInsert.processWorker()
	}

	for _, file := range fileList {
		scanner := bufio.NewScanner(bytes.NewReader(file))

		headerMap := make(map[string]int)

		for scanner.Scan() {
			line := scanner.Text()
			row := strings.Split(line, "\t")
			totalRow++
			if lineNumber == 0 {
				for i, col := range row {
					headerMap[col] = i
				}
				lineNumber++
			} else {
				parser.ch <- parserRow{
					line:      row,
					headerMap: headerMap,
				}
			}
		}
	}

	if err = parser.done(); err != nil {
		logger.Error("failed parsing file", zap.Error(err))
		return nil, status.Error(codes.Internal, parser.err.Error())
	}
	if err = dbInsert.done(); err != nil {
		logger.Error("failed inserting to database", zap.Error(err))
		return nil, status.Error(codes.Internal, dbInsert.err.Error())
	}

	var maxTime *timestamppb.Timestamp
	var minTime *timestamppb.Timestamp

	if parser.maxTime == nil {
		maxTime = nil
	} else {
		maxTime = timestamppb.New(*parser.maxTime)
	}
	if parser.minTime == nil {
		minTime = nil
	} else {
		minTime = timestamppb.New(*parser.minTime)
	}

	perfEnd := time.Now()
	logger.Info("processed", zap.Duration("duration", perfEnd.Sub(perfStart)))

	return &pb.ProcessFileResponse{
		TotalRows:     totalRow - 1, // mines the header line
		ProcessedRows: parser.processedRow.Load(),
		DroppedRows:   parser.droppedRow.Load(),
		InsertedRows:  dbInsert.insertedRow.Load(),
		MaxTime:       maxTime,
		MinTime:       minTime,
	}, nil
}
