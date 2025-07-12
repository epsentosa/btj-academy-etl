package handler

import (
	"context"

	"processor/handler/nyc_trip"
	"processor/lib"
	"processor/logger"
	pb "processor/protos"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type InputFileHandler struct {
	pb.UnimplementedTransformServiceServer
	InputFileNYCTrip *nyc_trip.InputFile
	InputFileTesting *InputFileTesting
}

type InputFileTesting struct {
	MapId *lib.MapId
}

func NewInputFileTesting(mapId *lib.MapId) *InputFileTesting {
	return &InputFileTesting{
		MapId: mapId,
	}
}

func (h *InputFileHandler) ProcessNYCTrip(ctx context.Context, req *pb.InputFileRequest) (*pb.ProcessFileResponse, error) {
	return h.InputFileNYCTrip.ProcessNYCTrip(ctx, req)
}

func (h *InputFileHandler) ProcessTesting(ctx context.Context, req *pb.InputFileTestRequest) (*pb.ProcessFileTestResponse, error) {
	locationID := req.GetLocationId()
	in := h.InputFileTesting
	logger.Info(
		"received testing request", zap.Int64("LocationID", locationID),
	)
	mapTaxiZone, err := in.MapId.GetTaxiZoneMap()
	if err != nil {
		logger.Error("failed getting mapTaxiZone", zap.Error(err))
		return nil, status.New(codes.Internal, err.Error()).Err()
	}
	loc, found := mapTaxiZone.Get(locationID)
	if !found {
		return nil, status.Errorf(codes.Internal, "failed getting map value from locationID %v", locationID)
	}

	logger.Info("testing finished")
	return &pb.ProcessFileTestResponse{
		Borough:     *loc.Borough,
		Zone:        *loc.Zone,
		ServiceZone: *loc.Service_zone,
	}, nil
}
