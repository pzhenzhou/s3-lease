package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pzhenzhou/s3-lease/lease"
	"github.com/pzhenzhou/s3-lease/pkg/common"
	"github.com/pzhenzhou/s3-lease/s3store"
	"go.uber.org/zap"
)

type options struct {
	endpoint       string
	region         string
	bucket         string
	key            string
	clientID       string
	leaseDuration  time.Duration
	renewDeadline  time.Duration
	requestTimeout time.Duration
	renewPeriod    time.Duration
	holdDuration   time.Duration
	release        bool
	productionLog  bool
}

func main() {
	settings := parseFlags()
	logger := common.InitLogger(settings.productionLog)
	logger.Info("candidate_started", zap.String("client_id", settings.clientID), zap.String("key", settings.key))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, settings, logger)
	cancel()
	if err != nil {
		logger.Error("candidate_failed", zap.Error(err))
	} else {
		logger.Info("candidate_completed")
	}
	_ = logger.Sync()
	if err != nil {
		os.Exit(1)
	}
}

func parseFlags() options {
	var settings options
	flag.StringVar(&settings.endpoint, "endpoint", "http://127.0.0.1:8333", "S3 endpoint")
	flag.StringVar(&settings.region, "region", "us-east-1", "AWS signing region")
	flag.StringVar(&settings.bucket, "bucket", "lease-tests", "lease bucket")
	flag.StringVar(&settings.key, "key", "", "full lease object key")
	flag.StringVar(&settings.clientID, "client-id", "", "stable logical client ID")
	flag.DurationVar(&settings.leaseDuration, "lease-duration", 15*time.Second, "advertised lease duration")
	flag.DurationVar(&settings.renewDeadline, "renew-deadline", 10*time.Second, "local renewal deadline")
	flag.DurationVar(&settings.requestTimeout, "request-timeout", 2*time.Second, "per-request timeout")
	flag.DurationVar(&settings.renewPeriod, "renew-period", time.Second, "renewal interval")
	flag.DurationVar(&settings.holdDuration, "hold-duration", 5*time.Second, "bounded holding duration")
	flag.BoolVar(&settings.release, "release", true, "release after the hold duration")
	flag.BoolVar(&settings.productionLog, "production-log", true, "emit production JSON logs")
	flag.Parse()
	return settings
}

func run(ctx context.Context, settings options, logger *zap.Logger) (err error) {
	defer func() {
		if err != nil {
			logger.Error("candidate_run_failed", zap.Error(err))
		}
	}()
	if settings.key == "" || settings.clientID == "" {
		return errors.New("key and client-id are required")
	}
	if settings.renewPeriod <= 0 || settings.holdDuration <= 0 {
		return errors.New("renew-period and hold-duration must be positive")
	}
	awsConfig, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(settings.region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("lease-dev", "local-test-only", "")),
	)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	client := s3.NewFromConfig(awsConfig, func(s3Options *s3.Options) {
		s3Options.BaseEndpoint = aws.String(settings.endpoint)
		s3Options.UsePathStyle = true
	})
	store, err := s3store.New(s3store.Config{Client: client, Logger: logger})
	if err != nil {
		return err
	}
	leaseClient, err := lease.New(lease.Config{
		Store:          store,
		Key:            lease.Key{Bucket: settings.bucket, ObjectKey: settings.key},
		ClientID:       settings.clientID,
		MetadataName:   "e2e-candidate",
		LeaseDuration:  settings.leaseDuration,
		RenewDeadline:  settings.renewDeadline,
		RequestTimeout: settings.requestTimeout,
		Logger:         logger,
	})
	if err != nil {
		return err
	}
	acquired, err := leaseClient.Require(ctx)
	if err != nil {
		return err
	}
	if err := acquired.Check(); err != nil {
		return err
	}
	logger.Info("candidate_acquired", zap.Uint64("epoch_id", acquired.EpochID()), zap.Time("valid_until", acquired.ValidUntil()))

	renewTimer := time.NewTicker(settings.renewPeriod)
	holdTimer := time.NewTimer(settings.holdDuration)
	defer renewTimer.Stop()
	defer holdTimer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-acquired.Done():
			return acquired.Check()
		case <-renewTimer.C:
			if err := leaseClient.Renew(ctx, acquired); err != nil {
				return err
			}
			logger.Info("candidate_renewed", zap.Uint64("epoch_id", acquired.EpochID()), zap.Time("valid_until", acquired.ValidUntil()))
		case <-holdTimer.C:
			if settings.release {
				if err := leaseClient.Release(ctx, acquired); err != nil {
					return err
				}
				logger.Info("candidate_released", zap.Uint64("epoch_id", acquired.EpochID()))
			}
			return nil
		}
	}
}
