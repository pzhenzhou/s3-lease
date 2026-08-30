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
	"github.com/pzhenzhou/s3-lease/recipes/mutex"
	"github.com/pzhenzhou/s3-lease/s3store"
	"go.uber.org/zap"
)

type options struct {
	mode            string
	endpoint        string
	region          string
	bucket          string
	key             string
	clientID        string
	leaseDuration   time.Duration
	renewDeadline   time.Duration
	requestTimeout  time.Duration
	renewPeriod     time.Duration
	retryPeriod     time.Duration
	observeInterval time.Duration
	shutdownTimeout time.Duration
	holdDuration    time.Duration
	cancelAfter     time.Duration
	release         bool
	releaseOnCancel bool
	productionLog   bool
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
	flag.StringVar(&settings.mode, "mode", "core", "candidate mode: core or mutex")
	flag.StringVar(&settings.endpoint, "endpoint", "http://127.0.0.1:8333", "S3 endpoint")
	flag.StringVar(&settings.region, "region", "us-east-1", "AWS signing region")
	flag.StringVar(&settings.bucket, "bucket", "lease-tests", "lease bucket")
	flag.StringVar(&settings.key, "key", "", "full lease object key")
	flag.StringVar(&settings.clientID, "client-id", "", "stable logical client ID")
	flag.DurationVar(&settings.leaseDuration, "lease-duration", 15*time.Second, "advertised lease duration")
	flag.DurationVar(&settings.renewDeadline, "renew-deadline", 10*time.Second, "local renewal deadline")
	flag.DurationVar(&settings.requestTimeout, "request-timeout", 2*time.Second, "per-request timeout")
	flag.DurationVar(&settings.renewPeriod, "renew-period", time.Second, "renewal interval")
	flag.DurationVar(&settings.retryPeriod, "retry-period", time.Second, "mutex acquisition and renewal interval")
	flag.DurationVar(&settings.observeInterval, "observe-interval", 500*time.Millisecond, "mutex waiting observation interval")
	flag.DurationVar(&settings.shutdownTimeout, "shutdown-timeout", 3*time.Second, "mutex work join timeout")
	flag.DurationVar(&settings.holdDuration, "hold-duration", 5*time.Second, "bounded holding duration")
	flag.DurationVar(&settings.cancelAfter, "cancel-after", 0, "cancel a mutex lifecycle after work starts")
	flag.BoolVar(&settings.release, "release", true, "release after the hold duration")
	flag.BoolVar(&settings.releaseOnCancel, "release-on-cancel", false, "release a mutex after canceled work joins")
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
	if settings.holdDuration <= 0 || settings.cancelAfter < 0 {
		return errors.New("hold-duration must be positive and cancel-after must not be negative")
	}
	if settings.mode != "core" && settings.mode != "mutex" {
		return errors.New("mode must be core or mutex")
	}
	if settings.mode == "core" && settings.renewPeriod <= 0 {
		return errors.New("renew-period must be positive")
	}
	if settings.mode == "mutex" &&
		(settings.retryPeriod <= 0 || settings.observeInterval <= 0 || settings.shutdownTimeout <= 0) {
		return errors.New("mutex timing values must be positive")
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
	if settings.mode == "mutex" {
		return runMutex(ctx, settings, leaseClient, logger)
	}
	return runCore(ctx, settings, leaseClient, logger)
}

func runCore(ctx context.Context, settings options, leaseClient lease.Client, logger *zap.Logger) error {
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

func runMutex(ctx context.Context, settings options, leaseClient lease.Client, logger *zap.Logger) error {
	lock, err := mutex.New(mutex.Config{
		Client:          leaseClient,
		RetryPeriod:     settings.retryPeriod,
		ObserveInterval: settings.observeInterval,
		ShutdownTimeout: settings.shutdownTimeout,
		ReleaseOnCancel: settings.releaseOnCancel,
		Logger:          logger,
	})
	if err != nil {
		return err
	}
	lockCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	return lock.WithLock(lockCtx, func(workCtx context.Context, epochID uint64) error {
		logger.Info("candidate_mutex_work_started",
			zap.String("client_id", settings.clientID),
			zap.Uint64("epoch_id", epochID))
		var cancelTimer *time.Timer
		if settings.cancelAfter > 0 {
			cancelTimer = time.AfterFunc(settings.cancelAfter, cancel)
			defer cancelTimer.Stop()
		}
		holdTimer := time.NewTimer(settings.holdDuration)
		defer holdTimer.Stop()
		select {
		case <-workCtx.Done():
			logger.Info("candidate_mutex_work_stopped",
				zap.String("client_id", settings.clientID),
				zap.Uint64("epoch_id", epochID),
				zap.Error(workCtx.Err()))
			return workCtx.Err()
		case <-holdTimer.C:
			logger.Info("candidate_mutex_work_completed",
				zap.String("client_id", settings.clientID),
				zap.Uint64("epoch_id", epochID))
			return nil
		}
	})
}
