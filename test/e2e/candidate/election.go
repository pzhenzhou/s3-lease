package main

import (
	"context"
	"errors"
	"time"

	"github.com/pzhenzhou/s3-lease/lease"
	"github.com/pzhenzhou/s3-lease/recipes/leaderelection"
	"go.uber.org/zap"
)

func runElection(ctx context.Context, settings options, leaseClient lease.Client, logger *zap.Logger) error {
	electionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workStarted := make(chan struct{}, 1)
	stopped := make(chan struct{})
	elector, err := leaderelection.New(leaderelection.Config{
		Client:          leaseClient,
		RetryPeriod:     settings.retryPeriod,
		ObserveInterval: settings.observeInterval,
		ShutdownTimeout: settings.shutdownTimeout,
		ReleaseOnCancel: settings.releaseOnCancel,
		Logger:          logger,
		Callbacks: leaderelection.Callbacks{
			OnStartedLeading: func(workCtx context.Context, epochID uint64) error {
				workStarted <- struct{}{}
				logger.Info("candidate_election_work_started",
					zap.String("client_id", settings.clientID),
					zap.Uint64("epoch_id", epochID))
				if settings.cancelAfter > 0 {
					timer := time.AfterFunc(settings.cancelAfter, cancel)
					defer timer.Stop()
				}
				if settings.workBehavior == "noncooperative" {
					time.Sleep(settings.holdDuration)
					logger.Info("candidate_election_work_completed",
						zap.String("client_id", settings.clientID), zap.Uint64("epoch_id", epochID))
					return nil
				}
				timer := time.NewTimer(settings.holdDuration)
				defer timer.Stop()
				select {
				case <-workCtx.Done():
					logger.Info("candidate_election_work_stopped",
						zap.String("client_id", settings.clientID),
						zap.Uint64("epoch_id", epochID),
						zap.Error(workCtx.Err()))
					return workCtx.Err()
				case <-timer.C:
					logger.Info("candidate_election_work_completed",
						zap.String("client_id", settings.clientID), zap.Uint64("epoch_id", epochID))
					if settings.workBehavior == "error" {
						return errors.New("configured election work failure")
					}
					return nil
				}
			},
			OnStoppedLeading: func() {
				logger.Info("candidate_election_stopped_leading", zap.String("client_id", settings.clientID))
				close(stopped)
			},
			OnLeaderObserved: func(callbackCtx context.Context, observation lease.Observation) {
				logger.Info("candidate_election_observer_started",
					zap.String("client_id", settings.clientID),
					zap.String("observed_client_id", observation.ClientID),
					zap.Uint64("epoch_id", observation.EpochID),
					zap.Uint64("sequence_id", observation.SequenceID))
				if settings.observerDelay > 0 {
					timer := time.NewTimer(settings.observerDelay)
					defer timer.Stop()
					select {
					case <-callbackCtx.Done():
						return
					case <-timer.C:
					}
				}
				logger.Info("candidate_election_leader_observed",
					zap.String("client_id", settings.clientID),
					zap.String("observed_client_id", observation.ClientID),
					zap.Uint64("epoch_id", observation.EpochID),
					zap.Uint64("sequence_id", observation.SequenceID))
			},
		},
	})
	if err != nil {
		return err
	}
	runErr := elector.Run(electionCtx)
	select {
	case <-workStarted:
		select {
		case <-stopped:
		case <-time.After(time.Second):
		}
	default:
	}
	return runErr
}
