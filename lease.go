package blobfs

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type segmentLease struct {
	SegmentID string
	LeaseID   string
	IssuedAt  time.Time
	ExpiresAt time.Time
	Holder    string
	RefCount  int64
}

type LeaseHandle struct {
	store         *Store
	id            string
	segment       string
	issued        time.Time
	expires       time.Time
	mu            sync.Mutex
	autoRenew     bool
	renewInterval time.Duration
}

func (h *LeaseHandle) Renew(ctx context.Context, ttl time.Duration) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if ttl <= 0 {
		return ErrInvalidLeaseTTL
	}
	h.mu.Lock()
	h.expires = time.Now().Add(ttl)
	expires := h.expires
	h.mu.Unlock()

	h.store.leaseMu.Lock()
	if lease, ok := h.store.leases[h.segment]; ok && lease.LeaseID == h.id {
		lease.ExpiresAt = expires
	}
	h.store.leaseMu.Unlock()
	return nil
}

func (h *LeaseHandle) Release(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	h.store.releaseLease(h.segment, h.id)
	return nil
}

func (h *LeaseHandle) ExpiresAt() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.expires
}

func (h *LeaseHandle) SegmentID() string {
	return h.segment
}

func (s *Store) grantLease(segmentID, holder string, ttl time.Duration) (*LeaseHandle, error) {
	if ttl < 0 {
		return nil, ErrInvalidLeaseTTL
	}
	if ttl == 0 {
		ttl = s.cfg.GC.DefaultLeaseTTL
		if ttl <= 0 {
			ttl = 24 * time.Hour
		}
	}
	if ttl < time.Minute {
		return nil, ErrInvalidLeaseTTL
	}
	if ttl > 168*time.Hour {
		ttl = 168 * time.Hour
	}

	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()

	if s.cfg.GC.MaxConcurrentLeases > 0 && len(s.leases) >= s.cfg.GC.MaxConcurrentLeases {
		if s.leases[segmentID] == nil {
			return nil, ErrTooManyLeases
		}
	}

	now := time.Now()
	leaseID := fmt.Sprintf("lease-%d-%d", now.UnixNano(), now.Unix()%10000)
	expires := now.Add(ttl)

	lease := &segmentLease{
		SegmentID: segmentID,
		LeaseID:   leaseID,
		IssuedAt:  now,
		ExpiresAt: expires,
		Holder:    holder,
		RefCount:  1,
	}
	s.leases[segmentID] = lease

	return &LeaseHandle{
		store:         s,
		id:            leaseID,
		segment:       segmentID,
		issued:        now,
		expires:       expires,
		autoRenew:     true,
		renewInterval: 5 * time.Minute,
	}, nil
}

func (s *Store) releaseLease(segmentID, leaseID string) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()

	lease, ok := s.leases[segmentID]
	if !ok || lease.LeaseID != leaseID {
		return
	}
	lease.RefCount--
	if lease.RefCount <= 0 {
		delete(s.leases, segmentID)
	}
}

func (s *Store) getActiveLease(segmentID string) *segmentLease {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()

	lease, ok := s.leases[segmentID]
	if !ok {
		return nil
	}
	if time.Now().After(lease.ExpiresAt) {
		delete(s.leases, segmentID)
		return nil
	}
	return lease
}

func (s *Store) getSegmentSafetyInfo(segmentID string) *SegmentSafetyInfo {
	s.leaseMu.Lock()
	lease := s.leases[segmentID]
	hasLease := false
	var expiresAt time.Time
	var holder string
	if lease != nil && time.Now().Before(lease.ExpiresAt) {
		hasLease = true
		expiresAt = lease.ExpiresAt
		holder = lease.Holder
	}
	s.leaseMu.Unlock()

	s.metaMu.RLock()
	seg := s.meta.Segments[segmentID]
	s.metaMu.RUnlock()

	info := &SegmentSafetyInfo{
		SegmentID:      segmentID,
		HasActiveLease: hasLease,
		LeaseExpiresAt: expiresAt,
		LeaseHolder:    holder,
	}
	if seg != nil {
		info.State = seg.State
		info.CanBeDeleted = !hasLease && seg.State != segmentStateDeleted && seg.State != segmentStateCompacting
	} else {
		info.State = "NOT_FOUND"
		info.CanBeDeleted = false
	}
	return info
}

type SegmentSafetyInfo struct {
	SegmentID      string
	State          string
	SafetyCutoff   time.Time
	HasActiveLease bool
	LeaseExpiresAt time.Time
	LeaseHolder    string
	CanBeDeleted   bool
}

func (s *Store) GetSegmentSafetyInfo(ctx context.Context, segmentID string) (*SegmentSafetyInfo, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	return s.getSegmentSafetyInfo(segmentID), nil
}
