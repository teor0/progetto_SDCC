package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// to run test:
// GATEWAY_URL=http://<PUBLIC_IPV4>:8080 go test -tags=integration \
//  ./test/integration/... -run TestModeratorAlert_DeliveredToAllSubscribers -v
// remember to scale the service:
// docker compose up -d --build --scale notification-service=3

// notificationEventDTO mirrors internal/handlers/notification.go's
// notificationDTO -- the JSON shape pushed over SSE, not the internal
// proto type.
type notificationEventDTO struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	PhotoID     string `json:"photoId,omitempty"`
	GalleryID   string `json:"galleryId"`
	GalleryName string `json:"galleryName,omitempty"`
	UploaderID  string `json:"uploaderId,omitempty"`
	Message     string `json:"message,omitempty"`
	PhotoURL    string `json:"photoUrl,omitempty"`
	OccurredAt  string `json:"occurredAt,omitempty"`
}

type sseEvent struct {
	name string
	data string
}

// openNotificationStream opens the SSE endpoint and parses events onto a
// channel in a background goroutine.
//
// IMPORTANT: this does not (and cannot, given the current handler) confirm
// server-side subscription registration completed before returning.
// internal/handlers/notification.go's Stream handler writes nothing to the
// HTTP response -- not even headers -- until the underlying gRPC stream's
// first Recv() succeeds (Gin's c.Stream calls the read step before ever
// flushing), and the client-side Subscribe() call returns as soon as the
// stream is created, independent of whether Notification Service's
// Registry.Subscribe has actually run yet. There is currently no
// observable signal that registration is complete. Callers of this
// function must add their own short delay before triggering whatever
// they expect to be notified about -- see the sleep in the tests below,
// and the comment explaining why it's there.
func openNotificationStream(t *testing.T, token string) (<-chan sseEvent, func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL()+"/api/notifications/stream", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)

	events := make(chan sseEvent, 16)

	go func() {
		defer close(events)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			// Expected on cancel() -- the request context was torn down.
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return
		}

		reader := bufio.NewReader(resp.Body)
		var name string
		var dataLines []string

		for {
			line, err := reader.ReadString('\n')
			trimmed := strings.TrimRight(line, "\r\n")

			switch {
			case strings.HasPrefix(trimmed, "event:"):
				name = strings.TrimSpace(strings.TrimPrefix(trimmed, "event:"))
			case strings.HasPrefix(trimmed, "data:"):
				dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(trimmed, "data:")))
			case trimmed == "" && name != "" && len(dataLines) > 0:
				select {
				case events <- sseEvent{name: name, data: strings.Join(dataLines, "\n")}:
				case <-ctx.Done():
					return
				}
				name = ""
				dataLines = nil
			}

			if err != nil {
				return
			}
		}
	}()

	return events, cancel
}

// waitForNotificationNonFatal is the goroutine-safe counterpart to
// waitForNotification below: t.Fatal/t.FailNow must only ever be called
// from the goroutine actually running the test function, so this returns
// nil on timeout or a closed stream instead of failing directly -- letting
// a caller that's waiting on several subscribers concurrently (from
// multiple goroutines) aggregate results and report failures from the
// main test goroutine itself.
func waitForNotificationNonFatal(
	events <-chan sseEvent,
	timeout time.Duration,
	match func(notificationEventDTO) bool,
) *notificationEventDTO {
	deadline := time.After(timeout)
	for {
		select {
		case e, ok := <-events:
			if !ok {
				return nil
			}
			if e.name != "notification" {
				continue
			}
			var n notificationEventDTO
			if err := json.Unmarshal([]byte(e.data), &n); err != nil {
				continue
			}
			if match(n) {
				return &n
			}
		case <-deadline:
			return nil
		}
	}
}

// waitForNotification reads from events until match returns true or
// timeout elapses, ignoring non-"notification" SSE events and malformed
// payloads along the way (SSE keep-alive comments, if the handler ever
// adds them, would otherwise fail json.Unmarshal and shouldn't fail the
// test). Only safe to call from the test's own goroutine -- see
// waitForNotificationNonFatal for the concurrent case.
func waitForNotification(
	t *testing.T,
	events <-chan sseEvent,
	timeout time.Duration,
	match func(notificationEventDTO) bool,
) *notificationEventDTO {
	t.Helper()

	n := waitForNotificationNonFatal(events, timeout, match)
	if n == nil {
		t.Fatal("timed out waiting for a matching notification")
	}
	return n
}

func sendModeratorAlert(t *testing.T, moderatorToken, galleryID, body string) *http.Response {
	t.Helper()
	return doJSON(t, http.MethodPost, "/photogallery/galleries/"+galleryID+"/alert", moderatorToken,
		map[string]string{"body": body}, nil)
}

// TestModeratorAlert_DeliveredToMembers is the main flow: a moderator
// sends an alert, and a subscribed member receives it over SSE -- proving
// the full chain (Gallery Service -> RabbitMQ -> Notification Service's
// consumer -> Registry fan-out -> SSE) actually works end to end, not
// just that each hop works in isolation against a mock.
func TestModeratorAlert_DeliveredToMembers(t *testing.T) {
	_, moderatorToken := registerUser(t, "ROLE_MODERATOR")
	_, memberToken := registerUser(t, "ROLE_USER")

	gallery := createTestGallery(t, moderatorToken, "Moderator Alert Test Gallery")
	joinTestGallery(t, memberToken, gallery.ID)

	events, cancel := openNotificationStream(t, memberToken)
	defer cancel()

	// See the warning on openNotificationStream: there is no signal that
	// server-side subscription registration has completed by the time
	// the HTTP request for the stream has merely been issued. This sleep
	// is a deliberate, documented race-mitigation, not a guarantee --
	// if this test becomes flaky under heavier load (e.g. during your
	// scalability testing), widen it before assuming something else broke.
	time.Sleep(500 * time.Millisecond)

	const alertBody = "Please review the gallery guidelines."
	resp := sendModeratorAlert(t, moderatorToken, gallery.ID, alertBody)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	notif := waitForNotification(t, events, 10*time.Second, func(n notificationEventDTO) bool {
		return n.Type == "NOTIFICATION_TYPE_MODERATOR_ALERT" && n.GalleryID == gallery.ID
	})

	require.Equal(t, alertBody, notif.Message)
	require.Equal(t, gallery.Name, notif.GalleryName,
		"galleryName should be populated by Consumer.galleryName -- if this is empty, that lookup regressed")
}

// TestModeratorAlert_DeliveredToAllSubscribers opens several independent
// SSE subscriptions (different members, all joined to the same gallery)
// and confirms every single one receives the alert.
//
// Run this against a scaled deployment to actually exercise the thing it
// is meant to catch:
//
//	docker compose up -d --build --scale notification-service=3
//	GATEWAY_URL=http://<PUBLIC_IPV4>:8080 go test -tags=integration \
//	  ./test/integration/... -run TestModeratorAlert_DeliveredToAllSubscribers -v
//
// With multiple replicas and the gateway's round-robin dial in place,
// each Subscribe call is likely spread across different replicas. This is
// the test that would have caught the original bug: a shared durable
// RabbitMQ queue made replicas competing consumers, so only whichever
// replica happened to drain a given delivery could act on it -- every
// subscriber pinned to a different replica silently never got notified,
// with no error anywhere. It now exercises the Redis Pub/Sub fan-out that
// replaced that design (Consumer -> Broadcaster.PublishNotification ->
// every replica's own Registry).
//
// Run against a single replica, this still passes -- for the same reason
// it always did before any of this existed. The point of scaling replicas
// for this specific run is to actually stress the cross-replica path
// instead of trivially succeeding because everything happened to be on
// one process. This test cannot directly confirm subscribers landed on
// different replicas -- nothing in the API surface exposes that -- it
// only proves the externally observable contract: every member who is
// subscribed receives the alert, regardless of which replica happened to
// handle their stream or the RabbitMQ delivery. Cross-reference `docker
// compose logs notification-service` during a scaled run if you want to
// eyeball the actual distribution.
func TestModeratorAlert_DeliveredToAllSubscribers(t *testing.T) {
	const numSubscribers = 5

	_, moderatorToken := registerUser(t, "ROLE_MODERATOR")
	gallery := createTestGallery(t, moderatorToken, "Multi-Subscriber Alert Test Gallery")

	type subscriber struct {
		events <-chan sseEvent
		cancel func()
	}

	subscribers := make([]subscriber, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		_, token := registerUser(t, "ROLE_USER")
		joinTestGallery(t, token, gallery.ID)

		events, cancel := openNotificationStream(t, token)
		subscribers[i] = subscriber{events: events, cancel: cancel}
	}
	defer func() {
		for _, s := range subscribers {
			s.cancel()
		}
	}()

	// Same race-mitigation as TestModeratorAlert_DeliveredToMembers, just
	// covering every subscription registering, not only one.
	time.Sleep(500 * time.Millisecond)

	const alertBody = "Multi-subscriber delivery check."
	resp := sendModeratorAlert(t, moderatorToken, gallery.ID, alertBody)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Wait for all subscribers concurrently, not sequentially -- delivery
	// to each is independent, so a sequential per-subscriber timeout would
	// make this test's worst-case duration scale with numSubscribers for
	// no real reason.
	var wg sync.WaitGroup
	results := make([]*notificationEventDTO, numSubscribers)
	for i, s := range subscribers {
		wg.Add(1)
		go func(i int, events <-chan sseEvent) {
			defer wg.Done()
			results[i] = waitForNotificationNonFatal(events, 10*time.Second, func(n notificationEventDTO) bool {
				return n.Type == "NOTIFICATION_TYPE_MODERATOR_ALERT" && n.GalleryID == gallery.ID
			})
		}(i, s.events)
	}
	wg.Wait()

	for i, notif := range results {
		if !assert.NotNilf(t, notif, "subscriber %d never received the alert -- likely a cross-replica delivery gap", i) {
			continue
		}
		assert.Equal(t, alertBody, notif.Message, "subscriber %d got the wrong message", i)
		assert.Equal(t, gallery.Name, notif.GalleryName, "subscriber %d got the wrong galleryName", i)
	}
}

// TestModeratorAlert_NotDeliveredToNonMemberModerator documents, with an
// actual assertion rather than just a comment, the known quirk that a
// moderator who never joined their own gallery (CreateGallery doesn't
// auto-add a Member row -- see gallery_flow_test.go) does not receive
// their own alert. This is a best-effort negative check: absence within
// a bounded window isn't a strict proof of "never," but it's enough to
// catch an accidental regression the other direction (e.g. if someone
// "fixes" this by broadcasting alerts to non-members).
func TestModeratorAlert_NotDeliveredToNonMemberModerator(t *testing.T) {
	_, moderatorToken := registerUser(t, "ROLE_MODERATOR")
	gallery := createTestGallery(t, moderatorToken, "Moderator Not Subscribed Test Gallery")

	events, cancel := openNotificationStream(t, moderatorToken)
	defer cancel()
	time.Sleep(500 * time.Millisecond)

	resp := sendModeratorAlert(t, moderatorToken, gallery.ID, "should not reach myself")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	select {
	case e, ok := <-events:
		if ok {
			t.Fatalf("expected no notification for a moderator who never joined their own gallery, got: %+v", e)
		}
	case <-time.After(2 * time.Second):
		// No event within the window -- consistent with the moderator not
		// being a registered subscriber for this gallery.
	}
}

// TestModeratorAlert_RejectsNonModerator confirms a regular member of the
// gallery cannot send an alert, even though they're a legitimate member.
func TestModeratorAlert_RejectsNonModerator(t *testing.T) {
	_, moderatorToken := registerUser(t, "ROLE_MODERATOR")
	_, memberToken := registerUser(t, "ROLE_USER")

	gallery := createTestGallery(t, moderatorToken, "Non-Moderator Alert Test Gallery")
	joinTestGallery(t, memberToken, gallery.ID)

	resp := sendModeratorAlert(t, memberToken, gallery.ID, "members shouldn't be able to do this")
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

// TestModeratorAlert_RejectsWrongGalleryModerator confirms being
// ROLE_MODERATOR isn't sufficient on its own -- CommandService.
// SendModeratorAlert checks callerID against this specific gallery's
// ModeratorID, so a moderator of a *different* gallery must also be
// rejected. This is exactly the check the frontend's ModeratorAlertForm
// mirrors client-side to avoid showing a form that would always fail.
func TestModeratorAlert_RejectsWrongGalleryModerator(t *testing.T) {
	_, ownerToken := registerUser(t, "ROLE_MODERATOR")
	_, otherModeratorToken := registerUser(t, "ROLE_MODERATOR")

	gallery := createTestGallery(t, ownerToken, "Wrong Moderator Alert Test Gallery")

	resp := sendModeratorAlert(t, otherModeratorToken, gallery.ID, "not my gallery")
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}
