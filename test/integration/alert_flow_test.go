//go:build integration

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
//
//	docker compose up -d --build --scale notification-service=3
//
// go test -tags=integration ./test/integration/... -v
//
//	Point at a non-local stack (AWS) with:
//	GATEWAY_URL=http://<PUBLIC_IPV4>:8080 go test -tags=integration \
//	  ./test/integration/... -run TestModeratorAlert_DeliveredToAllSubscribers -v
//
// This test doesn't uses mock so YOU NEED TO CLEANUP THE TEST RESULTS AFTER
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
// sends an alert, and a subscribed member receives it over SSE proving
// the full chain (Gallery Service -> RabbitMQ -> Notification Service's
// consumer -> Registry fan-out -> SSE)
func TestModeratorAlert_DeliveredToMembers(t *testing.T) {
	_, moderatorToken := registerUser(t, "ROLE_MODERATOR")
	_, memberToken := registerUser(t, "ROLE_USER")

	gallery := createTestGallery(t, moderatorToken, "Moderator Alert Test Gallery")
	joinTestGallery(t, memberToken, gallery.ID)

	events, cancel := openNotificationStream(t, memberToken)
	defer cancel()

	//  there is no signal that subscription has completed so we use sleep
	time.Sleep(500 * time.Millisecond)

	const alertBody = "Please review the gallery guidelines."
	resp := sendModeratorAlert(t, moderatorToken, gallery.ID, alertBody)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	notif := waitForNotification(t, events, 10*time.Second, func(n notificationEventDTO) bool {
		return n.Type == "NOTIFICATION_TYPE_MODERATOR_ALERT" && n.GalleryID == gallery.ID
	})

	require.Equal(t, alertBody, notif.Message)
	require.Equal(t, gallery.Name, notif.GalleryName,
		"galleryName should be populated by Consumer.galleryName")
}

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

	time.Sleep(500 * time.Millisecond)

	const alertBody = "Multi-subscriber delivery check."
	resp := sendModeratorAlert(t, moderatorToken, gallery.ID, alertBody)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Wait for all subscribers concurrently, not sequentially
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
		if !assert.NotNilf(t, notif, "subscriber %d never received the alert", i) {
			continue
		}
		assert.Equal(t, alertBody, notif.Message, "subscriber %d got the wrong message", i)
		assert.Equal(t, gallery.Name, notif.GalleryName, "subscriber %d got the wrong galleryName", i)
	}
}

// TestModeratorAlert_RejectsNonModerator confirms a regular member of the
// gallery cannot send an alert, even if they're a legitimate member.
func TestModeratorAlert_RejectsNonModerator(t *testing.T) {
	_, moderatorToken := registerUser(t, "ROLE_MODERATOR")
	_, memberToken := registerUser(t, "ROLE_USER")

	gallery := createTestGallery(t, moderatorToken, "Non-Moderator Alert Test Gallery")
	joinTestGallery(t, memberToken, gallery.ID)

	resp := sendModeratorAlert(t, memberToken, gallery.ID, "members shouldn't be able to do this")
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

// TestModeratorAlert_RejectsWrongGalleryModerator checks callerID against this specific gallery's
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
