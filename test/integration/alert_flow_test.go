package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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

// waitForNotification reads from events until match returns true or
// timeout elapses, ignoring non-"notification" SSE events and malformed
// payloads along the way (SSE keep-alive comments, if the handler ever
// adds them, would otherwise fail json.Unmarshal and shouldn't fail the
// test).
func waitForNotification(
	t *testing.T,
	events <-chan sseEvent,
	timeout time.Duration,
	match func(notificationEventDTO) bool,
) *notificationEventDTO {
	t.Helper()

	deadline := time.After(timeout)
	for {
		select {
		case e, ok := <-events:
			if !ok {
				t.Fatal("notification stream closed before a matching event arrived")
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
			t.Fatal("timed out waiting for expected notification")
		}
	}
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
