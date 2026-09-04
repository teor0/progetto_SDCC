import { useState } from "react";
import { galleryApi } from "../services/galleryApi";
import { authStore } from "../stores/AuthStore";
import "./ModeratorAlertForm.css";

type ModeratorAlertFormProps = {
    galleryId: string;
    galleryModeratorId: string;
};

export default function ModeratorAlertForm({
                                               galleryId,
                                               galleryModeratorId,
                                           }: ModeratorAlertFormProps) {
    const auth = authStore.getState();
    const [message, setMessage] = useState("");
    const [sending, setSending] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [sent, setSent] = useState(false);

    // Being ROLE_MODERATOR alone isn't enough -- the backend only allows
    // the gallery's own moderator (CommandService.SendModeratorAlert
    // checks callerID != g.ModeratorID). This mirrors that check so a
    // moderator of a *different* gallery doesn't see a form that would
    // always fail with PermissionDenied.
    if (auth.role !== "ROLE_MODERATOR" || auth.userId !== galleryModeratorId) {
        return null;
    }

    async function handleSend() {
        const body = message.trim();
        if (!body) {
            return;
        }

        setSending(true);
        setError(null);
        setSent(false);

        try {
            await galleryApi.sendModeratorAlert(galleryId, body);
            setMessage("");
            setSent(true);
        } catch (err) {
            setError(err instanceof Error ? err.message : "Failed to send alert");
        } finally {
            setSending(false);
        }
    }

    return (
        <section className="moderator-alert">
            <h3>Send Moderator Alert</h3>

            <textarea
                placeholder="Alert message for all gallery members"
                value={message}
                onChange={(event) => setMessage(event.target.value)}
                disabled={sending}
            />

            <button onClick={handleSend} disabled={sending || !message.trim()}>
                {sending ? "Sending..." : "Send Alert"}
            </button>

            {error && <p className="alert-error">Error: {error}</p>}
            {sent && <p className="alert-success">Alert sent.</p>}
        </section>
    );
}