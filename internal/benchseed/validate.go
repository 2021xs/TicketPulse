package benchseed

import (
	"encoding/json"
	"fmt"

	"ticketpulse/internal/ticket"
)

func sameState(a, b ticket.State) bool {
	av, _ := json.Marshal(a)
	bv, _ := json.Marshal(b)
	return string(av) == string(bv)
}
func ValidateBundle(b Bundle) error {
	t := b.Ticket
	if t.Version != int64(len(b.Histories)) || t.Version < 1 || len(b.Histories) > 40 {
		return fmt.Errorf("seed ticket %d version/history budget differs", t.ID)
	}
	comments := map[int64]ticket.Comment{}
	for _, c := range b.Comments {
		if c.ID <= 0 || c.TicketID != t.ID || c.Version < 2 {
			return fmt.Errorf("seed comment identity differs")
		}
		comments[c.ID] = c
	}
	var previous *ticket.State
	var resolved bool
	var reopened int64
	for i, h := range b.Histories {
		if h.Version != int64(i+1) || h.TicketID != t.ID || h.After.Version != h.Version || h.ResponsibleTeamID != h.After.CurrentTeamID {
			return fmt.Errorf("seed ticket %d history key/team differs", t.ID)
		}
		if i == 0 {
			if h.Action != ticket.ActionCreated || h.Before != nil || h.After.Status != ticket.StatusPending || !h.OccurredAt.Equal(t.CreatedAt) {
				return fmt.Errorf("seed creation differs")
			}
		} else if h.Before == nil || !sameState(*previous, *h.Before) || !h.OccurredAt.After(b.Histories[i-1].OccurredAt) {
			return fmt.Errorf("seed history chain differs")
		}
		if h.Action == ticket.ActionCommented {
			if h.CommentID == nil {
				return fmt.Errorf("seed comment history missing link")
			}
			c, exists := comments[*h.CommentID]
			if !exists || c.Version != h.Version || c.AuthorID != h.OperatorID || !c.CreatedAt.Equal(h.OccurredAt) {
				return fmt.Errorf("seed comment row/history disagrees")
			}
			delete(comments, *h.CommentID)
		} else if h.CommentID != nil {
			return fmt.Errorf("non-comment history has link")
		}
		if h.Action == ticket.ActionResolved {
			if h.Before == nil || h.Before.Status != ticket.StatusInProgress || h.After.Status != ticket.StatusAwaitingConfirmation || h.After.FirstResolvedAt == nil {
				return fmt.Errorf("seed invalid resolution")
			}
			if !resolved {
				if !h.After.FirstResolvedAt.Equal(h.OccurredAt) {
					return fmt.Errorf("seed first resolution timestamp differs")
				}
				resolved = true
			}
		}
		if h.Action == ticket.ActionReopened {
			reopened++
		}
		if h.After.ReopenCount != reopened {
			return fmt.Errorf("seed reopen count differs")
		}
		if previous != nil && previous.FirstResolvedAt != nil && (h.After.FirstResolvedAt == nil || !previous.FirstResolvedAt.Equal(*h.After.FirstResolvedAt)) {
			return fmt.Errorf("seed overwritten first resolution")
		}
		state := h.After
		previous = &state
	}
	if len(comments) != 0 || !sameState(*previous, t.State()) || !b.Histories[len(b.Histories)-1].OccurredAt.Equal(t.UpdatedAt) {
		return fmt.Errorf("seed final ticket/comment state differs")
	}
	if (t.Status == ticket.StatusClosed) != (t.ClosedAt != nil) {
		return fmt.Errorf("seed closed timestamp differs")
	}
	return nil
}
