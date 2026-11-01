package controller

import "time"

type ErrorResponse struct {
	Error string `json:"error" example:"опрос не найден"`
}

type SessionResponse struct {
	Ready bool `json:"ready" example:"true"`
}

type VoteRequest struct {
	Options []string `json:"options" binding:"required,min=1,max=10,unique" example:"1"`
}

type VoteResponse struct {
	Status     string `json:"status" enums:"accepted"`
	ReceiptURL string `json:"receipt_url" example:"/api/v1/polls/11111111111141118111111111111111/receipt"`
}

type ReceiptResponse struct {
	Status  string   `json:"status" enums:"accepted,not_submitted_or_forgotten"`
	Options []string `json:"options,omitempty" example:"1"`
}

type CreatePollRequest struct {
	Question string    `json:"question" binding:"required" minLength:"1" maxLength:"300"`
	Type     string    `json:"type" binding:"required" enums:"single,multiple"`
	Options  []string  `json:"options" binding:"required,min=2,max=10,unique" minLength:"1" maxLength:"120"`
	OpensAt  time.Time `json:"opens_at" binding:"required" format:"date-time"`
	ClosesAt time.Time `json:"closes_at" binding:"required" format:"date-time"`
}
