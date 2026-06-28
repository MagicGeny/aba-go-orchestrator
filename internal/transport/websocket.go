package transport

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type Hub struct {
	// Map of CampaignID -> set of connections
	clients sync.Map
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // For dev
	},
}

func NewHub() *Hub {
	return &Hub{}
}

func (h *Hub) HandleStream(w http.ResponseWriter, r *http.Request) {
	campaignIDStr := r.URL.Query().Get("id")
	campaignID, err := uuid.Parse(campaignIDStr)
	if err != nil {
		http.Error(w, "invalid campaign id", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	// Register client
	actual, _ := h.clients.LoadOrStore(campaignID, &sync.Map{})
	conns := actual.(*sync.Map)
	conns.Store(conn, struct{}{})
	defer conns.Delete(conn)

	// Keep connection alive until client disconnects or context is done
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

func (h *Hub) BroadcastStatus(campaignID uuid.UUID, state any) {
	actual, ok := h.clients.Load(campaignID)
	if !ok {
		return
	}

	conns := actual.(*sync.Map)
	data, _ := json.Marshal(state)

	conns.Range(func(key, value any) bool {
		conn := key.(*websocket.Conn)
		_ = conn.WriteMessage(websocket.TextMessage, data)
		return true
	})
}
