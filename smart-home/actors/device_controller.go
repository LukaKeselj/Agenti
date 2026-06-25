package actors

import (
	"sync"

	af "github.com/LukaKeselj/Agenti/actor-framework"
)

// ── DeviceControllerActor ──────────────────────────────────────

// DeviceControllerActor simulates IoT devices (AC, heating, blinds,
// lights) in each room.  It receives AdjustEnvironment commands
// from the Coordinator and reports device status to the Logger.
type DeviceControllerActor struct {
	af.BaseActor

	mu      sync.Mutex
	devices map[string]map[string]float64 // roomID → device → value
}

// NewDeviceControllerActor creates a new device controller.
func NewDeviceControllerActor(id af.ActorID) *DeviceControllerActor {
	return &DeviceControllerActor{
		BaseActor: af.NewBaseActor(id),
		devices:   make(map[string]map[string]float64),
	}
}

func (d *DeviceControllerActor) Receive(ctx af.ActorContext, msg af.Message) {
	switch msg.MsgType {
	case MsgAdjustEnvironment:
		d.handleAdjustEnvironment(ctx, msg)
	}
}

func (d *DeviceControllerActor) handleAdjustEnvironment(ctx af.ActorContext, msg af.Message) {
	p, ok := msg.Payload.(AdjustEnvironmentPayload)
	if !ok {
		return
	}

	d.mu.Lock()
	room := p.RoomID
	if d.devices[room] == nil {
		d.devices[room] = make(map[string]float64)
	}
	d.devices[room]["temperature"] = p.TargetTemp
	d.devices[room]["humidity"] = p.TargetHum
	d.devices[room]["light"] = p.TargetLight
	d.mu.Unlock()

	ctx.Log().Info("environment adjusted",
		"room", room,
		"temp", p.TargetTemp,
		"hum", p.TargetHum,
		"light", p.TargetLight,
	)

	// Report each device status to logger (fire-and-forget).
	for _, device := range []string{"ac", "heating", "blinds", "lights"} {
		// Try to find the logger.
		if logRef, err := ctx.System().Lookup("logger"); err == nil {
			logRef.Tell(af.Message{
				MsgType: MsgDeviceStatus,
				Payload: DeviceStatusPayload{
					RoomID:     room,
					DeviceType: device,
					Action:     "adjusted",
					ActorID:    string(ctx.Self().ID()),
				},
			})
		}
	}
}

// GetDeviceValue returns the current value of a device in a room.
func (d *DeviceControllerActor) GetDeviceValue(roomID, device string) (float64, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.devices[roomID] == nil {
		return 0, false
	}
	v, ok := d.devices[roomID][device]
	return v, ok
}


