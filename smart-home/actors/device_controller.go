package actors

import (
	"sync"

	af "github.com/LukaKeselj/Agenti/actor-framework"
	"github.com/LukaKeselj/Agenti/smart-home/persistence"
)

// ── DeviceControllerActor ──────────────────────────────────────

// DeviceControllerActor simulates IoT devices (AC, heating, blinds,
// lights) in each room.  It receives AdjustEnvironment commands
// from the Coordinator and reports device status to the Logger.
type DeviceControllerActor struct {
	af.BaseActor

	mu        sync.Mutex
	devices   map[string]map[string]float64 // roomID → device → value
	persister *persistence.Persister
}

type deviceSavedState struct {
	Devices map[string]map[string]float64 `json:"devices"`
}

func (d *DeviceControllerActor) SetPersister(p *persistence.Persister) {
	d.persister = p
}

func (d *DeviceControllerActor) OnPreStart(ctx af.ActorContext) error {
	if d.persister == nil {
		return nil
	}
	var state deviceSavedState
	if err := d.persister.Load(string(d.ID()), &state); err != nil {
		return nil
	}
	d.mu.Lock()
	d.devices = state.Devices
	if d.devices == nil {
		d.devices = make(map[string]map[string]float64)
	}
	d.mu.Unlock()
	ctx.Log().Info("restored device controller state", "rooms", len(state.Devices))
	return nil
}

func (d *DeviceControllerActor) persistState() {
	if d.persister == nil {
		return
	}
	d.mu.Lock()
	state := deviceSavedState{Devices: d.devices}
	d.mu.Unlock()
	_ = d.persister.Save(string(d.ID()), state)
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

	d.persistState()
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


