package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/op/go-logging"

	"github.com/coinman-dev/3ax-ui/v2/database"
	"github.com/coinman-dev/3ax-ui/v2/database/model"
	xuilogger "github.com/coinman-dev/3ax-ui/v2/logger"
	"github.com/coinman-dev/3ax-ui/v2/mtproto"
)

// TestMtprotoSameEmailFlow is the core acceptance test for the refactor: two
// clients of one mtproto inbound may share an Email, each gets a unique Uuid and
// its own FakeTLS secret, and per-client traffic / quota enforcement works on the
// dedicated table.
func TestMtprotoSameEmailFlow(t *testing.T) {
	xuilogger.InitLogger(logging.ERROR)
	// Point at the mtg-multi binary when present (local dev) so the multi-user
	// backend is exercised; in CI the path is absent and we fall back gracefully.
	if _, err := os.Stat("/usr/local/x-ui/bin"); err == nil {
		os.Setenv("XUI_BIN_FOLDER", "/usr/local/x-ui/bin")
	}
	dbPath := filepath.Join(t.TempDir(), "x-ui.db")
	if err := database.InitDB(dbPath); err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	db := database.GetDB()

	// A disabled inbound so reconcile won't spawn a real sidecar process.
	ib := &model.Inbound{
		Remark:   "mt",
		Enable:   false,
		Port:     38445,
		Protocol: model.MTProto,
		Tag:      "inbound-38445",
		Settings: `{"fakeTlsDomain":"www.cloudflare.com"}`,
	}
	if err := db.Create(ib).Error; err != nil {
		t.Fatalf("create inbound: %v", err)
	}

	svc := &MtprotoClientService{}
	c1 := &model.MtprotoClient{InboundId: ib.Id, Email: "shared", Enable: true}
	c2 := &model.MtprotoClient{InboundId: ib.Id, Email: "shared", Enable: true}
	if err := svc.AddClient(c1); err != nil {
		t.Fatalf("add c1: %v", err)
	}
	if err := svc.AddClient(c2); err != nil {
		t.Fatalf("add c2 (same email must be allowed): %v", err)
	}

	// Same email, distinct uuid + distinct secret, both valid for the domain.
	if c1.Email != c2.Email {
		t.Fatal("emails should be equal")
	}
	if c1.Uuid == c2.Uuid {
		t.Fatalf("uuids must be unique: %q", c1.Uuid)
	}
	if c1.Secret == c2.Secret {
		t.Fatalf("secrets must differ: %q", c1.Secret)
	}
	suffix := "ee" // FakeTLS marker
	if !strings.HasPrefix(c1.Secret, suffix) || !strings.HasPrefix(c2.Secret, suffix) {
		t.Fatalf("secrets must be FakeTLS: %q %q", c1.Secret, c2.Secret)
	}

	clients, err := svc.GetClientsByInbound(ib.Id)
	if err != nil || len(clients) != 2 {
		t.Fatalf("expected 2 clients, got %d (err %v)", len(clients), err)
	}

	// Instance derivation + backend detection.
	inst, ok := mtproto.InstanceFromInbound(ib, clients)
	if !ok {
		t.Fatal("expected a usable instance")
	}
	if inst.MultiUser != mtproto.MultiUserSupported() {
		t.Fatalf("MultiUser flag should mirror backend detection: %v", inst.MultiUser)
	}
	if len(inst.Clients) != 2 {
		t.Fatalf("expected 2 active clients, got %d", len(inst.Clients))
	}

	// Disable one → only one active.
	if err := svc.ToggleClientByUuid(c2.Uuid, false); err != nil {
		t.Fatalf("toggle: %v", err)
	}
	clients, _ = svc.GetClientsByInbound(ib.Id)
	inst, _ = mtproto.InstanceFromInbound(ib, clients)
	if len(inst.Clients) != 1 || inst.Clients[0].Id != c1.Uuid {
		t.Fatalf("expected only c1 active, got %+v", inst.Clients)
	}
	if err := svc.ToggleClientByUuid(c2.Uuid, true); err != nil {
		t.Fatalf("re-enable: %v", err)
	}

	// Traffic accumulation by uuid.
	now := time.Now().UnixMilli()
	svc.RecordTraffic([]mtproto.Traffic{{Uuid: c1.Uuid, Up: 100, Down: 200, LastSeen: now}})
	got, _ := svc.GetClientByUuid(c1.Uuid)
	if got.Upload != 100 || got.Download != 200 || got.AllTime != 300 {
		t.Fatalf("traffic not accumulated: up=%d down=%d all=%d", got.Upload, got.Download, got.AllTime)
	}
	if got.LastOnline == 0 {
		t.Fatal("last_online should be set from a live scrape")
	}

	// Quota enforcement: a 50-byte cap with 60 bytes of traffic disables the client.
	db.Model(&model.MtprotoClient{}).Where("uuid = ?", c2.Uuid).Update("total_gb", 50)
	changed := svc.RecordTraffic([]mtproto.Traffic{{Uuid: c2.Uuid, Up: 60, Down: 0}})
	if !changed {
		t.Fatal("quota breach should report a change")
	}
	got2, _ := svc.GetClientByUuid(c2.Uuid)
	if got2.Enable {
		t.Fatal("client over quota must be disabled")
	}

	mtproto.GetManager().StopAll()
}

// TestMtprotoBulkActions covers the "general actions" that must reach the
// dedicated mtproto_clients table: reset-all-traffic and delete-depleted.
func TestMtprotoBulkActions(t *testing.T) {
	xuilogger.InitLogger(logging.ERROR)
	dbPath := filepath.Join(t.TempDir(), "x-ui.db")
	if err := database.InitDB(dbPath); err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	db := database.GetDB()
	ib := &model.Inbound{Remark: "mt", Enable: false, Port: 38446, Protocol: model.MTProto, Tag: "inbound-38446", Settings: `{"fakeTlsDomain":"www.cloudflare.com"}`}
	if err := db.Create(ib).Error; err != nil {
		t.Fatal(err)
	}
	svc := &MtprotoClientService{}
	over := &model.MtprotoClient{InboundId: ib.Id, Email: "over", Enable: true, TotalGB: 50}
	ok := &model.MtprotoClient{InboundId: ib.Id, Email: "ok", Enable: true}
	if err := svc.AddClient(over); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddClient(ok); err != nil {
		t.Fatal(err)
	}
	db.Model(&model.MtprotoClient{}).Where("uuid = ?", ok.Uuid).Updates(map[string]any{"upload": 10, "download": 20})

	// Reset-all zeroes the resettable counters.
	if err := svc.ResetAllClientTraffics(ib.Id); err != nil {
		t.Fatalf("ResetAllClientTraffics: %v", err)
	}
	gotOk, _ := svc.GetClientByUuid(ok.Uuid)
	if gotOk.Upload != 0 || gotOk.Download != 0 {
		t.Fatalf("reset-all should zero up/down: %+v", gotOk)
	}

	// Make `over` exceed its quota, then delete-depleted removes only it.
	db.Model(&model.MtprotoClient{}).Where("uuid = ?", over.Uuid).Updates(map[string]any{"upload": 60})
	if err := svc.DelDepletedClients(ib.Id); err != nil {
		t.Fatalf("DelDepletedClients: %v", err)
	}
	if _, err := svc.GetClientByUuid(over.Uuid); err == nil {
		t.Fatal("depleted client should have been deleted")
	}
	if _, err := svc.GetClientByUuid(ok.Uuid); err != nil {
		t.Fatal("healthy client must remain")
	}

	mtproto.GetManager().StopAll()
}
