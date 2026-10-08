module github.com/debanganthakuria/narad-go/prometheus

go 1.23.0

// The client release these metrics need. Importers resolve the client
// at this version or newer, since they ignore the replace below, so it
// must be a tagged release: tag the client first, then this module as
// prometheus/vX.Y.Z. Raise it when the metrics start using newer client
// API.
require (
	github.com/debanganthakuria/narad-go v0.1.0
	github.com/prometheus/client_golang v1.20.5
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	golang.org/x/sys v0.22.0 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
)

// In-repo development and CI build against the client next door. Go
// applies a replace only in the main module, so importers never see it.
replace github.com/debanganthakuria/narad-go => ../
